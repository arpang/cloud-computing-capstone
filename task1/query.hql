set hive.vectorized.execution.enabled=true;
set hive.optimize.skewjoin=true;
set hive.skewjoin.key=50000;
set mapred.reduce.tasks=300;


CREATE EXTERNAL TABLE oneyear(Year int, Quarter int, Month int, DayofMonth int, DayOfWeek int, FlightDate date, FlightNum string, UniqueCarrier string, Origin string, Dest string, CRSDepTime int, DepTime int, DepDelay float, DepDelayMinutes float, CRSArrTime int, ArrTime int, ArrDelay float, ArrDelayMinutes float, Cancelled boolean, Diverted boolean) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 's3://capstone-transport-data/data_2008';

CREATE EXTERNAL TABLE transport(Year int, Quarter int, Month int, DayofMonth int, DayOfWeek int, FlightDate date, FlightNum string, UniqueCarrier string, Origin string, Dest string, CRSDepTime int, DepTime int, DepDelay float, DepDelayMinutes float, CRSArrTime int, ArrTime int, ArrDelay float, ArrDelayMinutes float, Cancelled boolean, Diverted boolean) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 's3://capstone-transport-data/data_newer';

1.2
select UniqueCarrier, round(avg(ArrDelay), 2) as avgArrDelay from transport where ArrDelay!=-100000 group by UniqueCarrier order by avgArrDelay limit 10;


1.3
select CASE DayOfWeek
           WHEN 1 THEN 'Monday'
           WHEN 2 THEN 'Tuesday'
           WHEN 3 THEN 'Wednesday'
           WHEN 4 THEN 'Thursday'
           WHEN 5 THEN 'Friday'
           WHEN 6 THEN 'Saturday'
           WHEN 7 THEN 'Sunday'
           END AS weekday, round(avg(ArrDelay), 2) as avgArrDelay from transport where ArrDelay!=-100000 and DayOfWeek!=-100000 group by DayOfWeek order by avgArrDelay;

2.1
CREATE EXTERNAL TABLE 2_1 (Origin string, UniqueCarrier string, avgDepDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_1", "dynamodb.column.mapping" = "Origin:origin,UniqueCarrier:carrier,avgDepDelay:avgDepDelay");

INSERT OVERWRITE TABLE 2_1 select Origin, UniqueCarrier, avgDepDelay from (select *, rank() OVER (PARTITION BY Origin order by avgDepDelay) as row_number from (select Origin, UniqueCarrier, round(avg(DepDelay), 2) as avgDepDelay from transport where DepDelay!=-100000 group by Origin, UniqueCarrier) A) B where row_number <=10;

  
2.2
CREATE EXTERNAL TABLE 2_2 (Origin string, Dest string, avgDepDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_2", "dynamodb.column.mapping" = "Origin:origin,Dest:dest,avgDepDelay:avgDepDelay");

INSERT OVERWRITE TABLE 2_2 select Origin, Dest, avgDepDelay from (select *, rank() OVER (PARTITION BY Origin order by avgDepDelay) as row_number from (select Origin, Dest, round(avg(DepDelay), 2) as avgDepDelay from transport where DepDelay!=-100000 group by Origin, Dest) A) B where row_number <=10;


2.4
CREATE EXTERNAL TABLE 2_4 (Origin string, Dest string, avgArrDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_4", "dynamodb.column.mapping" = "Origin:origin,Dest:dest,avgArrDelay:avgArrDelay");

INSERT OVERWRITE TABLE 2_4 select Origin, Dest, round(avg(ArrDelay), 2) as avgArrDelay from transport where ArrDelay!=-100000 group by Origin, Dest;

3.1  select airport, totalfrequency, rank, log2(totalfrequency), log2(rank) from(
      select *, rank() OVER(order by totalfrequency desc) as rank from(
        select airport, sum(frequency) as totalfrequency from (select origin as airport, count(*) as frequency from transport group by origin UNION ALL select dest as airport, count(*) as frequency from transport group by dest
        ) A group by airport
      ) B
    ) C;

3.2
CREATE EXTERNAL TABLE 3_2 (route string, tripstartdate string, totalarrdelay double, firstlegdetails string, firstlegarrdelay double, secondlegdetails string, secondlegarrdelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "3_2", "dynamodb.column.mapping" = "route:route,tripstartdate:tripstartdate,totalarrdelay:totalarrdelay,firstlegdetails:firstlegdetails,firstlegarrdelay:firstlegarrdelay,secondlegdetails:secondlegdetails,secondlegarrdelay:secondlegarrdelay");



INSERT OVERWRITE TABLE 3_2 select route, tripstartdate, totalarrdelay, firstlegdetails, firstlegarrdelay, secondlegdetails, secondlegarrdelay from (
    select *, rank() OVER (PARTITION BY route, tripstartdate order by totalarrdelay, row_number) as rank from (
        select
            row_number() over() as row_number,
            CONCAT_WS ('-', first_leg.origin, first_leg.dest, second_leg.dest) as route, 
            first_leg.flightdate as tripstartdate,
            first_leg.arrdelay + second_leg.arrdelay as totalarrdelay, 
            CONCAT_WS(" ", first_leg.uniquecarrier, first_leg.flightnum, cast(first_leg.CRSDepTime as string), cast(first_leg.flightdate as string)) as firstlegdetails,
            CONCAT_WS(" ", second_leg.uniquecarrier, second_leg.flightnum, cast(second_leg.CRSDepTime as string), cast(second_leg.flightdate as string)) as secondlegdetails, 
            first_leg.arrdelay as firstlegarrdelay, 
            second_leg.arrdelay as secondlegarrdelay 
            from
            (
                select * from 
                (
                    select *, rank() OVER (PARTITION BY origin, dest, flightdate order by arrdelay) as rank from 
                    (
                        select * from oneyear where CRSDepTime<1200 and arrdelay!=-100000 
                    ) A
                ) B where rank=1
            ) first_leg INNER JOIN 
            (
                select * from 
                (
                    select *, rank() OVER (PARTITION BY origin, dest, flightdate order by arrdelay) as rank from 
                    (
                        select * from oneyear where CRSDepTime>1200 and arrdelay!=-100000 
                    ) C
                ) D where rank=1
            ) second_leg on first_leg.dest=second_leg.origin where 
            unix_timestamp(second_leg.flightdate)-unix_timestamp(first_leg.flightdate)=2*24*60*60
        ) E 
    ) F where rank=1;