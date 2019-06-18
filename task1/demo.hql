set hive.vectorized.execution.enabled=true;
set hive.optimize.skewjoin=true;
set hive.skewjoin.key=50000;
set mapred.reduce.tasks=300;


CREATE EXTERNAL TABLE transport(Year int, Quarter int, Month int, DayofMonth int, DayOfWeek int, FlightDate date, FlightNum string, UniqueCarrier string, Origin string, Dest string, CRSDepTime int, DepTime int, DepDelay float, DepDelayMinutes float, CRSArrTime int, ArrTime int, ArrDelay float, ArrDelayMinutes float, Cancelled boolean, Diverted boolean) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 's3://capstone-transport-data/data_newer';

CREATE EXTERNAL TABLE 2_1 (Origin string, UniqueCarrier string, avgDepDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_1", "dynamodb.column.mapping" = "Origin:origin,UniqueCarrier:carrier,avgDepDelay:avgDepDelay");

CREATE EXTERNAL TABLE 2_2 (Origin string, Dest string, avgDepDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_2", "dynamodb.column.mapping" = "Origin:origin,Dest:dest,avgDepDelay:avgDepDelay");

CREATE EXTERNAL TABLE 2_4 (Origin string, Dest string, avgArrDelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "2_4", "dynamodb.column.mapping" = "Origin:origin,Dest:dest,avgArrDelay:avgArrDelay");

CREATE EXTERNAL TABLE 3_2 (route string, tripstartdate string, totalarrdelay double, firstlegdetails string, firstlegarrdelay double, secondlegdetails string, secondlegarrdelay double) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ("dynamodb.table.name" = "3_2", "dynamodb.column.mapping" = "route:route,tripstartdate:tripstartdate,totalarrdelay:totalarrdelay,firstlegdetails:firstlegdetails,firstlegarrdelay:firstlegarrdelay,secondlegdetails:secondlegdetails,secondlegarrdelay:secondlegarrdelay");


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
select * from 2_1 where origin='CMI' order by avgDepDelay;
select * from 2_1 where origin='BWI' order by avgDepDelay;
select * from 2_1 where origin='MIA' order by avgDepDelay;
select * from 2_1 where origin='LAX' order by avgDepDelay;
select * from 2_1 where origin='IAH' order by avgDepDelay;
select * from 2_1 where origin='SFO' order by avgDepDelay;


2.2
select * from 2_2 where origin='CMI' order by avgDepDelay;
select * from 2_2 where origin='BWI' order by avgDepDelay;
select * from 2_2 where origin='MIA' order by avgDepDelay;
select * from 2_2 where origin='LAX' order by avgDepDelay;
select * from 2_2 where origin='IAH' order by avgDepDelay;
select * from 2_2 where origin='SFO' order by avgDepDelay;

2.4
SELECT * FROM 2_4 WHERE ORIGIN='CMI' AND DEST='ORD'; 
SELECT * FROM 2_4 WHERE ORIGIN='IND' AND DEST='CMH'; 
SELECT * FROM 2_4 WHERE ORIGIN='DFW' AND DEST='IAH'; 
SELECT * FROM 2_4 WHERE ORIGIN='LAX' AND DEST='SFO'; 
SELECT * FROM 2_4 WHERE ORIGIN='JFK' AND DEST='LAX'; 
SELECT * FROM 2_4 WHERE ORIGIN='ATL' AND DEST='PHX'; 

3.2
select * from 3_2 where route='CMI-ORD-LAX' and tripstartdate='2008-03-04';
select * from 3_2 where route='JAX-DFW-CRP' and tripstartdate='2008-09-09';
select * from 3_2 where route='SLC-BFL-LAX' and tripstartdate='2008-04-01';
select * from 3_2 where route='LAX-SFO-PHX' and tripstartdate='2008-07-12';
select * from 3_2 where route='DFW-ORD-DFW' and tripstartdate='2008-06-10';
select * from 3_2 where route='LAX-ORD-JFK' and tripstartdate='2008-01-01';