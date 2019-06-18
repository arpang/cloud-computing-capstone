import json
import csv
from os import listdir


path = '/Users/aragrawa/personal/courses/capstone-cloud-computing/data/csv/'
filenames = listdir(path)
filecount = 0

default_number = '-100000'
default_bool = '0'


for csvfilename in filenames: 
	filecount += 1
	print "Processing file #: " + str(filecount) + " " + csvfilename
	csvfile = open(path + csvfilename, 'r')
	jsonfilename = csvfilename.split('.')[0] + '.json'
	jsonfile = open(path + jsonfilename, 'w')
	reader = csv.DictReader(csvfile)
	for row in reader:
		projected_row = {}
		projected_row['FlightDate'] = row['FlightDate']
		projected_row['UniqueCarrier'] = row['UniqueCarrier']
		projected_row['Origin'] = row['Origin']
		projected_row['Dest'] = row['Dest']
		projected_row['FlightNum'] = row['FlightNum']

		projected_row['Year'] = row['Year'] if row['Year']!= "" else default_number
		projected_row['Quarter'] = row['Quarter'] if row['Quarter']!= "" else default_number
		projected_row['Month'] = row['Month'] if row['Month']!= "" else default_number
		projected_row['DayofMonth'] = row['DayofMonth'] if row['DayofMonth']!= "" else default_number
		projected_row['DayOfWeek'] = row['DayOfWeek'] if row['DayOfWeek']!= "" else default_number

		projected_row['CRSDepTime'] = row['CRSDepTime'] if row['CRSDepTime']!= "" else default_number
		projected_row['DepTime'] = row['DepTime'] if row['DepTime']!= "" else default_number
		projected_row['DepDelay'] = row['DepDelay'] if row['DepDelay']!= "" else default_number
		projected_row['DepDelayMinutes'] = row['DepDelayMinutes'] if row['DepDelayMinutes']!= "" else default_number
		
		projected_row['CRSArrTime'] = row['CRSArrTime'] if row['CRSArrTime']!= "" else default_number
		projected_row['ArrTime'] = row['ArrTime'] if row['ArrTime']!= "" else default_number
		projected_row['ArrDelay'] = row['ArrDelay'] if row['ArrDelay']!= "" else default_number
		projected_row['ArrDelayMinutes'] = row['ArrDelayMinutes'] if row['ArrDelayMinutes']!= "" else default_number

		projected_row['Cancelled'] = row['Cancelled'] if row['Cancelled']!= "" else default_bool
		projected_row['Diverted'] = row['Diverted'] if row['Diverted']!= "" else default_bool

		projected_row['Year'] = int(projected_row['Year'])
		projected_row['Quarter'] = int(projected_row['Quarter'])
		projected_row['Month'] = int(projected_row['Month'])
		projected_row['DayofMonth'] = int(projected_row['DayofMonth'])
		projected_row['DayOfWeek'] = int(projected_row['DayOfWeek'])

		projected_row['CRSDepTime'] = int(projected_row['CRSDepTime'])
		projected_row['DepTime'] = int(projected_row['DepTime'])
		projected_row['DepDelay'] = float(projected_row['DepDelay'])
		projected_row['DepDelayMinutes'] = float(projected_row['DepDelayMinutes'])
		
		projected_row['CRSArrTime'] = int(projected_row['CRSArrTime'])
		projected_row['ArrTime'] = int(projected_row['ArrTime'])
		projected_row['ArrDelay'] = float(projected_row['ArrDelay'])
		projected_row['ArrDelayMinutes'] = float(projected_row['ArrDelayMinutes'])
		
		projected_row['Cancelled'] = bool(float(projected_row['Cancelled']))
		projected_row['Diverted'] = bool(float(projected_row['Diverted']))
		json.dump(projected_row, jsonfile)
		jsonfile.write('\n')
	jsonfile.close()