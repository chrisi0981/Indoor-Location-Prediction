#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys

import datetime
import time
import numpy
import re

from operator import itemgetter
from functools import partial

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Prepare_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")

	fields = ['room','date_time','timestamp','occupied']
	
	result = dbHandler.select("SELECT room,time,occupied FROM Occupancy_Data WHERE occupied IS NOT NULL")
	
	data_insert = []
	
	for row in result:
		
		if len(data_insert) > 5000:
			dbHandler.insert_bulk("Occupancy_Data_final",fields,data_insert)
			data_insert = []
			
		tmp = []
		tmp.append(row[0])
		
		data_time = row[1].split(" ")		
		date = map(int,data_time[0].split("/"))
		
		new_date = "2015-"
		
		if date[0] < 10:
			new_date = "%s0%i-" % (new_date,date[0])
		else:
			new_date = "%s%i-" % (new_date,date[0])
		
		if date[1] < 10:
			new_date = "%s0%i" % (new_date,date[1])
		else:
			new_date = "%s%i" % (new_date,date[1])
			
		time_field = map(int,data_time[1].split(":"))
		
		new_time = ""
		
		if time_field[0] < 10:
			new_time = "%s0%i:" % (new_time,time_field[0])
		else:
			new_time = "%s%i:" % (new_time,time_field[0])
			
		if time_field[1] < 10:
			new_time = "%s0%i:00" % (new_time,time_field[1])
		else:
			new_time = "%s%i:00" % (new_time,time_field[1])
			
		tmp.append("%s %s" % (new_date,new_time))		
		tmp.append(long(time.mktime(datetime.datetime(2015,date[0],date[1],time_field[0],time_field[1],0).timetuple())))
		
		if int(row[2]) <= 1:
			tmp.append(int(row[2]))
		else:
			tmp.append(1)
		
		data_insert.append(tmp)		
	
	dbHandler.insert_bulk("Occupancy_Data_final",fields,data_insert)
	
	fields = ['room','date_time','timestamp','temperature']
	
	result = dbHandler.select("SELECT room,time,temperature FROM Temperature_Data WHERE temperature IS NOT NULL")
	
	data_insert = []
	
	for row in result:
		
		if len(data_insert) > 5000:
			dbHandler.insert_bulk("Temperature_Data_final",fields,data_insert)
			data_insert = []
			
		tmp = []
		tmp.append(row[0])
		
		data_time = row[1].split(" ")		
		date = map(int,data_time[0].split("/"))
		
		new_date = "2015-"
		
		if date[0] < 10:
			new_date = "%s0%i-" % (new_date,date[0])
		else:
			new_date = "%s%i-" % (new_date,date[0])
		
		if date[1] < 10:
			new_date = "%s0%i" % (new_date,date[1])
		else:
			new_date = "%s%i" % (new_date,date[1])
			
		time_field = map(int,data_time[1].split(":"))
		
		new_time = ""
		
		if time_field[0] < 10:
			new_time = "%s0%i:" % (new_time,time_field[0])
		else:
			new_time = "%s%i:" % (new_time,time_field[0])
			
		if time_field[1] < 10:
			new_time = "%s0%i:00" % (new_time,time_field[1])
		else:
			new_time = "%s%i:00" % (new_time,time_field[1])
			
		tmp.append("%s %s" % (new_date,new_time))		
		tmp.append(long(time.mktime(datetime.datetime(2015,date[0],date[1],time_field[0],time_field[1],0).timetuple())))
		tmp.append(float(row[2]))
		
		data_insert.append(tmp)
	
	dbHandler.insert_bulk("Temperature_Data_final",fields,data_insert)

def Define_User_List():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")
	
	result = dbHandler.select("SELECT DISTINCT room FROM Occupancy_Data")
	
	rooms = []

	for row in result:
		rooms.append((int(filter(str.isdigit,row[0])),row[0]))

	rooms = sorted(rooms,key=itemgetter(0))
	
	fields = ['room_id','room_occupancy_tag']
	
	for room in rooms:
		dbHandler.insert("Rooms",fields,[room[0],room[1]])
		
	result = dbHandler.select("SELECT DISTINCT room FROM Temperature_Data")
	
	for row in result:
		dbHandler.update("UPDATE Rooms SET room_temperature_tag = '%s' WHERE room_id = %i" % (row[0],int(filter(str.isdigit,row[0]))))
		

def Correlate_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")
	
	fields = ['room_id','date_time','timestamp','occupied','temperature']
	
	result = dbHandler.select("SELECT * FROM Rooms WHERE room_id != 5403")
	
	rooms = []
	
	for row in result:
		rooms.append((row[1],row[2],row[3]))
		
	
	for room in rooms:
		
		result = dbHandler.select("SELECT date_time,timestamp,occupied FROM Occupancy_Data_final WHERE room = '%s'" % (room[1]))
		
		occupancy_data = []
		
		for row in result:
			occupancy_data.append((row[0],int(row[1]),int(row[2])))
		
		result = dbHandler.select("SELECT date_time,timestamp,temperature FROM Temperature_Data_final WHERE room = '%s'" % (room[2]))
		
		temperature_data = []
		
		for row in result:
			temperature_data.append((row[0],int(row[1]),float(row[2])))
			
		
		bulk_insert = []
		
		for data_point in occupancy_data:
			
			if len(bulk_insert) > 5000:
				dbHandler.insert_bulk("GHC_Data",fields,bulk_insert)
				bulk_insert = []
			
			closest = min(temperature_data, key=partial(L1_Distance,data_point))
			
			tmp = []
			tmp.append(room[0])
			tmp.append(data_point[0])
			tmp.append(data_point[1])
			tmp.append(data_point[2])
			tmp.append(closest[2])
			
			bulk_insert.append(tmp)
			
		dbHandler.insert_bulk("GHC_Data",fields,bulk_insert)
	
def L1_Distance(x,y):
	
	return abs(x[1]-y[1])

def Prepare_Event_Data():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")	
	
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration']
	
	rooms = []
	
	#result = dbHandler.select("SELECT DISTINCT room_id FROM GHC_Data")
	result = dbHandler.select("SELECT DISTINCT room_id FROM MicroclimateControl_Data.Room_IDs")
	
	for row in result:
		rooms.append(int(row[0]))
		
	room_id = 0		
	
	for room_id in rooms:					
		
		# Timestamp,Date,Time,Time Index,DoW,Occupancy		
		#result = dbHandler.select("SELECT timestamp,FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') AS date,FROM_UNIXTIME(timestamp,'%%H:%%i:%%s') as time,FLOOR((HOUR(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s'))*60+MINUTE(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s')))/5) AS time_index,WEEKDAY(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')) AS dow,occupied,DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),MONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),CEIL(DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d'))/7) FROM GHC_Data WHERE room_id = %i ORDER BY timestamp" % (room_id))
		result = dbHandler.select("SELECT timestamp,FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') AS date,FROM_UNIXTIME(timestamp,'%%H:%%i:%%s') as time,FLOOR((HOUR(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s'))*60+MINUTE(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s')))/5) AS time_index,WEEKDAY(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')) AS dow,current_occupancy,DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),MONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),CEIL(DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d'))/7) FROM MicroclimateControl_Data.LeaveTime_Feature_Matrix_%i WHERE threshold = 1  ORDER BY timestamp" % (room_id))
		
		data = []
				
		for row in result:
			data.append(row)		
			
		
		if len(data) > 0:			
			
			new_data = []
			new_test_data = []
			
			last_occupancy = ""
			transition_time = -1
			last_duration = 0
			transition_time_index = -1
			
			for data_point in data:								
				
				if last_occupancy == "":
					last_occupancy = int(data_point[5])
					transition_time = int(data_point[0])
					transition_time_index = int(data_point[3])
					
				if last_occupancy != int(data_point[5]):
					
					duration = ((int(data_point[0])-transition_time)/60)/5
					season = 0
					week_weekend = 0
					
					if int(data_point[4]) <= 4:
						week_weekend = 1
					
					if int(data_point[7]) < 4: 
						season = 0 # Winter
						
					if int(data_point[7]) >= 4 and int(data_point[7]) < 7:
						season = 1 # Spring
						
					if int(data_point[7]) >= 7 and int(data_point[7]) < 10:
						season = 2 # Summer
						
					if int(data_point[7]) >= 10:
						season = 3 # Fall
					
					for current_time in range(((int(data_point[0])-transition_time)/60)/5):
					
						current_timestamp = datetime.datetime.fromtimestamp(int(float(data_point[0])-(duration - current_time)*5*60))
					
						tmp = []
						tmp.append(room_id)
						tmp.append(float(data_point[0])-(duration - current_time)*5*60)
						tmp.append(current_timestamp.strftime('%Y-%m-%d'))
						tmp.append(current_timestamp.strftime('%H:%M:%S'))						
						tmp.append(Get_Time_Index(tmp[-1].split(":")))
						
						if current_timestamp.weekday() > 4:
							tmp.append(0)
						else:
							tmp.append(1)
							
						tmp.append(current_timestamp.weekday())
						tmp.append(last_occupancy)
						tmp.append(current_time)
						tmp.append(transition_time_index)
						tmp.append(last_duration)						
						
						new_data.append(tmp)					
					
					last_occupancy = int(data_point[5])
					last_duration = duration
					transition_time = int(data_point[0])
					transition_time_index = int(data_point[3])
				
				if len(new_data) > 5000:
					dbHandler.insert_bulk("GHC_Occupancy_Data",data_fields,new_data)
					new_data = []
					
			dbHandler.insert_bulk("GHC_Occupancy_Data",data_fields,new_data)
		
def Get_Time_Index(current_time):
	
	hour = int(current_time[0])
	minute = int(current_time[1])
	
	return int(math.floor(float(hour*60 + minute)/5))

def Prepare_Room_ID():
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction_Study")
	
	fields = ['room_id','room_name','occupant_job','number_occupants']
	
	room_data = {}
	
	result = dbHandler.select("SELECT * FROM roomOccupation")
	
	for row in result:
		
		tmp = []
		tmp.append(row[2])
		tmp.append(int(row[3]))
		
		room_data[row[1]] = tmp
				
	result = dbHandler.select("SELECT * FROM Room_IDs")
	
	for row in result:
		
		room_number = re.findall('\d+', row[1], flags=0)[0]
		
		new_row = list(row)
		
		if room_number in room_data:
			new_row.append(room_data[room_number][0])
			new_row.append(room_data[room_number][1])
		else:
			new_row.append("")
			new_row.append(0)
			
		dbHandler.insert("Room_IDs_final", fields, new_row)

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	#USER = int(sys.argv[3])
	
	#Prepare_Data()
	#Define_User_List()
	#Correlate_Data()
	#Prepare_Event_Data()
	Prepare_Room_ID()