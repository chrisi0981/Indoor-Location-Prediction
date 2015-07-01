#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys
from multiprocessing import Process
from multiprocessing import Pool
import os
import thread
import threading

import random
import itertools

import time
import datetime
from pytz import timezone

import numpy
from collections import Counter
from operator import itemgetter
#import probstat
import pickle
import gc
#import pp

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Prepare_Data(room_id):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")
	dbHandler_data = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction_Study")
	
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','singular_pattern_id']
	
	sp,sp_duration = Get_Singular_Patterns(room_id)
	
	# Timestamp,Date,Time,Time Index,DoW,Occupancy		
	result = dbHandler.select("SELECT timestamp,FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') AS date,FROM_UNIXTIME(timestamp,'%%H:%%i:%%s') as time,FLOOR((HOUR(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s'))*60+MINUTE(FROM_UNIXTIME(timestamp,'%%H:%%i:%%s')))/5) AS time_index,WEEKDAY(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')) AS dow,current_location,DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),MONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d')),CEIL(DAYOFMONTH(FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d'))/7) FROM GHC_Occupancy_Data WHERE user_id = %i AND FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') >= '2011-11-01' AND FROM_UNIXTIME(timestamp,'%%Y-%%m-%%d') < '2012-01-01' ORDER BY timestamp" % (room_id))
	
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
				last_occupancy = Get_Occupancy(data_point[5])
				transition_time = int(data_point[0])
				transition_time_index = int(data_point[3])
				
			if last_occupancy != Get_Occupancy(data_point[5]):
				
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
											
					singular_patterns = []
					
					activities = ["Transition-To","Transition-From","First Arrival","Last Departure","Staying-At","Duration"]
					
					time_index = int(tmp[4])
					time_index_15 = int(math.floor(float(tmp[4])/3))
					
					for poss_location in range(2):
						for activity in range(6):
							temporal_offset = 0
							sp_tmp = []
							
							if activity < 5:
								# Daily
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
								
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range): # Maximum search radius of 30 mins
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][0]) > 0:
										lower = search_ti-k
										break
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][0]) > 0:
										upper = search_ti+k+1
										break
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][0]									
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][0]										
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][0]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][0]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][0]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][0]
								
							if activity < 5:
								# Day of Week
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
							
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range):
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][3][int(tmp[6])]) > 0:
										lower = search_ti-k
										break								
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][3][int(tmp[6])]) > 0:
										upper = search_ti+k+1
										break
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][3][int(tmp[6])]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][3][int(tmp[6])]									
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][3][int(tmp[6])]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][3][int(tmp[6])]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][3][int(tmp[6])]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][3][int(tmp[6])]
							
							if activity < 5:
								
								# Day of Month
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
								
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range):
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]) > 0:
										lower = search_ti-k
										break
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]) > 0:
										upper = search_ti+k+1
										break																
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]									
								else:
									if upper != -1:										
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]
								
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][5][tmp[5]][int(current_timestamp.strftime('%d'))-1]
							
							if activity < 5:
								
								# Season
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
								
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range):
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][2][season]) > 0:
										lower = search_ti-k
										break
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][2][season]) > 0:
										upper = search_ti+k+1
										break
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][2][season]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][2][season]
								
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][2][season]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][2][season]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][2][season]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][2][season]
							
							if activity < 5:
								
								# Week vs. Weekend
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
								
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range):
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][1][tmp[5]]) > 0:
										lower = search_ti-k
										break
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][1][tmp[5]]) > 0:
										upper = search_ti+k+1
										break
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][1][tmp[5]]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][1][tmp[5]]									
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][1][tmp[5]]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][1][tmp[5]]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][1][tmp[5]]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][1][tmp[5]]
							
							if activity < 5:
								
								# Number in Month
								lower = -1
								upper = -1
								
								search_range = 6
								search_ti = time_index
								
								if activity == 4:
									search_range = 2
									search_ti = time_index_15
								
								for k in range(search_range):
									if (search_ti-k) >= 0 and len(sp[poss_location][activity][search_ti-k][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]) > 0:
										lower = search_ti-k
										break
										
								search_range = 5
								bound = 287
								
								if activity == 4:
									search_range = 1
									bound = 95
								
								for k in range(search_range):
									if (search_ti+(k+1)) <= bound and len(sp[poss_location][activity][search_ti+(k+1)][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]) > 0:
										upper = search_ti+k+1
										break
								
								if (search_ti-lower) <= math.fabs(upper-search_ti) and lower != -1:
									sp_tmp = sp_tmp + sp[poss_location][activity][lower][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp[poss_location][activity][upper][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]
							else:
								# Duration
								lower = -1
								upper = -1
								
								for k in range(current_time+1):
									if (current_time-k) < len(sp_duration[poss_location]) and len(sp_duration[poss_location][current_time-k][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]) > 0:
										lower = current_time-k
										break
										
								for k in range(current_time+1,len(sp_duration[poss_location])):
									if len(sp_duration[poss_location][k][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]) > 0:
										upper = k
										break
								
								if (current_time-lower) <= math.fabs(upper-current_time) and lower != -1:
									sp_tmp = sp_tmp + sp_duration[poss_location][lower][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]
								else:
									if upper != -1:
										sp_tmp = sp_tmp + sp_duration[poss_location][upper][4][tmp[6]][int(math.ceil(int(current_timestamp.strftime('%d'))/7))]
							
							singular_patterns = singular_patterns + sp_tmp
					
					tmp.append(','.join(map(str,singular_patterns)))
					
					new_data.append(tmp)					
				
				last_occupancy = Get_Occupancy(data_point[5])
				last_duration = duration
				transition_time = int(data_point[0])
				transition_time_index = int(data_point[3])
			
			if len(new_data) > 5000:
				dbHandler_data.insert_bulk("GHC_Test_Data",data_fields,new_data)
				new_data = []
				
		dbHandler_data.insert_bulk("GHC_Test_Data",data_fields,new_data)
		
def Get_Time_Index(current_time):
	
	hour = int(current_time[0])
	minute = int(current_time[1])
	
	return int(math.floor(float(hour*60 + minute)/5))

def Get_Occupancy(occupancy):
	
	return int(occupancy)
	
	"""
	if occupancy == 'Occupied':
		return 1
	
	if occupancy == 'Unoccupied':
		return 0
	"""

def Get_Singular_Patterns(user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")
		
		
	result = dbHandler.select("SELECT max(time) FROM Singular_Pattern_Base WHERE activity = 'Duration' AND user_id = %i" % (user))
		
	max_duration = 0
	
	for row in result:
		if row[0] != None:
			max_duration = int(row[0])
	
	singular_patterns = []
	
	for j in range(2):
		activities = []
	
		for j in range(5):
			time = []
		
			bound = 288
			
			if j == 4:
				bound = 96
		
			for j in range(bound):
				specificity = []

				specificity.append([]) # Daily
				
				tmp = []
				tmp.append([])
				tmp.append([])
				specificity.append(tmp) # Week vs. Weekend
				
				tmp = []
				
				for k in range(4):
					tmp.append([])
					
				specificity.append(tmp) # Season
				
				tmp = []
				
				for k in range(7):
					tmp.append([])
					
				specificity.append(tmp) # Weekday
				
				tmp = []
				
				for k in range(7):
					tmp_1 = []
					
					for l in range(7):
						tmp_1.append([])
					
					tmp.append(tmp_1)
					
				specificity.append(tmp) # Number in Month
				
				tmp = []
				
				for l in range(2):
					tmp_1 = []
					
					for k in range(31):
						tmp_1.append([])
					
					tmp.append(tmp_1)
					
				specificity.append(tmp) # Day of Month
				
				time.append(specificity)
			
			activities.append(time)
		
		singular_patterns.append(activities)
			
		
	singular_patterns_duration = []
		
	locations = []
	
	for j in range(2):
		time = []
	
		for j in range(max_duration):
			specificity = []

			specificity.append([]) # Daily
			
			tmp = []
			tmp.append([])
			tmp.append([])
			specificity.append(tmp) # Week vs. Weekend
			
			tmp = []
			
			for k in range(4):
				tmp.append([])
				
			specificity.append(tmp) # Season
			
			tmp = []
			
			for k in range(7):
				tmp.append([])
				
			specificity.append(tmp) # Weekday
			
			tmp = []
			
			for k in range(7):
				tmp_1 = []
				
				for l in range(7):
					tmp_1.append([])
				
				tmp.append(tmp_1)
				
			specificity.append(tmp) # Number in Month
			
			tmp = []
			
			for l in range(2):
				tmp_1 = []
				
				for k in range(31):
					tmp_1.append([])
				
				tmp.append(tmp_1)
				
			specificity.append(tmp) # Day of Month
			
			time.append(specificity)
		
		singular_patterns_duration.append(time)
		
		
	result = dbHandler.select("SELECT * FROM Singular_Pattern_Base WHERE user_id = %i AND LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2" % (user))
	
	"""	
	Singular Pattern Schema:	
		Location -> Activity -> Time -> Specificity -> Pattern ID
		
	Singular Pattern Duration Schema:	
		Location -> Duration -> Specificity -> Pattern ID
	"""
	
	for row in result:
		
		if row[3] != 'Duration':
			if int(row[14]) == 0:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])].append(int(row[2])) # Daily
				
			if int(row[14]) == 1:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[12])].append(int(row[2])) # Week vs. Weekend
				
			if int(row[14]) == 2:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[11])].append(int(row[2])) # Season
				
			if int(row[14]) == 3:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[8])].append(int(row[2])) # Day of Week
				
			if int(row[14]) == 4:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[8])][int(row[13])-1].append(int(row[2])) # Number in Month
				
			if int(row[14]) == 5:
				singular_patterns[int(row[4])][Get_Activity_Code(row[3])][int(row[5])][int(row[14])][int(row[12])][int(row[9])-1].append(int(row[2])) # Day of Month
		else:			
			if int(row[14]) == 0:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])].append(int(row[2])) # Daily
				
			if int(row[14]) == 1:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[12])].append(int(row[2])) # Week vs. Weekend
				
			if int(row[14]) == 2:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[11])].append(int(row[2])) # Season
				
			if int(row[14]) == 3:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[8])].append(int(row[2])) # Day of Week
				
			if int(row[14]) == 4:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[8])][int(row[13])-1].append(int(row[2])) # Number in Month
				
			if int(row[14]) == 5:
				singular_patterns_duration[int(row[4])][int(row[5])-1][int(row[14])][int(row[12])][int(row[9])-1].append(int(row[2])) # Day of Month
		
			
	return singular_patterns,singular_patterns_duration

def Get_Activity_Code(activity):
	
	if activity == 'Transition-To':
		return 0
	
	if activity == 'Transition-From':
		return 1		
	
	if activity == 'First Arrival':
		return 2
	
	if activity == 'Last Departure':
		return 3
	
	if activity == 'Staying-At':
		return 4

def Gradient_Descent(user,pattern_length,look_ahead,epsilon,data):
	
	pass

def Predict_Occupancy(user,pattern_length,look_ahead,ensemble,processes,eps):
	
	data_fields = ['user_id','timestamp','date','time','time_index','week_weekend','current_dow','current_location','current_duration','transition_time','last_duration','singular_pattern_id','occupancy_30','prob_30','pattern_30','pattern_length']
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction_Study")
	
	dbHandler.update("SET global max_allowed_packet=104857600")
	
	"""
	
	Load extracted patterns!
	
	"""
	#dbHandler.createTable("Pattern_Base.Pattern_Base_%i" % (user),"SELECT * FROM Pattern_Base_template")
	dbHandler.truncateTable("Singular_Pattern_Base")
	dbHandler.update("INSERT INTO Singular_Pattern_Base SELECT * FROM Pattern_Base_Study.Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i" % (user))
		
	max_sp_id = -1
	
	result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		max_sp_id = int(row[0])
		
	singular_patterns_prob = numpy.zeros(max_sp_id,float)
	singular_patterns_occ = numpy.zeros(max_sp_id,int)
	singular_patterns_time = [-1 for k in range(max_sp_id)]
	singular_patterns_frequency = [-1 for k in range(max_sp_id)]
	singular_patterns_specificity = [-1 for k in range(max_sp_id)]
	singular_patterns_duration = [-1 for k in range(max_sp_id)]
	singular_patterns_activity = numpy.zeros(max_sp_id,int)
	
	result = dbHandler.select("SELECT pattern_id,probability,location,time,LENGTH(members) - LENGTH(REPLACE(members, ',', ''))-1,specificity,activity FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		singular_patterns_prob[int(row[0])-1] = float(row[1])
		singular_patterns_occ[int(row[0])-1] = int(row[2])
		singular_patterns_time[int(row[0])-1] = int(row[3])
		singular_patterns_frequency[int(row[0])-1] = int(row[4])
		singular_patterns_specificity[int(row[0])-1] = int(row[5])
		
		if row[6] == "Duration":
			singular_patterns_duration[int(row[0])-1] = 1
		else:
			singular_patterns_duration[int(row[0])-1] = 0
			
		if row[6] == "Staying-At":
			singular_patterns_activity[int(row[0])-1] = 1
	
	# Specificity weight: 0.14285714
	occupied = numpy.nonzero(singular_patterns_occ)[0]
	not_occupied = numpy.where(numpy.array(singular_patterns_occ) == 0)[0]
			
	result = dbHandler.select("SELECT pattern_members,probability,ROUND(specificity/0.14285714)-1,TRIM(TRAILING ',' FROM SUBSTRING_INDEX(pattern_members, ',',-2)),count,ROUND(count/probability) FROM Pattern_Base_Study.Pattern_Base_%i" % (user))
	
	valid_patterns = {}
	valid_patterns_mod = [{} for k in range(max_sp_id)]
	
	for row in result:
		
		try:
			current_values = valid_patterns[row[0]]
			current_values[int(row[2])] = float(row[1])
			current_values[7] = int(row[3])			
			current_values[8+1+int(row[2])] = int(row[4])
			current_values[15+1+int(row[2])] = int(row[5])
			valid_patterns[row[0]] = current_values	
		except KeyError, e:
			tmp = [0 for k in range(23)]
			tmp[int(row[2])] = float(row[1])
			tmp[7] = int(row[3])			
			tmp[8] = numpy.amax([singular_patterns_specificity[k-1] for k in map(int,row[0][1:-1].split(","))])
			tmp[8+1+int(row[2])] = int(row[4])
			tmp[15+1+int(row[2])] = int(row[5])
			valid_patterns[row[0]] = tmp
			
		if row[0] in valid_patterns_mod[int(row[3])-1]:
			current_values = valid_patterns_mod[int(row[3])-1][row[0]]
			current_values[int(row[2])] = float(row[1])
			current_values[7] = int(row[3])
			current_values[8+1+int(row[2])] = int(row[4])
			current_values[15+1+int(row[2])] = int(row[5])		
			valid_patterns_mod[int(row[3])-1][row[0]] = current_values
		else:
			tmp = [0 for k in range(23)]
			tmp[int(row[2])] = float(row[1])
			tmp[7] = int(row[3])
			tmp[8] = numpy.amax([singular_patterns_specificity[k-1] for k in map(int,row[0][1:-1].split(","))])
			tmp[8+1+int(row[2])] = int(row[4])
			tmp[15+1+int(row[2])] = int(row[5])
			valid_patterns_mod[int(row[3])-1][row[0]] = tmp
			
	
	result = dbHandler.select("SELECT user_id,timestamp,date,time,time_index,week_weekend,current_dow,current_location,current_duration,transition_time,last_duration,singular_pattern_id,WEEK(date),MONTH(date),WEEKDAY(date) FROM GHC_Test_Data WHERE user_id = %i ORDER BY timestamp" % (user))
	
	window = []	
	
	patterns = []
	dates = []
	days = []
	weeks = []
	months = []
	timestamps = []
	
	result_data = []
	
	weights_mean = [1,1,1,1,1,1,1]
	weights_median = [1,1,1,1,1,1,1]
	weights_max = [1,1,1,1,1,1,1]
	weights_weighted_max = [1,1,1,1,1,1,1]
	
	last_date = ""	
	
	day_windows = []
	current_windows = []
	
	for row in result:		
		if len(window) < look_ahead:
			window.append(row)
			last_date = row[2]
			
			if row[11] != '':
				data_point_patterns = map(int,row[11].split(","))
				
				if int(row[7]) == 1:
					patterns.append(["%i" % (k) for k in data_point_patterns if (k-1) in occupied and ((singular_patterns_specificity[k-1] < 4 and singular_patterns_frequency[k-1] > 2) or (singular_patterns_specificity[k-1] >= 3))])
				else:
					patterns.append(["%i" % (k) for k in data_point_patterns if (k-1) in not_occupied and ((singular_patterns_specificity[k-1] < 4 and singular_patterns_frequency[k-1] > 2) or (singular_patterns_specificity[k-1] >= 3))])							
				
				date = row[2].split('-')
				dates.append(datetime.date(int(date[0]),int(date[1]),int(date[2])))
				days.append(int(row[14]))
				weeks.append(int(row[12]))
				months.append(int(row[13]))
				timestamps.append(int(row[1]))
		else:					
			target_patterns = row[11].split(',')
			
			if pattern_length == 1:
				
				max_prob = -1
				occ = -1
				predicted_pattern = 0
				
				for target in target_patterns:
					if target != '':
						if singular_patterns_prob[int(target)-1] > max_prob:
							max_prob = singular_patterns_prob[int(target)-1]
							occ = singular_patterns_occ[int(target)-1]
							predicted_pattern = int(target)
						
				row = list(row)
				row.append(occ)
				row.append(max_prob)
				row.append(predicted_pattern)
				row.append(pattern_length)
				
				result_data.append(row)
			else:					
				if last_date != row[2]:
					current_windows.append([k for k in window])
					day_windows.append(current_windows)
					current_windows = []
					window = []
					window.append(row)
				else:							
					current_windows.append([k for k in window])
					
				last_date = row[2]	
					
			if window[0][11] != '':						
				data_point_patterns = map(int,window[0][11].split(","))
				
				if int(window[0][7]) == 1:
					patterns.append(["%i" % (k) for k in data_point_patterns if (k-1) in occupied and ((singular_patterns_specificity[k-1] < 4 and singular_patterns_frequency[k-1] > 2) or (singular_patterns_specificity[k-1] >= 3))])							
				else:
					patterns.append(["%i" % (k) for k in data_point_patterns if (k-1) in not_occupied and ((singular_patterns_specificity[k-1] < 4 and singular_patterns_frequency[k-1] > 2) or (singular_patterns_specificity[k-1] >= 3))])							
					
				date = window[0][2].split('-')
				dates.append(datetime.date(int(date[0]),int(date[1]),int(date[2])))
				days.append(int(window[0][14]))
				weeks.append(int(window[0][12]))
				months.append(int(window[0][13]))
				timestamps.append(int(window[0][1]))
				
			window.pop(0)
			window.append(row)
			
	current_windows.append([k for k in window])
	day_windows.append(current_windows)		
	
	"""
	======================= Prediction Step =======================
	"""
	
	fields = ['user_id',
			  'timestamp',
			  'date',
			  'time',
			  'time_index',
			  'week_weekend',
			  'current_dow',
			  'current_location',
			  'current_duration',
			  'transition_time',
			  'last_duration',
			  'singular_pattern_id',
			  'mean_prediction',
			  'mean_condition',
			  'mean_probability',
			  'mean_weight',
			  'median_prediction',
			  'median_condition',
			  'median_probability',
			  'median_weight',
			  'max_prediction',
			  'max_condition',
			  'max_probability',
			  'max_weight',
			  'max_pattern',
			  'max_weighted_prediction',
			  'max_weighted_probability',
			  'max_weighted_pattern',
			  'ensemble_prediction',
			  'ensemble_frequency',
			  'ensemble_probability']
	
	fields_SP = ['user_id',
			  'timestamp',
			  'date',
			  'time',
			  'time_index',
			  'week_weekend',
			  'current_dow',
			  'current_location',
			  'current_duration',
			  'transition_time',
			  'last_duration',
			  'singular_pattern_id',			  
			  'prediction',
			  'probability',
			  'chosen_pattern',
			  'specificity',
			  'feature_combination']
	
	pattern_fields = ['user_id',
					  'timestamp',
					  'temporal_condition',
					  'pattern_length',
					  'patterns']		
				
		
	start = time.time()			
		
	#mean_tmp,median_tmp,max_tmp,max_weighted_tmp,prediction_result,pattern_result = Predict_Day(day_windows[0],pattern_length,dates,weeks,months,timestamps,patterns,valid_patterns,valid_patterns_mod,singular_patterns_occ,[0,1],0.5,weights_mean,weights_median,weights_max,weights_weighted_max,eps,singular_patterns_prob,singular_patterns_duration,singular_patterns_specificity,singular_patterns_activity,singular_patterns_frequency)
	#dbHandler.insert_bulk("GHC_Test_Result",fields,prediction_result)
	
	
	pool = Pool(processes)	
	current_counter = 0
	
	while current_counter < len(day_windows):#
	
		threads = []
		
		for k in range(processes):
			if current_counter + k < len(day_windows):#
				threads.append(pool.apply_async(Predict_Day, (day_windows[current_counter+k],pattern_length,dates,weeks,months,timestamps,patterns,valid_patterns,valid_patterns_mod,singular_patterns_occ,[0,1],0.1,weights_mean,weights_median,weights_max,weights_weighted_max,eps,singular_patterns_prob,singular_patterns_duration,singular_patterns_specificity,singular_patterns_activity,singular_patterns_frequency,)))
				#threads.append(pool.apply_async(Predict_Day_SP, (day_windows[current_counter+k],pattern_length,dates,weeks,months,timestamps,patterns,valid_patterns,valid_patterns_mod,singular_patterns_occ,[0,1],0.1,weights_mean,weights_median,weights_max,eps,singular_patterns_prob,singular_patterns_duration,singular_patterns_specificity,)))
		
		mean_result = []
		median_result = []
		max_result = []
		max_weighted_result = []
		
		for thread in threads:			
			mean_tmp,median_tmp,max_tmp,max_weighted_tmp,prediction_result,pattern_result = thread.get()
			dbHandler.insert_bulk("GHC_Test_Result",fields,prediction_result)
			#prediction_result = thread.get()
			#dbHandler.insert_bulk("GHC_Test_Result_SP",fields_SP,prediction_result)
				
			mean_result.append(mean_tmp)
			median_result.append(median_tmp)
			max_result.append(max_tmp)
			max_weighted_result.append(max_weighted_tmp)
			
		for k in range(3):#7
			weights_mean[k] = numpy.mean(map(itemgetter(k),mean_result))
			weights_median[k] = numpy.mean(map(itemgetter(k),median_result))
			weights_max[k] = numpy.mean(map(itemgetter(k),max_result))
			weights_weighted_max[k] = numpy.mean(map(itemgetter(k),max_weighted_result))
			
		current_counter = current_counter + len(threads)	
	
	print time.time() - start	
	
	
def Predict_Day(windows,pattern_length,dates,weeks,months,timestamps,patterns,valid_patterns,valid_patterns_mod,singular_patterns_occ,targets,cut_off,weights_mean,weights_median,weights_max,weights_weighted_max,eps,singular_patterns_prob,singular_patterns_duration,singular_patterns_specificity,singular_patterns_activity,singular_patterns_frequency):
	
	temporal_complexity = [[] for k in range(4)]
	
	prediction_result = []
	pattern_result = [[] for k in range(7)]
	last_prediction_correct = False
	
	for window in windows:
	
		if window[-1][11] != '':
			start = time.time()
			
			data_point_patterns = [k for k in map(int,window[-1][11].split(",")) if singular_patterns_duration[k-1] == 0]
			current_data_point_patterns = [k for k in map(int,window[0][11].split(",")) if singular_patterns_duration[k-1] == 0 and singular_patterns_occ[k-1] == int(window[0][7])]
			
			date = window[0][2].split('-')
			date = datetime.date(int(date[0]),int(date[1]),int(date[2]))
			same_day_indices = numpy.where(numpy.array(dates) == date)[0]						
			same_day_indices = [k for k in same_day_indices if timestamps[k] <= window[0][1]]			
			previous_day_indicies = list(same_day_indices) + [k for k in numpy.where(numpy.array(dates) == (date + datetime.timedelta(days=-1)))[0]]						
			previous_2day_indicies = previous_day_indicies + [k for k in numpy.where(numpy.array(dates) == (date + datetime.timedelta(days=-2)))[0]]
			
			same_week_indices = numpy.where(numpy.array(weeks) == window[0][12])[0]
			previous_week_indicies = list(same_week_indices) + [k for k in numpy.where(numpy.array(weeks) == (window[0][12]-1)%53)[0]]
			previous_2week_indicies = previous_week_indicies + [k for k in numpy.where(numpy.array(weeks) == (window[0][12]-2)%53)[0]]
			
			same_month_indices = numpy.where(numpy.array(months) == window[0][13])[0]
			
			temporal_complexity[0].append(time.time() - start)
			
			start = time.time()
			
			#0			
			sd_comb = Create_Pattern_Combinations(patterns,same_day_indices,data_point_patterns,valid_patterns_mod,current_data_point_patterns)			
			#1										
			pd_comb = Create_Pattern_Combinations(patterns,previous_day_indicies,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			#2										
			p2d_comb = Create_Pattern_Combinations(patterns,previous_2day_indicies,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			"""
			#3			
			sw_comb = Create_Pattern_Combinations(patterns,same_week_indices,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			#4										
			pw_comb = Create_Pattern_Combinations(patterns,previous_week_indicies,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			#5			
			p2w_comb = Create_Pattern_Combinations(patterns,previous_2week_indicies,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			
			#6			
			sm_comb = Create_Pattern_Combinations(patterns,same_month_indices,data_point_patterns,valid_patterns_mod,current_data_point_patterns)
			"""
			
			temporal_complexity[1].append(time.time() - start)
			
			start = time.time()
			
			sd_prob = [(k,valid_patterns[k][6],6,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][6],singular_patterns_prob[valid_patterns[k][7]-1],singular_patterns_frequency[valid_patterns[k][7]-1]) for k in sd_comb if valid_patterns[k][6] != 0 and valid_patterns[k][15] >= 3 and valid_patterns[k][6] >= cut_off and singular_patterns_activity[valid_patterns[k][7]-1] == 1]			
			pd_prob = [(k,valid_patterns[k][5],5,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][5],singular_patterns_prob[valid_patterns[k][7]-1],singular_patterns_frequency[valid_patterns[k][7]-1]) for k in pd_comb if valid_patterns[k][5] != 0 and valid_patterns[k][14] >= 3 and valid_patterns[k][5] >= cut_off and singular_patterns_activity[valid_patterns[k][7]-1] == 1]
			p2d_prob = [(k,valid_patterns[k][4],4,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][4],singular_patterns_prob[valid_patterns[k][7]-1],singular_patterns_frequency[valid_patterns[k][7]-1]) for k in p2d_comb if valid_patterns[k][4] != 0 and valid_patterns[k][13] >= 3 and valid_patterns[k][4] >= cut_off and singular_patterns_activity[valid_patterns[k][7]-1] == 1]
			
			"""
			sw_prob = [(k,valid_patterns[k][3],3,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][3],singular_patterns_prob[valid_patterns[k][7]-1]) for k in sw_comb if valid_patterns[k][3] != 0 and valid_patterns[k][12] >= 3]
			pw_prob = [(k,valid_patterns[k][2],2,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][2],singular_patterns_prob[valid_patterns[k][7]-1]) for k in pw_comb if valid_patterns[k][2] != 0 and valid_patterns[k][11] >= 3]
			p2w_prob =[(k,valid_patterns[k][1],1,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][1],singular_patterns_prob[valid_patterns[k][7]-1]) for k in p2w_comb if valid_patterns[k][1] != 0 and valid_patterns[k][10] >= 3]
			
			sm_prob = [(k,valid_patterns[k][0],0,valid_patterns[k][7],singular_patterns_occ[valid_patterns[k][7]-1],valid_patterns[k][0],singular_patterns_prob[valid_patterns[k][7]-1]) for k in sm_comb if valid_patterns[k][0] != 0 and valid_patterns[k][9] >= 3]
			"""
			
			temporal_complexity[2].append(time.time() - start)
			
			"""
			sd_patterns = [k[0] for k in sd_prob if len(k) > 0]
			pd_patterns = [k[0] for k in pd_prob if len(k) > 0]
			p2d_patterns = [k[0] for k in p2d_prob if len(k) > 0]
			sw_patterns = [k[0] for k in sw_prob if len(k) > 0]
			pw_patterns = [k[0] for k in pw_prob if len(k) > 0]
			p2w_patterns = [k[0] for k in p2w_prob if len(k) > 0]
			sm_patterns = [k[0] for k in sm_prob if len(k) > 0]
						
			pattern_result[0] =  pattern_result[0] + sm_patterns
			pattern_result[1] =  pattern_result[1] + p2w_patterns
			pattern_result[2] =  pattern_result[2] + pw_patterns
			pattern_result[3] =  pattern_result[3] + sw_patterns
			pattern_result[4] =  pattern_result[4] + p2d_patterns
			pattern_result[5] =  pattern_result[5] + pd_patterns
			pattern_result[6] =  pattern_result[6] + sd_patterns
			"""						
			
			start = time.time()
			
			target_probabilities = []
			target_max_prob = []
			
			for current_target in targets:
				
				target_tmp = []
				target_max_tmp = []
			
				# Same Day Probability
				target_tmp.append([sd_prob[k] for k in range(len(sd_prob)) if sd_prob[k][4] == current_target and sd_prob[k][5] >= cut_off])
				target_max_tmp.append([sd_prob[k] for k in range(len(sd_prob)) if sd_prob[k][4] == current_target and sd_prob[k][5] >= cut_off])
				
				# One Adjacent Day Probability
				target_tmp.append([pd_prob[k] for k in range(len(pd_prob)) if pd_prob[k][4] == current_target and pd_prob[k][5] >= cut_off])
				target_max_tmp.append([pd_prob[k] for k in range(len(pd_prob)) if pd_prob[k][4] == current_target and pd_prob[k][5] >= cut_off])
				
				# Two Adjacent Day Probability
				target_tmp.append([p2d_prob[k] for k in range(len(p2d_prob)) if p2d_prob[k][4] == current_target and p2d_prob[k][5] >= cut_off])
				target_max_tmp.append([p2d_prob[k] for k in range(len(p2d_prob)) if p2d_prob[k][4] == current_target and p2d_prob[k][5] >= cut_off])
				"""
				# Same Week Probability
				target_tmp.append([sw_prob[k][5] for k in range(len(sw_prob)) if sw_prob[k][4] == current_target and sw_prob[k][5] >= cut_off])
				target_max_tmp.append([sw_prob[k][5]*sw_prob[k][6] for k in range(len(sw_prob)) if sw_prob[k][4] == current_target and sw_prob[k][5] >= cut_off])	
				
				# One Adjacent Week Probability
				target_tmp.append([pw_prob[k][5] for k in range(len(pw_prob)) if pw_prob[k][4] == current_target and pw_prob[k][5] >= cut_off])
				target_max_tmp.append([pw_prob[k][5]*pw_prob[k][6] for k in range(len(pw_prob)) if pw_prob[k][4] == current_target and pw_prob[k][5] >= cut_off])
				
				# Two Adjacent Week Probability
				target_tmp.append([p2w_prob[k][5] for k in range(len(p2w_prob)) if p2w_prob[k][4] == current_target and p2w_prob[k][5] >= cut_off])
				target_max_tmp.append([p2w_prob[k][5]*p2w_prob[k][6] for k in range(len(p2w_prob)) if p2w_prob[k][4] == current_target and p2w_prob[k][5] >= cut_off])
				
				# Same Month Probability
				target_tmp.append([sm_prob[k][5] for k in range(len(sm_prob)) if sm_prob[k][4] == current_target and sm_prob[k][5] >= cut_off])
				target_max_tmp.append([sm_prob[k][5]*sm_prob[k][6] for k in range(len(sm_prob)) if sm_prob[k][4] == current_target and sm_prob[k][5] >= cut_off])
				"""
				target_probabilities.append(target_tmp)
				target_max_prob.append(target_max_tmp)
			
			
			mean_probs = []
			median_probs = []
			max_probs = []
			max_weighted_probs = []
			
			for k in range(len(targets)):
				
				mean_tmp = []
				median_tmp = []
				max_tmp = []
				max_weighted_tmp = []
				
				for l in range(len(target_probabilities[k])):
					if len(target_probabilities[k][l]) > 0:
						mean_tmp.append(numpy.mean(map(lambda x: x[5]*weights_mean[l],target_probabilities[k][l])))
						median_tmp.append(numpy.median(map(lambda x: x[5]*weights_median[l],target_probabilities[k][l])))
						
						max_index = numpy.argmax(map(lambda x: x[5]*weights_max[l],target_probabilities[k][l]))
						max_weighted_index = numpy.argmax(map(lambda x: x[5]*x[6]*weights_weighted_max[l],target_max_prob[k][l]))
						
						max_tmp.append(target_probabilities[k][l][max_index])
						max_weighted_tmp.append(target_probabilities[k][l][max_weighted_index])
					else:
						mean_tmp.append(0)
						median_tmp.append(0)
						max_tmp.append(None)
						max_weighted_tmp.append(None)
				
				mean_probs.append(mean_tmp)
				median_probs.append(median_tmp)
				max_probs.append(max_tmp)
				max_weighted_probs.append(max_weighted_tmp)
				
			#print mean_probs
			#print median_probs
			#print max_probs
			#print max_weighted_probs
			
			mean_prediction = []
			mean_prediction_prob = []
			
			median_prediction = []
			median_prediction_prob = []
			
			max_prediction = []
			max_prediction_prob = []
			
			max_weighted_prediction = []
			max_weighted_prob = []
			
			for l in range(3):#7
				mean_prediction.append(targets[numpy.argmax(map(itemgetter(l),mean_probs))])
				mean_prediction_prob.append(numpy.amax(map(itemgetter(l),mean_probs)))
				median_prediction.append(targets[numpy.argmax(map(itemgetter(l),median_probs))])
				median_prediction_prob.append(numpy.amax(map(itemgetter(l),median_probs)))
				
				max_tmp = []
				
				for k in max_probs:
					if  k[l] != None:
						max_tmp.append(k[l][5]*weights_max[l])
					else:
						max_tmp.append(0)
				
				max_weighted_tmp = []
				
				for k in max_weighted_probs:
					if k[l] != None:
						max_weighted_tmp.append(k[l][5]*k[l][6]*weights_weighted_max[l])
					else:
						max_weighted_tmp.append(0)								
					
				weighted_pick = 0
				
				if numpy.amax(max_weighted_tmp) != numpy.amin(max_weighted_tmp):
					tmp = numpy.argmax(max_weighted_tmp)
					weighted_pick = tmp			
					max_weighted_prediction.append(max_weighted_probs[tmp][l])
					max_weighted_prob.append(max_weighted_tmp[tmp])
				else:					
					tmp = numpy.argmax(max_weighted_tmp)
					random_choice = random.randint(0,len(targets)-1)
					weighted_pick = random_choice
					max_weighted_prediction.append(max_weighted_probs[random_choice][l])					
					max_weighted_prob.append(max_weighted_tmp[tmp])
					
				if numpy.amax(max_tmp) != numpy.amin(max_tmp):
					tmp = numpy.argmax(max_tmp)					
					max_prediction.append(max_probs[tmp][l])
					max_prediction_prob.append(max_tmp[tmp])
				else:					
					tmp = numpy.argmax(max_tmp)	
					random_choice = random.choice([k for k in range(len(targets)) if k != weighted_pick])
					max_prediction.append(max_probs[random_choice][l])
					max_prediction_prob.append(max_tmp[tmp])
			
			mean_pred_index = numpy.argmax(mean_prediction_prob)
			median_pred_index = numpy.argmax(median_prediction_prob)						
			
			if numpy.amax(max_prediction_prob) != numpy.amin(max_prediction_prob):
				max_pred_index = numpy.argmax(max_prediction_prob)
			else:
				max_pred_index = random.randint(0,len(max_prediction_prob)-1)				 
			
			if numpy.amax(max_weighted_prob) != numpy.amin(max_weighted_prob):
				max_weighted_pred_index = numpy.argmax(max_weighted_prob)
			else:
				max_weighted_pred_index = random.randint(0,len(max_weighted_prob)-1)						
			
			current_result = [window[-1][k] for k in range(len(window[-1])-3)]
			# Mean Result
			current_result.append(mean_prediction[mean_pred_index])
			current_result.append(mean_pred_index)
			current_result.append(float(mean_prediction_prob[mean_pred_index])/float(weights_mean[mean_pred_index]))
			current_result.append(weights_mean[mean_pred_index])
			
			# Median Result
			current_result.append(median_prediction[median_pred_index])
			current_result.append(median_pred_index)
			current_result.append(float(median_prediction_prob[median_pred_index])/float(weights_median[median_pred_index]))
			current_result.append(weights_median[median_pred_index])
			
			# Max Result
			if max_prediction[max_pred_index] != None:
				current_result.append(max_prediction[max_pred_index][4])
				current_result.append(max_pred_index)
				current_result.append(float(max_prediction_prob[max_pred_index])/float(weights_max[max_pred_index]))
				current_result.append(weights_max[max_pred_index])
				current_result.append(max_prediction[max_pred_index][0])
			else:
				current_result.append(-1)
				current_result.append(max_pred_index)
				current_result.append(0)
				current_result.append(weights_max[max_pred_index])
				current_result.append("")
			
			# Max Weighted Result
			if max_weighted_prediction[max_weighted_pred_index] != None:
				current_result.append(max_weighted_prediction[max_weighted_pred_index][4])
				current_result.append(float(max_weighted_prob[max_weighted_pred_index])/float(weights_weighted_max[max_weighted_pred_index]))
				current_result.append(max_weighted_prediction[max_weighted_pred_index][0])
			else:
				current_result.append(-1)				
				current_result.append(0)
				current_result.append("")			
			
			# Ensemble
			if max_prediction[max_pred_index] != None and max_weighted_prediction[max_weighted_pred_index] != None:
				if (float(max_prediction[max_pred_index][7])/float(max_weighted_prediction[max_weighted_pred_index][7]) < 0.5 and last_prediction_correct) or last_prediction_correct:
					current_result.append(max_prediction[max_pred_index][4])
					current_result.append(max_prediction[max_pred_index][7])
					current_result.append(float(max_prediction_prob[max_pred_index])/float(weights_max[max_pred_index]))
				else:
					current_result.append(max_weighted_prediction[max_weighted_pred_index][4])
					current_result.append(max_weighted_prediction[max_weighted_pred_index][7])
					current_result.append(float(max_weighted_prob[max_weighted_pred_index])/float(weights_weighted_max[max_weighted_pred_index]))
			else:
				current_result.append(-1)
				current_result.append(0)
				current_result.append(0)
			
			prediction_result.append(current_result)
			
			if int(current_result[20]) == int(window[-1][7]):
				last_prediction_correct = True
			else:
				last_prediction_correct = False
			
			# Weight Update			
			for l in range(3):#7
				if mean_prediction[l] == int(window[-1][7]):
					weights_mean[l] = weights_mean[l] + eps
				else:
					if weights_mean[l]-eps > 1:
						weights_mean[l] = weights_mean[l] - eps
						
				if median_prediction[l] == int(window[-1][7]):
					weights_median[l] = weights_median[l] + eps
				else:
					if weights_median[l]-eps > 1:
						weights_median[l] = weights_median[l] - eps
						
				if max_prediction[l] != None and max_prediction[l][4] == int(window[-1][7]):
					weights_max[l] = weights_max[l] + eps
				else:
					if weights_max[l]-eps > 1:
						weights_max[l] = weights_max[l] - eps
						
				if max_weighted_prediction[l] != None and max_weighted_prediction[l][4] == int(window[-1][7]):
					weights_weighted_max[l] = weights_weighted_max[l] + eps
				else:
					if weights_weighted_max[l] - eps > 1:
						weights_weighted_max[l] = weights_weighted_max[l] - eps
			
			temporal_complexity[3].append(time.time() - start)
					
	
	#print [sum(k) for k in temporal_complexity],[numpy.mean(k) for k in temporal_complexity]
	
	return weights_mean,weights_median,weights_max,weights_weighted_max,prediction_result,pattern_result

def Predict_Day_SP(windows,pattern_length,dates,weeks,months,timestamps,patterns,valid_patterns,valid_patterns_mod,singular_patterns_occ,targets,cut_off,weights_mean,weights_median,weights_max,eps,singular_patterns_prob,singular_patterns_duration,singular_patterns_specificity):
	
	feature_combination = []
	
	for k in range(1,7):
		feature_combination = feature_combination + [list(comb) for comb in itertools.combinations(range(6),k)]
		
	prediction_result = []
	
	for comb in feature_combination:
		for window in windows:
		
			if window[-1][11] != '':
				
				data_point_patterns = [(k,singular_patterns_prob[k-1]) for k in map(int,window[-1][11].split(",")) if singular_patterns_duration[k-1] == 0 and singular_patterns_specificity[k-1] in comb]
				
				current_result = [window[-1][k] for k in range(len(window[-1])-3)]
				
				if len(data_point_patterns) > 0:
					result_pattern = sorted(data_point_patterns, key=itemgetter(1),reverse=True)
					
					last_specificity = -1
					
					if len(result_pattern) > 1:
						potential_result = []
						potential_result.append(result_pattern[0])
						
						for i in range(1,len(result_pattern)):
							if (result_pattern[i-1][1] - result_pattern[i][1]) <= 0.05:
								potential_result.append(result_pattern[i])
							else:
								break
						
						result_pattern = None						
						
						for pattern in potential_result:
							if singular_patterns_specificity[pattern[0]-1] > last_specificity:
								last_specificity = singular_patterns_specificity[pattern[0]-1]
								result_pattern = pattern
					else:
						result_pattern = result_pattern[0]
					
					current_result.append(singular_patterns_occ[result_pattern[0]-1])
					current_result.append(result_pattern[1])
					current_result.append(result_pattern[0])
					current_result.append(last_specificity)
					current_result.append(",".join(map(str,comb)))
				else:
					current_result.append(0)
					current_result.append(0)
					current_result.append(-1)
					current_result.append(-1)
					current_result.append(",".join(map(str,comb)))
					
				
				prediction_result.append(current_result)	
	
	return prediction_result

def Create_Pattern_Combinations(patterns,indices,target_patterns,valid_patterns,current_patterns):
	
	unique_patterns = set([i for k in indices for i in patterns[k]])	
	possible_patterns = [l for k in target_patterns for l in valid_patterns[k-1] if set(l.split(',')[1:-2]).issubset(unique_patterns)] #  and any(map(lambda x: x in current_patterns,map(int,l.split(',')[1:-2])))	
	
	return possible_patterns
	
def Create_Pattern_Combinations_old(patterns,indices,target_patterns,pattern_length,list_pl_2,previous_unique_patterns,previous_patterns,valid_patterns):
	
	if len(indices) >= pattern_length-1:				
				
		final_patterns = [patterns[k] for k in indices]		
		unique_patterns = set([i for k in final_patterns for i in k if i not in previous_unique_patterns])		
		
		new_comb = [[] for k in range(len(list_pl_2))]
		comb = [[] for k in range(len(list_pl_2))]
		comb_long = [[] for k in range(len(list_pl_2))]
		comb_tmp = [[] for k in range(len(list_pl_2))]
		
		comb_test = [[] for k in range(len(list_pl_2))]
		
		for current_pattern_length in range(len(list_pl_2)):
			if len(list_pl_2[current_pattern_length]) == 0:
				
				if current_pattern_length == 0 or (current_pattern_length > 0 and len(comb_long[current_pattern_length-1]) == 0):
					for k in itertools.permutations(unique_patterns,current_pattern_length+1):
						comb_long[current_pattern_length].append(list(k))
					
					for k in itertools.permutations(unique_patterns,current_pattern_length):
						new_comb[current_pattern_length].append(list(k))
					
				else:										
					for k in itertools.permutations(unique_patterns,current_pattern_length+1):
						comb_long[current_pattern_length].append(list(k))
						
					for k in comb_long[current_pattern_length-1]:
						new_comb[current_pattern_length].append(k)
						
				for k in comb_long[current_pattern_length]:
					for i in target_patterns:
						tmp = ",%s,%s," % (','.join(k),i)
						
						if tmp in valid_patterns:
							comb[current_pattern_length].append(tmp)
			else:												
				
				for k in list_pl_2[current_pattern_length]:
					for i in range(len(k)+1):
						for j in unique_patterns:
							l = list(k)
							l.insert(i,j)						
							
							comb_long[current_pattern_length].append(l)
							
							for m in target_patterns:
								comb_tmp[current_pattern_length].append(",%s,%s," % (','.join(l),m))
								
							if current_pattern_length == 0 or (current_pattern_length > 0 and len(comb_long[current_pattern_length-1]) == 0):
								if i < len(k):
									n = list(k)
									n[i] = j
									
									if not n in new_comb[current_pattern_length]:
										new_comb[current_pattern_length].append(n)
									
				if (current_pattern_length > 0 and len(comb_long[current_pattern_length-1]) != 0):
					for k in comb_long[current_pattern_length-1]:
						new_comb[current_pattern_length].append(k)
				
				previous_comb = [",%s,%s," % (','.join(list(k)),i) for k in previous_patterns[current_pattern_length] for i in target_patterns]			
				
				tmp = comb_tmp[current_pattern_length] + previous_comb
				
				for k in tmp:
					
					if k in valid_patterns:
						comb[current_pattern_length].append(k)
		
		return comb,[list_pl_2[k]+new_comb[k] for k in range(len(list_pl_2))],set(list(previous_unique_patterns)+list(unique_patterns)),[previous_patterns[k] + comb_long[k] for k in range(len(previous_patterns))]		
	else:
		return [[] for k in range(pattern_length-1)],[[] for k in range(pattern_length-1)],[],[[] for k in range(pattern_length-1)]

# ================================= End of Support Methods =================================

if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	mode = int(sys.argv[3])
	user = int(sys.argv[4])
	
	num_processes = 5
	proccesing_start = 52
	
	if mode == 0:
		pool = Pool(num_processes)
		
		threads = []
		
		for k in range(num_processes):
			threads.append(pool.apply_async(Prepare_Data, ((proccesing_start + num_processes*user + k),)))
		
		for thread in threads:
			thread.get()
			
	if mode == 1:
		Predict_Occupancy(user,3,6,5,6,0.01)
		
	if mode == 2:
		"""
		feature_combination = []
				
		for k in range(1,4):
			feature_combination = feature_combination + [list(comb) for comb in itertools.combinations(range(4),k) if not 2 in list(comb)]
			
		print len(feature_combination)
		print feature_combination
		"""
		
		l1 = [1,2,3]
		l2 = [2,5,6]
		
		print map(lambda x: x in l2,l1)