#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import Database_Handler
import math
import array
import sys

import numpy

from operator import itemgetter

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Analyze_Patterns(user):
	
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction")
	
	dbHandler.update("SET global max_allowed_packet=104857600")
	
	"""
	
	Load extracted patterns!
	
	"""
	dbHandler.truncateTable("Singular_Pattern_Base")
	dbHandler.update("INSERT INTO Singular_Pattern_Base SELECT * FROM Pattern_Base.Singular_Pattern_Base WHERE LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 2 AND user_id = %i" % (user))
		
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
	
	result = dbHandler.select("SELECT pattern_id,probability,location,time,LENGTH(members) - LENGTH(REPLACE(members, ',', '')),specificity,activity FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
	
	for row in result:
		singular_patterns_prob[int(row[0])-1] = float(row[1])
		singular_patterns_occ[int(row[0])-1] = int(row[2])
		singular_patterns_time[int(row[0])-1] = int(row[3])
		singular_patterns_frequency[int(row[0])-1] = int(row[4])-1
		singular_patterns_specificity[int(row[0])-1] = int(row[5])		
		
		if row[6] == "Duration":
			singular_patterns_duration[int(row[0])-1] = 1
		else:
			singular_patterns_duration[int(row[0])-1] = 0
	
	# Specificity weight: 0.14285714
	occupied = numpy.nonzero(singular_patterns_occ)[0]
	not_occupied = numpy.where(numpy.array(singular_patterns_occ) == 0)[0]
			
	max_pattern_length = 1
	
	result = dbHandler.select("SELECT max(pattern_length) FROM Pattern_Base.Pattern_Base_%i" % (user))
	
	for row in result:
		max_pattern_length = int(row[0])
	
	result = dbHandler.select("SELECT pattern_members,probability,ROUND(specificity/0.14285714)-1,TRIM(TRAILING ',' FROM SUBSTRING_INDEX(pattern_members, ',',-2)),pattern_length,pattern_condition,count FROM Pattern_Base.Pattern_Base_%i" % (user))
	
	valid_patterns = []
	valid_patterns.append({})
	
	for i in range(len(singular_patterns_prob)):
		tmp = [0 for k in range(38)]
		
		for k in range(7):
			tmp[k] = singular_patterns_prob[i]
			tmp[10+k] = singular_patterns_frequency[i]
			
			if singular_patterns_prob[i] > 0:
				tmp[24 + k] = -1*singular_patterns_prob[i]*numpy.log2(singular_patterns_prob[i])
			
		tmp[7] = ""
		tmp[8] = i+1
		tmp[9] = 1
		valid_patterns[0][",%i," % (i+1)] = tmp
	
	for row in result:
		
		if int(row[4]) > len(valid_patterns):
			valid_patterns.append({})
		
		if row[0] in valid_patterns[int(row[4])-1]:
			current_values = valid_patterns[int(row[4])-1][row[0]]
			current_values[int(row[2])] = float(row[1])
			current_values[10+int(row[2])] = int(row[6])
			valid_patterns[int(row[4])-1][row[0]] = current_values
		else:
			tmp = [0 for k in range(38)]
			tmp[int(row[2])] = float(row[1])
			tmp[7] = row[5]
			tmp[8] = int(row[3])			
			condition = [singular_patterns_specificity[k-1] for k in map(int,row[0][1:-1].split(","))]
			if numpy.amax(condition) == numpy.amin(condition):
				tmp[9] = 1
			valid_patterns[int(row[4])-1][row[0]] = tmp
	
	num_negative = 0
	negative_pattern_list = []
	negative_length_list = []
	duration_events = 0
	temporal_cond = 0
	
	for i in range(1,len(valid_patterns)):
		for pattern in valid_patterns[i]:	
			for tc in range(7):
				
				if valid_patterns[i][pattern][tc] > 0:
					valid_patterns[i][pattern][10 + tc] = valid_patterns[i][pattern][tc]*valid_patterns[i-1][valid_patterns[i][pattern][7]][tc]
					
					# Conditional Entropy
					valid_patterns[i][pattern][17 + tc] = -1*valid_patterns[i][pattern][10 + tc]*valid_patterns[i][pattern][tc]*numpy.log2(valid_patterns[i][pattern][tc])
					
					# Entropy of Pattern P
					valid_patterns[i][pattern][24 + tc] = valid_patterns[i][pattern][17 + tc] + valid_patterns[i-1][valid_patterns[i][pattern][7]][24 + tc]
					
					# Information Gain
					valid_patterns[i][pattern][31 + tc] = -1*singular_patterns_prob[valid_patterns[i][pattern][8]-1]*numpy.log2(singular_patterns_prob[valid_patterns[i][pattern][8]-1]) - valid_patterns[i][pattern][17 + tc]
					
					if valid_patterns[i][pattern][31 + tc] < 0:
						valid_patterns[i][pattern][31 + tc] = 0
						num_negative = num_negative + 1
						negative_pattern_list.append(pattern)
						negative_length_list.append(i)
						
						if singular_patterns_duration[valid_patterns[i][pattern][8]-1] == 1:
							duration_events = duration_events + 1
							
						if valid_patterns[i][pattern][9] == 0:
							temporal_cond = temporal_cond + 1
		
	print valid_patterns[3][',68,3141,3482,2264,']
	print valid_patterns[2][',68,3141,3482,']
	print valid_patterns[1][',68,3141,']
	print valid_patterns[0][',68,']
	print valid_patterns[0][',2264,']
	print num_negative,duration_events,temporal_cond
	
	print numpy.mean(negative_length_list)
	print valid_patterns[negative_length_list[0]][negative_pattern_list[0]]
	
if __name__ == "__main__":
		
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	USER = int(sys.argv[3])
	
	Analyze_Patterns(USER)