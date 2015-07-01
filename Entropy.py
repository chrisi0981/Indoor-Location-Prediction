#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import math
import array
import sys
from multiprocessing import Process
from multiprocessing import Pool
import os
import thread
import threading
import Queue
import numpy
import random
from scipy.optimize import fsolve
import Database_Handler

from operator import itemgetter
import pandas as panda
import itertools

import time

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "root"
PASSWORD = "htvgt794jj"

# Entropy calculation using the Lempel-Ziv estimation
def containsQuery(history,query):
    n = len(query)    
    return any( (query == history[i:i+n]) for i in xrange(len(history)-n+1) )

def checkQuery(history,current):
    for i in range(0,len(current)+1):
        if not containsQuery(history,current[0:i]):
            return i
        
    return 0

def randomizeData(originalData,fraction):    
    
    data = []
    data_1 = []
    
    for index in range(0,len(originalData)):
        data.append(int(originalData[index]))
        data_1.append(int(originalData[index]))
    
    unknowns = int(len(data)*fraction) # number of unknown locations
    
    upi = int(unknowns/5) # unkownsPerInterval
    ipi = int(len(data)/5) # indiciesPerInterval
    
    indicies_1 = random.sample(range(0,ipi),upi)
    indicies_2 = random.sample(range(ipi,2*ipi),upi)
    indicies_3 = random.sample(range(2*ipi,3*ipi),upi)
    indicies_4 = random.sample(range(3*ipi,4*ipi),upi)
    indicies_5 = random.sample(range(4*ipi,len(data)),upi)
    
    for i in range(0,upi):
        data[indicies_1[i]] = 17
        data[indicies_2[i]] = 17
        data[indicies_3[i]] = 17
        data[indicies_4[i]] = 17
        data[indicies_5[i]] = 17
        
        if indicies_1[i] > 0:
            data_1[indicies_1[i]] = data_1[indicies_1[i-1]]
        else:
            data_1[indicies_1[i]] = 17
            
        if indicies_2[i] > 0:
            data_1[indicies_2[i]] = data_1[indicies_2[i-1]]
        else:
            data_1[indicies_2[i]] = 17
            
        if indicies_3[i] > 0:
            data_1[indicies_3[i]] = data_1[indicies_3[i-1]]
        else:
            data_1[indicies_3[i]] = 17
            
        if indicies_4[i] > 0:
            data_1[indicies_4[i]] = data_1[indicies_4[i-1]]
        else:
            data_1[indicies_4[i]] = 17
            
        if indicies_5[i] > 0:
            data_1[indicies_5[i]] = data_1[indicies_5[i-1]]
        else:
            data_1[indicies_5[i]] = 17
    
    return data,data_1

def calculateEntropy(data):    
    deltas = [ checkQuery(data[0:i],data[i:len(data)]) for i in range(0,len(data)) ]    
    entropy = (numpy.log2(len(data)) * len(data))/sum(deltas)
    
    return entropy

def calculateUncorrelatedEntropy(data):
    
    dataLength=len(data)
    uniqueLocation = list(set(data))
    
    entropy = 0
    
    for i in range(0,len(uniqueLocation)):
        num = data.count(uniqueLocation[i])
        prob = float(num)/float(dataLength)
        
        if prob > 0:
            entropy = entropy + (prob)*numpy.log2(prob)
        
    return entropy*(-1)

def calculateStep(data,fraction):
    randData_1,randData_2 = randomizeData(data,fraction)
    
    entropy_1a = calculateEntropy(randData_1)
    entropy_1b = calculateUncorrelatedEntropy(randData_1)
    entropy_2a = calculateEntropy(randData_2)
    entropy_2b = calculateUncorrelatedEntropy(randData_2)
    
    returnData = []
    
    returnData.append(randData_1)
    returnData.append(randData_2)
    returnData.append(entropy_1a)
    returnData.append(entropy_1b)
    returnData.append(entropy_2a)
    returnData.append(entropy_2b)
    
    return returnData

def getOutput(result):
    output = "Data1 \n["
    
    data = result[0]
    
    output = "%s%i" % (output,int(data[0]))
    
    for index in range(1,len(data)):
        output = "%s,%i" % (output,int(data[index]))
    
    data = result[1]
    
    output = "%s]\nData2:\n[%i" % (output,int(data[0]))
    
    for index in range(1,len(data)):
        output = "%s,%i" % (output,int(data[index]))
        
    output = "%s]\n" % (output)
    
    output = "%sH(data1)=%f H_unc(data1)=%f H(data2)=%f H_unc(data2)=%f" % (output,float(result[2]),float(result[3]),float(result[4]),float(result[5]))
    
    return output

def checkData(data):
    
    uniqueLocation = list(set(data))
    
    output = "%i:%i" % (uniqueLocation[0],data.count(uniqueLocation[0]))
    
    for i in range(1,len(uniqueLocation)):
        num = data.count(uniqueLocation[i])
        output = "%s,%i:%i" % (output,uniqueLocation[i],num)
        
    return output

def getEntropyResult(result):
    output = "H(data1)=%f H_unc(data1)=%f H(data2)=%f H_unc(data2)=%f" % (float(result[2]),float(result[3]),float(result[4]),float(result[5]))
    
    return output
 
def Entropy_for_User(user):
    
    dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base")
    
    result = dbHandler.select("SELECT max(event_id) FROM Events_GHC_final WHERE user_id = %i" % (user))
    
    max_event_id = 0
    
    for row in result:
        max_event_id = int(row[0])
    
    events = []
    raw_data = []
    raw_data_1 = []
    
    for k in range(max_event_id):
        events.append([])
        raw_data.append(k+1)
        raw_data_1.append(1)
    
    result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
    
    max_sp_id = 0
    
    for row in result:
        max_sp_id = int(row[0])
    
    result = dbHandler.select("SELECT members,probability,pattern_id FROM Singular_Pattern_Base WHERE user_id = %i AND LENGTH(members) - LENGTH(REPLACE(members, ',', '')) > 3  ORDER BY pattern_id" % (user))
    
    singular_patterns = numpy.zeros(max_sp_id,float)
    singular_patterns_events = []
    
    for k in range(max_sp_id):
        singular_patterns_events.append([])
    
    for row in result:
        singular_patterns[int(row[2])-1] = float(row[1])
        
        members = row[0][1:-1].split(",")
        singular_patterns_events[int(row[2])-1] = map(int,members)
        
        for member in members:
            events[int(member)-1].append(int(row[2])-1)
            
        if len(events[int(member)-1]) == 0:
            print int(row[2])-1
    
    # Length 1 Patterns
    new_data = []
            
    for data_point in raw_data:                
        
        event = events[data_point-1]
        
        max_probability = 0
        best_pattern = -1
        
        for k in range(len(event)):
            if singular_patterns[event[k]] > max_probability:
                max_probability = singular_patterns[event[k]]
                best_pattern = event[k] + 1        
                
        if best_pattern == -1:
            new_data.append(data_point)
        else:        
            new_data.append(best_pattern)
                    
    
    """
    fields = ['user_id','pattern_length','entropy']
   
    values = []
    values.append(user)
    values.append(0)
    values.append(calculateEntropy(raw_data))
    
    dbHandler.insert("Entropy",fields,values)
    
    values = []
    values.append(user)
    values.append(1)
    values.append(calculateEntropy(new_data))
    
    dbHandler.insert("Entropy",fields,values)
    """
        
    for pattern_length in range(2,3):
    
        remaining_data = [v for j,v in enumerate(raw_data)]
        entropy_data = [v for j,v in enumerate(raw_data)]
    
        pattern_id = max(remaining_data) + 1
    
        pattern_base = {}
        pattern_base_cond = {}
        pattern_index = []
        pattern_index_cond = []
        pattern_events = {}
        
        result = dbHandler.select("SELECT pattern_condition,pattern_members,count*specificity FROM Pattern_Base_%i WHERE pattern_length = %i AND count > 3" % (user,pattern_length))
                
        for row in result:            
            try:
                current_values = pattern_base[row[1][1:-1]]
                tmp = []
                tmp.append(row[1])
                tmp.append(float(row[2]))
                tmp.append([singular_patterns_events[v-1] for j,v in enumerate(map(int,row[1][1:-1].split(',')))])
                tmp.append(current_values[0][3])
                current_values.append(tmp)
                pattern_base[row[1][1:-1]] = current_values                
            except KeyError, e:
                tmp = []
                tmp.append(row[1])
                tmp.append(float(row[2]))
                tmp.append([singular_patterns_events[v-1] for j,v in enumerate(map(int,row[1][1:-1].split(',')))])
                tmp.append(pattern_id)
                pattern_id = pattern_id + 1
                tmp1 = []
                tmp1.append(tmp)
                pattern_base[row[1][1:-1]] = tmp1
                pattern_index.append(row[1][1:-1])
                pattern_events[row[1][1:-1]] = [singular_patterns_events[v-1] for j,v in enumerate(map(int,row[1][1:-1].split(',')))]
                
            try:
                current_values = pattern_base_cond[row[0][1:-1]]
                tmp = []
                tmp.append(row[1])
                tmp.append(float(row[2]))
                #tmp.append([singular_patterns_events[v-1] for j,v in enumerate(map(int,row[1][1:-1].split(',')))])                
                current_values.append(tmp)
                pattern_base_cond[row[0][1:-1]] = current_values
            except KeyError, e:
                tmp = []
                tmp.append(row[1])
                tmp.append(float(row[2]))
                #tmp.append([singular_patterns_events[v-1] for j,v in enumerate(map(int,row[1][1:-1].split(',')))])                                
                tmp1 = []
                tmp1.append(tmp)
                pattern_base_cond[row[0][1:-1]] = tmp1
                pattern_index_cond.append(row[0][1:-1])
        
        for k in range(len(pattern_index)):    
            pattern_base[pattern_index[k]].sort(key=itemgetter(1),reverse=True)                                                
        
        for k in range(len(pattern_index_cond)):    
            pattern_base_cond[pattern_index_cond[k]].sort(key=itemgetter(1),reverse=True)
        
        current_window = []                
        
        for i in range (len(remaining_data)):
            #print len(remaining_data),len(entropy_data)
            
            if len(current_window) < pattern_length -1:
                current_window.append(remaining_data[i])
            else:
                
                pattern_found = False
                found_pattern = None
                found_list = None
                
                for k in xrange(i+1,len(remaining_data)):
                    if pattern_length == 2:
                        product = itertools.product(events[current_window[0]-1],events[remaining_data[k]-1])
                        
                    max_comb_count = 0
                    best_comb = None
                    best_list = []
                    best_comb_id = -1
                    
                    for comb in product:
                        if pattern_base.has_key(','.join(map(str,comb))):                            
                            comb_events = singular_patterns_events[comb[0]-1]+singular_patterns_events[comb[1]-1]
                            
                            events_1 = singular_patterns_events[comb[0]-1]
                            events_2 = singular_patterns_events[comb[1]-1]
                            
                            tmp = [j for j,v in enumerate(remaining_data[i+1:len(remaining_data)+1]) if v in comb_events]
                            
                            final_list = []
                            used_indices = []
                            
                            for j in range(len(tmp)):
                                if remaining_data[tmp[j]] in events_1:
                                    for k in xrange(j+1,len(tmp)):
                                        if remaining_data[tmp[k]] in events_2 and not tmp[k] in used_indices:
                                            p = []
                                            p.append(tmp[j])
                                            p.append(tmp[k])
                                            final_list.append(p)
                                            used_indices.append(tmp[k])
                                            break
                            
                            if len(final_list) > max_comb_count:
                                max_comb_count = len(tmp)
                                best_comb = comb
                                best_comb_id = pattern_base[','.join(map(str,comb))][0][3]
                                best_list = final_list
                            
                            #entropy_data.append(pattern_base[','.join(map(str,comb))][0][3])
                            
                        
                    if max_comb_count > 0:
                        found_pattern = best_comb
                        pattern_found = True
                        found_list = best_list
                        break
                    
                if pattern_found:
                    #print found_pattern
                    #print found_list
                    
                    entropy_data[i-1] = best_comb_id
                    entropy_data[i] = best_comb_id
                    
                    for li in found_list:                        
                        for j in range(0,len(li)):
                            entropy_data[li[j]] = best_comb_id
                            
                else:
                    event = events[current_window[0]-1]
        
                    max_probability = 0
                    best_pattern = -1
                    
                    for k in range(len(event)):
                        if singular_patterns[event[k]] > max_probability:
                            max_probability = singular_patterns[event[k]]
                            best_pattern = event[k] + 1        
                            
                    if best_pattern != -1:
                        entropy_data[i-1] = (best_pattern)
                        
                        
                current_window.pop(0)
                current_window.append(remaining_data[i])
        
        #entropy_data = [v for j,v in enumerate(entropy_data) if v != -1]
        
        print entropy_data
        
        print "Start Calculating Entropy for Pattern %i" % (pattern_length)
        
        fields = ['user_id','pattern_length','entropy']
        
        values = []
        values.append(user)
        values.append(pattern_length)
        values.append(calculateEntropy(entropy_data))
        
        dbHandler.insert("Entropy",fields,values)
        
        
def Entropy_Analysis():
    
    dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Pattern_Base")
    
    user_ids = []
    
    result = dbHandler.select("SELECT DISTINCT user_id FROM Entropy")
    
    for row in result:
        user_ids.append(int(row[0]))
        
    fields = ['user_id','entropy_change']
    
    for user in user_ids:
        
        entropy = []
        entropy.append(0)
        entropy.append(0)
        
        result = dbHandler.select("SELECT * FROM Entropy WHERE user_id = %i" % (user))
        
        for row in result:
            entropy[int(row[2])] = float(row[3])
                    
        dbHandler.insert("Entropy_Analysis",fields,[user,(1-float(entropy[1]/entropy[0]))])
    
def Pattern_Analysis(user):
    
    dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Pattern_Base")
    
    result = dbHandler.select("SELECT max(pattern_id) FROM Singular_Pattern_Base WHERE user_id = %i" % (user))
    
    max_id = 0
    
    for row in result:
        max_id = int(row[0])
    
    result = dbHandler.select("SELECT pattern_id,probability FROM Singular_Pattern_Base WHERE user_id = %i ORDER BY pattern_id" % (user))        
    
    singular_patterns = numpy.zeros(max_id,float)
    
    for row in result:
        singular_patterns[int(row[0])-1] = float(row[1])
        
    probabilities = []
    max_pattern_length = 8
    
    for pattern_length in range(2,max_pattern_length+1):
        
        result = dbHandler.select("SELECT * FROM Pattern_Base_%i WHERE pattern_length = %i" % (user,pattern_length))
        
        previous_pattern = ""
        current_probabilities = []
        current_specificity = []
        
        pattern_probabilities = []
        
        for k in range(max_id):
            pattern_probabilities.append([])
        
        for row in result:
            
            if previous_pattern == "":
                previous_pattern = row[3]
                current_probabilities.append(float(row[7]))
                current_specificity.append(float(row[7])*float(row[9]))
            else:
                if previous_pattern == row[3]:
                    current_probabilities.append(float(row[7]))
                    current_specificity.append(float(row[7])*float(row[9]))
                else:
                    index = numpy.argmax(current_specificity)
                    members = previous_pattern[1:-1].split(",")
                    pattern_probabilities[int(members[-1])-1].append(current_probabilities[index])
                    
                    previous_pattern = row[3]
                    current_probabilities = []
                    current_specificity = []
                    current_probabilities.append(float(row[7]))
                    current_specificity.append(float(row[7])*float(row[9]))
                    
        index = numpy.argmax(current_specificity)
        members = previous_pattern[1:-1].split(",")
        pattern_probabilities[int(members[-1])-1].append(current_probabilities[index])
        
        tmp_probabilities = numpy.zeros(max_id,float)
        
        for k in range(len(singular_patterns)):
            if len(pattern_probabilities[k]) > 0:
                tmp_probabilities[k] = (numpy.amax(pattern_probabilities[k]))
    
        probabilities.append(tmp_probabilities)
            
    
    entropy = []
    
    for k in range(max_pattern_length-1):
        entropy.append([])
    
    non_zero = []    
    
    for k in range(max_pattern_length-1):        
        tmp = filter(lambda x : x == 0, probabilities[k])
        non_zero.append(len(tmp))        
            
    
    for k in range(1,max_pattern_length-1):
        print float(non_zero[k-1])/float(non_zero[k])
    
    for k in range(len(singular_patterns)):
        if probabilities[0][k] > 0:
            for l in range(max_pattern_length-1):
                if probabilities[l][k] > 0:
                    entropy[l].append(math.log(probabilities[l][k],2)*probabilities[l][k])
                else:
                    entropy[l].append(math.log(probabilities[l][k]+0.001,2)*probabilities[l][k])
                    
            #print (k+1),entropy[0],probabilities[0][k],entropy[1],probabilities[1][k],entropy[2],probabilities[2][k]
            
    mean_probs = []
    
    for k in range(max_pattern_length-1):
        mean_probs.append(numpy.mean(probabilities[k]))
                        
    for k in range(1,max_pattern_length-1):
        print mean_probs[k]/mean_probs[0]
        
    for k in range(1,max_pattern_length-1):
        ig = 0
        
        for l in range(len(entropy[k])):
            ig = ig + entropy[k-1][l] - entropy[k][l]
            
        print k,ig

# Main Program
if __name__ == "__main__":
    
    sys.setrecursionlimit(15000)
    #Entropy_for_User(22)
        
    dbHandler = Database_Handler.Database_Handler("localhost",3306, "root", "htvgt794jj", "Pattern_Base")
    
    """
    user_ids = []
    
    result = dbHandler.select("SELECT DISTINCT user_id FROM Singular_Pattern_Base")
    
    for row in result:
        user_ids.append(int(row[0]))
        
    for user in user_ids:
        Entropy_for_User(user)
    """
    Entropy_for_User(2)