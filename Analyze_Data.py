#!/usr/bin/python
# -*- coding: utf-8 -*-

import Database_Handler
import math
import sys
import numpy

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "NA"
PASSWORD = "NA"

def Compare_Training_Test():
    
    dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_Study")
    
    training = [numpy.zeros(288,float) for k in range(50)]
    test = [numpy.zeros(288,float) for k in range(50)]
    
    result = dbHandler.select("SELECT user_id,time_index,avg(current_location) FROM Data_GHC WHERE user_id < 51 GROUP BY user_id,time_index ORDER BY user_id,time_index")
    
    for row in result:
        training[int(row[0])-1][int(row[1])] = float(row[2])
        
    result = dbHandler.select("SELECT user_id,time_index,avg(current_location) FROM Pattern_Prediction_Study.GHC_Test_Result_SP WHERE user_id < 51 GROUP BY user_id,time_index ORDER BY user_id,time_index")
    
    for row in result:
        test[int(row[0])-1][int(row[1])] = float(row[2])
        
    difference = []
    
    for user in range(50):
        difference.append([math.fabs(training[user][k]-test[user][k]) for k in range(288)])
        
    output = ""
    
    for user in range(50):
        output = "%s\n%i,%s" % (output,user,",".join(map(str,difference[user])))
        
    print output

def Results_By_Room():
    
    dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Prediction_Study")
    
    result_data_correct = [numpy.zeros(288,int) for k in range(0,90)]
    result_data_total = [numpy.zeros(288,int) for k in range(0,90)]
    
    result = dbHandler.select("SELECT user_id,time_index,count(id) FROM GHC_Test_Result_SP WHERE current_location = prediction AND (date < '2011-11-22' OR date > '2011-11-29') AND feature_combination = '1,3' GROUP BY user_id,time_index ORDER BY user_id,time_index")
    
    for row in result:        
        result_data_correct[int(row[0])-1][int(row[1])] = int(row[2])
        
    result = dbHandler.select("SELECT user_id,time_index,count(id) FROM GHC_Test_Result_SP WHERE (date < '2011-11-22' OR date > '2011-11-29') AND feature_combination = '1,3' GROUP BY user_id,time_index ORDER BY user_id,time_index")
    
    for row in result:        
        result_data_total[int(row[0])-1][int(row[1])] = int(row[2])
        
    for i in range(len(result_data_total)):
        for j in range(len(result_data_total[i])):
            
            if result_data_total[i][j] > 0:
                values = []
                values.append(i+1)
                values.append(j)
                values.append(float(result_data_correct[i][j])/float(result_data_total[i][j]))
                values.append("Singular_Pattern_Prediction")
                
                dbHandler.insert("Result_Analysis", ['user_id','time_index','accuracy','prediction_type'], values)
            

if __name__ == "__main__":
        
    USERNAME = sys.argv[1]
    PASSWORD = sys.argv[2]
    
    Results_By_Room()