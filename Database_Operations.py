#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import sys
import Database_Handler

DATABASE_HOST = "localhost"
PORT = 3306
USERNAME = "root"
PASSWORD = "htvgt794jj"

def Split_Table(user):
		
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_final")	
	dbHandler.createTable("Pattern_Base_%i" % (user),"SELECT * FROM Pattern_Base.Pattern_Base WHERE user_id = %i" % (user))
	
if __name__ == "__main__":
	
	USERNAME = sys.argv[1]
	PASSWORD = sys.argv[2]
	
	Split_Table(3)
	
	"""
	dbHandler = Database_Handler.Database_Handler(DATABASE_HOST,PORT, USERNAME, PASSWORD, "Pattern_Base_final")
	
	dbHandler.select("SELECT user_id FROM Data_GHC_tmp")
	
	max_user_id = 0
	
	for row in result:
		max_user_id = int(row[0])
		
	for user in range(3,max_user_id):
		Split_Table(user)
	"""