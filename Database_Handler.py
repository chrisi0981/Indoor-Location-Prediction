#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import numpy
import warnings

class Database_Handler:
     
    def __init__(self,host,port,user,user_password,name):
        self._database_host = host
        self._database_port = port
        self._database_user = user
        self._database_user_password = user_password
        self._database_name = name
        
    def printDatabaseInformation(self):
        print "Host: %s" % (self._database_host)
        print "Port: %i" % (self._database_port)
        print "User: %s" % (self._database_user)
        print "User Password: %s" % (self._database_user_password)
        print "Name: %s" % (self._database_name)

    def getNumber(self, number):

        
        if number == 'inf':
            return 50000
        
        if number == '-inf':
            return -50000                
        
        return number

    def getString(self, string):
        
        if string != None:        
            newString = string.replace("'","").replace("`","")
            
            if string == 'inf':
                newString = 50000
                
            if string == '-inf':
                newString = -50000
            
            try:
                string_tmp = newString.encode('latin-1','ignore')
                
                return string_tmp
            except UnicodeDecodeError:
                print string
        else:
            return ""
    

    # This method handles database inserts and takes three parameters:
    #
    # table_name: the name of the table in which values should be inserted
    # fields: a list of field names
    # values: a list of values for each field
    #
    # it is to not that the type of each field is determined by the method
    
    def insert(self, table_name, fields, values):
        
        query = "INSERT INTO %s (" % (table_name)
        
        for i in range(len(fields)-1):
            query = "%s%s," % (query,fields[i])
        
        query = "%s%s) VALUES (" % (query,fields[len(fields)-1])
        
        for i in range(len(values)-1):
            
            if isinstance(values[i],int) or isinstance(values[i],long):
                query = "%s%i," % (query,self.getNumber(values[i]))
            else:
                if isinstance(values[i],str):
                    query = "%s'%s'," % (query,self.getString(values[i]))
                else:
                    if isinstance(values[i],float):
                        query = "%s%f," % (query,self.getNumber(values[i]))
                    else:
                        query = "%s'%s'," % (query,self.getString(values[i]))
        
        i = len(values)-1
        
        if isinstance(values[i],int) or isinstance(values[i],long):
            query = "%s%i)" % (query,self.getNumber(values[i]))
        else:
            if isinstance(values[i],str):
                query = "%s'%s')" % (query,self.getString(values[i]))
            else:
                if isinstance(values[i],float):
                    query = "%s%f)" % (query,self.getNumber(values[i]))
                else:
                    query = "%s'%s')" % (query,self.getString(values[i]))
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        con.autocommit(True)
        cursor = con.cursor()
        id = 0
        try:
            cursor.execute(query)
            id = con.insert_id()         
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query
        
        cursor.close()
        con.close()
        
        return id
    
    
    def insert_bulk(self, table_name, fields, values):
        
        query = "INSERT INTO %s (" % (table_name)
        
        for i in range(len(fields)-1):
            query = "%s%s," % (query,fields[i])
        
        query = "%s%s) VALUES " % (query,fields[len(fields)-1])
        
        for k in range(len(values)):
            if k == 0:
                query = "%s(" % (query)
            else:
                query = "%s,(" % (query)
            
            for i in range(len(values[k])-1):
                
                if isinstance(values[k][i],int) or isinstance(values[k][i],long):
                    query = "%s%i," % (query,self.getNumber(values[k][i]))
                else:
                    if isinstance(values[k][i],str):
                        query = "%s'%s'," % (query,self.getString(values[k][i]))
                    else:
                        if isinstance(values[k][i],float):
                            query = "%s%f," % (query,self.getNumber(values[k][i]))
                        else:
                            query = "%s'%s'," % (query,self.getString(values[k][i]))
            
            i = len(values[k])-1
            
            if isinstance(values[k][i],int) or isinstance(values[k][i],long):
                query = "%s%i)" % (query,self.getNumber(values[k][i]))
            else:
                if isinstance(values[k][i],str):
                    query = "%s'%s')" % (query,self.getString(values[k][i]))
                else:
                    if isinstance(values[k][i],float):
                        query = "%s%f)" % (query,self.getNumber(values[k][i]))
                    else:
                        query = "%s'%s')" % (query,self.getString(values[k][i]))
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        con.autocommit(True)
        cursor = con.cursor()
        id = 0
        try:
            warnings.filterwarnings("ignore", "Data truncated for column *")
            cursor.execute(query)
            id = con.insert_id()         
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            #print query
        
        cursor.close()
        con.close()
        
        return id
        
        
    def select(self, query):
        
        #print query
        
        result = None
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query
        else:
            result = cursor.fetchall()
        
        cursor.close()
        con.close()
        
        return result
    
    def update(self, query):
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        con.autocommit(True)
        
        try:
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query
        
        cursor.close()
        con.close()
        
    
    def dropTable(self, table_name):
        
        query = "DROP TABLE IF EXISTS %s" % (table_name)
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:
            warnings.filterwarnings("ignore", "Unknown table.*")
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query        
        
        cursor.close()
        con.close()
        
    def createTable(self, table_name, selectString):
        
        if selectString == "":
            query = "CREATE TABLE IF NOT EXISTS %s" % (table_name)
        else:
            query = "CREATE TABLE IF NOT EXISTS %s (%s)" % (table_name,selectString)                
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:
            warnings.filterwarnings("ignore", "Table already exists.*")
            warnings.filterwarnings("ignore", "Unknown table.*")
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query        
        
        cursor.close()
        con.close()
        
    def deleteData(self, query):
    
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:
            warnings.filterwarnings("ignore", "Unknown table.*")
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query        
        
        cursor.close()
        con.close()
    
    
    def truncateTable(self, table_name):
        
        query = "TRUNCATE TABLE %s" % (table_name)
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:
            warnings.filterwarnings("ignore", "Unknown table.*")
            cursor.execute(query)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print query        
        
        cursor.close()
        con.close()
    
    # Special Methods
    def getGreatestIndex(self, table_name,device_id):
        
        table_id = 0
        
        selectString = "SELECT id FROM %s WHERE device_id = '%s' ORDER BY id DESC LIMIT 1" % (table_name,device_id)
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:            
            cursor.execute(selectString)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print selectString
        else:
            result = cursor.fetchall()
            
            for row in result:
                table_id = int(row[0])
            
        cursor.close()
        con.close()
        
        return table_id
    
    def getGreatestTimestamp(self, table_name,device_id):
        
        timestamp = 0
        
        selectString = "SELECT timestamp FROM %s WHERE device_id = '%s' ORDER BY id DESC LIMIT 1" % (table_name,device_id)
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:            
            cursor.execute(selectString)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print selectString
        else:
            result = cursor.fetchall()
            
            for row in result:
                timestamp = int(row[0])
            
        cursor.close()
        con.close()
        
        return timestamp
    
    """
        Fields, Fields_types, and default_values need to be of equal size
    """
    def createNewTable(self, table_name,fields,field_types,default_values):
        
        create_string = "CREATE TABLE IF NOT EXISTS `%s` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT," % (table_name)
        
        for i in range(len(fields)):
            
            create_string = "%s`%s` %s %s," % (create_string,fields[i],field_types[i],default_values[i])
            
        create_string = "%sPRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;" % (create_string)
        
        con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
        cursor = con.cursor()
        
        try:            
            cursor.execute(create_string)            
        except MySQLdb.Error, e:        
            print "Error %d: %s" % (e.args[0], e.args[1])
            print create_string
            
        cursor.close()
        con.close()
        
    def alterTable(self, table_name,fields,field_types,default_values):
        
        for i in range(len(fields)):
            alter_string = "ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s" % (table_name,fields[i],field_types[i],default_values[i])
            
            con = MySQLdb.connect(host=self._database_host, port=self._database_port, user=self._database_user, passwd=self._database_user_password, db=self._database_name)
            cursor = con.cursor()
            
            try:            
                cursor.execute(alter_string)            
            except MySQLdb.Error, e:        
                print "Error %d: %s" % (e.args[0], e.args[1])
                print alter_string
                
            cursor.close()
            con.close()
    
    
    
    
    
    
    
    
