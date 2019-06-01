#!/usr/bin/python
'''
    Project : "ArgusMedia"
    Module Name: PTC.py
    Created Date: 2016-07-25
    Scope: Initiate the respective scraper

    Version:V1: 2016-7-26,Sathishkandaraja
    Details:
'''

# Importing required python libraries
import pymysql.cursors
import pymysql
import os, sys
# import configparser
import time
import datetime
import subprocess
from tendo import singleton
from time import strftime
import imp
from random import randint
from time import sleep

# sleep(randint(10,100))
Fundalytics_Utility = imp.load_source('Fundalytics_Utility', '/home/merit/argusmedia/multithreading/Fundalytics_Utility.py')
# Main Program starts.    
# Program wont start If any instance runs parallelly.
me = singleton.SingleInstance()

# Getting total count of running process.
Running_count = subprocess.Popen(['ps -ef | grep python | wc -l'], stdout=subprocess.PIPE,shell=True)
Running_status = Running_count.communicate()[0]

# Checking whether total count is less then 80.
if int(Running_status) > 80:
    sys.exit()

# Getting host name(Running server IP).
Hostname = subprocess.Popen(['hostname -I'], stdout=subprocess.PIPE,shell=True)
Host_ip = Hostname.communicate()[0]            

print datetime.datetime.now()

# DB connection function.

connection=Fundalytics_Utility.DB_connection()
mysql_connection=connection.cursor()



error_log=''

# Update taskStatus from idle to Allotted and hostname as running server ip based on NextRetryStartDateTime and IsActive.
# update ExecutionStartDateTime is now.
# update ExecutionEndDateTime,ErrorReason,ProcessID as null.
try:
    Update_Query="UPDATE ScraperSchedule SET  Hostname='"+str(Host_ip).strip()+"', TaskStatus='Allotted', ExecutionStartDateTime = NOW(), ExecutionEndDateTime=NULL , ErrorReason=NULL, ProcessID=NULL WHERE DataSourceID IN (SELECT DataSourceID FROM (SELECT DataSourceID  FROM ScraperSchedule WHERE TaskStatus='idle' AND NextRetryStartDateTime <= NOW() AND IsActive = 'n' LIMIT 2 )AS dt )"
    #print Update_Query
    mysql_connection.execute(Update_Query)
    connection.commit()
except Exception as e:
    error_log=e;
    Fundalytics_Utility.log('','PTC-Module',error_log,'Error','')

# Selecting the market name and ScraperFileName based on the hostname and DataSourceID .
try:
    select_Query="SELECT ScraperFileName,DataSourceID,DataSourceName,MarketName,ScraperParameters FROM ScraperSchedule WHERE DataSourceID IN (SELECT DataSourceID FROM ScraperSchedule WHERE  Hostname='"+str(Host_ip).strip()+"' AND TaskStatus='Allotted')"    
    mysql_connection.execute(select_Query)
    result = mysql_connection.fetchall()
except Exception as e:
    error_log=e;
    Fundalytics_Utility.log('','PTC-Module',error_log,'Error','')
print "RESUUUUU",result

# Start the scrapper as per select_Query result and split the value.
while result:
	for row in result:
		ScraperFileName = row[0]
		DataSourceID = row[1]
		DataSourceName = row[2]
		MarketName = row[3]
		ScraperParameters= row[4]
		print DataSourceID
		print result

		# Running_count = subprocess.Popen(['ps -ef | grep python | wc -l'], stdout=subprocess.PIPE,shell=True)
		Running_count = subprocess.Popen(['ps -ef | grep py |grep merit|wc -l'], stdout=subprocess.PIPE,shell=True)
		Running_status = Running_count.communicate()[0]
		
		if int(Running_status) > 3:
			time.sleep(5)
			print "sleeping for 5 mins.....\n"
			# Selecting the market name and ScraperFileName based on the hostname and DataSourceID .
			try:
				select_Query="SELECT ScraperFileName,DataSourceID,DataSourceName,MarketName,ScraperParameters FROM ScraperSchedule WHERE DataSourceID IN (SELECT DataSourceID FROM ScraperSchedule WHERE  Hostname='"+str(Host_ip).strip()+"' AND TaskStatus='Allotted')"    
				mysql_connection.execute(select_Query)
				result = mysql_connection.fetchall()
				print "Result",result
			except Exception as e:
				error_log=e;
				Fundalytics_Utility.log('','PTC-Module',error_log,'Error','')
		else:
			File_check=os.path.isfile("/home/merit/argusmedia/"+str(ScraperFileName))
			print "File_check",File_check, os.getcwd()
			
			# If the file is available then only this block will be excute
			
			if File_check:
				print "\nGoing to run..\n"
				
				try:
					Update_Query="UPDATE ScraperSchedule SET TaskStatus='started' WHERE DataSourceID='"+DataSourceID+"'"
					mysql_connection.execute(Update_Query)
					connection.commit()
				except Exception as e:
					error_log=e;
					Fundalytics_Utility.log('DataSourceID','PTC-Module',error_log,'Error','')
				
				#os.system("nohup python "+str(ScraperFileName)+" "+str(DataSourceName)+" "+str(MarketName)+" "+str(MarketName)+" "+str(ScraperParameters)+" &")
				os.system("nohup /usr/bin/python /home/merit/argusmedia/"+str(ScraperFileName)+" "+str(DataSourceID)+" "+str(DataSourceName)+" "+str(MarketName)+" "+str(ScraperParameters)+" "+str(Host_ip).strip()+" &")
				# print "Host IP",Host_ip
				
				# os.system("/usr/bin/python /home/merit/argusmedia/"+str(ScraperFileName)+" "+str(DataSourceID)+" "+str(DataSourceName)+" "+str(MarketName)+" "+str(ScraperParameters)+" "+str(Host_ip).strip()+" &")
				

			else:
				error_log='File not available in the path'
				Fundalytics_Utility.log('DataSourceID','PTC-Module',error_log,'Error','')
                
    # Disconnect the database connection.
mysql_connection.close()
connection.close()
