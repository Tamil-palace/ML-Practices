#!/usr/bin/python
import threading
import time
import pymysql.cursors
# import logging
import random
import Queue
import pymysql
import os, sys
# import configparser
import time
import datetime
import subprocess
from time import strftime
import imp

# logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s', )

BUF_SIZE = 3
q = Queue.Queue(BUF_SIZE)

Hostname = subprocess.Popen(['hostname -I'], stdout=subprocess.PIPE,shell=True)
Host_ip = Hostname.communicate()[0]

class ProducerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, Host_ip=None):
        super(ProducerThread, self).__init__()
        self.target = target
        self.name = name
        self.hostip = Host_ip


    def run(self):
        while True:
            if not q.full():
				# cursor=''
				try:
					connection = pymysql.connect(host='ec2-54-217-30-199.eu-west-1.compute.amazonaws.com',database='Fundalytics',user='merit',password='merit@123')
					cursor = connection.cursor()

				except Exception as e:
					error_log=e;
					print error_log
				
				try:
					Update_Query="UPDATE ScraperSchedule SET  Hostname='"+str(Host_ip).strip()+"', TaskStatus='Allotted', ExecutionStartDateTime = NOW(), ExecutionEndDateTime=NULL , ErrorReason=NULL, ProcessID=NULL WHERE DataSourceID IN (SELECT DataSourceID FROM (SELECT DataSourceID  FROM ScraperSchedule WHERE TaskStatus='idle' AND NextRetryStartDateTime <= NOW() AND IsActive = 'n' LIMIT 2)AS dt )"
					cursor.execute(Update_Query)
					connection.commit()
				except Exception as e:
					error_log=e;
					print error_log
				
				try:
					select_Query="SELECT ScraperFileName,DataSourceID,DataSourceName,MarketName,ScraperParameters FROM ScraperSchedule WHERE DataSourceID IN (SELECT DataSourceID FROM ScraperSchedule WHERE  Hostname='"+str(Host_ip).strip()+"' AND TaskStatus='Allotted')"
					cursor.execute(select_Query)
					result = cursor.fetchall()

				except Exception as e:
					error_log = e;
					print error_log

				for row in result:
					ScraperFileName = row[0]
					DataSourceID = row[1]
					DataSourceName = row[2]
					MarketName = row[3]
					ScraperParameters= row[4]
					print "\npushinto Queue",DataSourceName,"\n"
					try:
						Update_Query="UPDATE ScraperSchedule SET TaskStatus='started' WHERE DataSourceID='"+DataSourceID+"'"
						cursor.execute(Update_Query)
						connection.commit()
					except Exception as e:
						error_log=e;

					q.put("/usr/bin/python /home/merit/argusmedia/"+str(ScraperFileName)+" "+str(DataSourceID)+" "+str(DataSourceName)+" "+str(MarketName)+" "+str(ScraperParameters)+" "+str(Host_ip).strip())
					# q.put(os.system("/usr/bin/python /home/merit/argusmedia/"+str(ScraperFileName)+" "+str(DataSourceID)+" "+str(DataSourceName)+" "+str(MarketName)+" "+str(ScraperParameters)+" "+str(Host_ip).strip()+""))
					# q.put("usr/bin/python /home/merit/argusmedia/Fundalytics_Scrapy.py 1001-001 ibex block-products none 10.12.145.17")
					
					
            time.sleep(60)
			# connection.close()

        return


class ConsumerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ConsumerThread, self).__init__()
        self.target = target
        self.name = name
        return

    def run(self):
        while True:
            if not q.empty():
                item = q.get()
                os.system(item+" &")
                print "\nqueue deleting ",item

                time.sleep(5)
        return


if __name__ == '__main__':

    jobs = []

    #for i in result
    print "ProducerThread\n"
    p = ProducerThread(target='ProducerThread')
    jobs.append(p)
    p.daemon = True
    for j in jobs:
        j.start()
    print "ConsumerThread\n"
    c = ConsumerThread(name='ConsumerThread')

    time.sleep(2)
    c.start()
    time.sleep(2)

