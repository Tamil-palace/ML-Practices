#-*- coding: utf-8 -*-
import pypyodbc
import time
import xlrd
import csv
import re,sys
from click._compat import raw_input
import os


def DB_connection():
    try:
#         connection_string ='Driver={SQL Server Native Client 11.0};Server=CH1DEVBD01;Database=OCR_Staging;Uid=User2;Pwd=Merit456;'
#         connection_string ='Driver={SQL Server Native Client 11.0};Server=CH1025BD03;Database=OCR;Uid=User2;Pwd=Merit456;'
#         connection = pypyodbc.connect(server="CH1025BD03",database="OCR",user="User2",password="Merit456")
#         connection = pypyodbc.connect(connection_string)
#live
        # connection = pymssql.connect(host='CH1025BD03', user='User2', password='Merit456', database='OCR')
        #staging
        connection = pymssql.connect(host='CH1DEVBD01', user='User2', password='Merit456', database='OCR_Staging')
        return connection
        
    except Exception as e:
        print ("error_log",e)
        error_log = e;

def dataInsert(fileName,tableName,clusterDetailsSno):
    connection = DB_connection()
    mssql_connection = connection.cursor()
    
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    f1 = open(fileName, 'rt', encoding="ISO-8859-1")
    reader = csv.reader(f, delimiter=',')
    reader1 = csv.reader(f1, delimiter=',')
    inputHeader = next(reader1)
    titleIndex=inputHeader.index("Title")
    
    trackItemIndex=inputHeader.index("Track Item")
#     print (reader)
    
    sourcecontent=[]
    for data in reader:
        
        sourcecontent.append(data)
#     print (len(sourcecontent))
    header=''
    tableValue=''
    recortCount=0
    totalcount= len(list(reader))
    totalcount1= len(sourcecontent)
    
    query=''
    headercount=0
    count=0
    for i, data in enumerate(sourcecontent):
#         print ("totalcount1",data)
#         input("data")
        if i == 0:
            for j,dt in enumerate(data):
                dt = dt.replace(' ','_')
                dt = dt.replace('Retailer_Item_ID1','Retailer_Item_ID')
                if j ==0:
                    header = 'insert into '+str(tableName)+' (['+str(dt)+']'
                elif j >0:
                    header = header +',['+str(dt)+']'
                headercount +=1
            header = header +',CatalogDetailsSno) values '
        elif recortCount >0:
            tableValueTemp=''
            endloop=0
            if 'needs review' not in str(re.sub(r"\s+\s*"," ",str(data[trackItemIndex]))).lower() and 'Output' in tableName:
                totalcount = totalcount - 1
#                 print ("i",i)

                if i == (totalcount1 - 1):
#                     print ("totalcount",totalcount1)
#                     input("sfsdf")
                    endloop=1
                else:
                    continue
            if endloop == 0:
                for k,dt_val in enumerate(data):
                    dt_val = dt_val.replace("'","''")
                    
                        
                    if k ==0:
                        tableValueTemp =  "('"+str(dt_val).strip()+ "'"
                    elif k >0:
                        if k == 6:
                            val=''
                            val=dt_val.encode('utf8')
                            val=re.sub(r"^b\'","",str(val))
                            val=re.sub(r"\'$","",str(val))
                            val=re.sub(r"^b\"","",str(val))
                            val=re.sub(r"\"$","",str(val))
                            val=re.sub(r"\\\'","'",str(val))
    
                            tableValueTemp = tableValueTemp +",'"+str(val).strip()+ "'"
                        else:
                            tableValueTemp = tableValueTemp +",'"+str(dt_val).strip()+ "'"
                   
                    if len(data) -1 == k and len(data) < headercount:
                        for tt in range((headercount - len(data))):
                            tableValueTemp = tableValueTemp +",''"
    
                tableValueTemp = tableValueTemp +",'"+str(clusterDetailsSno)+"')"        
                tableValue = tableValue +","+ tableValueTemp
        recortCount += 1
        if recortCount == 900  :
            
            tableValue = re.sub(r"^\,","",str(tableValue))
            query = (header +" "+tableValue+";")
#             print ("query",query) 
            mssql_connection.execute(query)
            connection.commit()
            totalcount = totalcount - recortCount
#             print ("totalcount",totalcount)
            tableValue,query='',''
            recortCount = 1
        elif totalcount < 900:
            tableValue = re.sub(r"^\,","",str(tableValue))
            query = (header +" "+tableValue+";")
        elif i == (totalcount1 - 1):
            tableValue = re.sub(r"^\,","",str(tableValue))
            query = (header +" "+tableValue+";")
        
#     print ("query",query)
    if str(query) !='':
#         print ("query",query) 
        mssql_connection.execute(query)
        connection.commit()
        tableValue,query='',''
        print ("Query Executed")
        
    updatequery="update "+str(tableName)+" set retailer_item_id = replace(retailer_item_id, '''', '') where retailer_item_id like '%''%'"
    mssql_connection.execute(updatequery)
    connection.commit()


if __name__ == "__main__":
    connection = DB_connection()
    mssql_connection = connection.cursor()
    inputFiles =['383']
    for k, fl in enumerate(inputFiles): 
        outputfiletemp = 'D:\\OCR\\29_01_2018\\Catalog_files\\'+str(fl)+'_output.csv'
        
        csvf = open(outputfiletemp, 'wt', newline='')
        writer = csv.writer(csvf)
        connection = DB_connection()
        mssql_connection = connection.cursor()
#         dbQuery='select * from ['+str(fl)+'_Output] where ProcessStatus =2'
        dbQuery='select * from ['+str(fl)+'_Output]'
        resultquery = mssql_connection.execute(dbQuery)
        result=resultquery.fetchall()
        title = [i[0] for i in resultquery.description]
        writer.writerow(title)
        for row in result:
            lines = [re.sub(r"^NA$", "", str(l).replace('&AMP;', '&')).strip() for l in row]
            writer.writerow(lines)
        mssql_connection.close()
        connection.close()
        csvf.close()