#-*- coding: utf-8 -*-
# import pypyodbc
import pymssql
import time
import xlrd
import csv
import re,sys
from click._compat import raw_input
import os


import imp
run = imp.load_source('run', '/home/merit/OCR/AsinINfo/run.py')

from run import DB_update
from run import Log_writer
# from run import Start_Process

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]


def DB_connection():
    try:
        # connection_string ='Driver={SQL Server Native Client 11.0};Server=CH1DEVBD01;Database=OCR_Staging;Uid=User2;Pwd=Merit456;'
#         connection_string ='Driver={SQL Server Native Client 11.0};Server=CH1025BD03;Database=OCR;Uid=User2;Pwd=Merit456;'
#         connection = pypyodbc.connect(server="CH1025BD03",database="OCR",user="User2",password="Merit456")
        #live
        connection = pymssql.connect(host='CH1025BD03', user='User2', password='Merit456', database='OCR')
        #staging
        # connection = pymssql.connect(host='CH1DEVBD01', user='User2', password='Merit456', database='OCR_Staging')
        # print(connection_string)
        # connection = pymssql.connect(connection_string)
        # print(connection)
        return connection
        
    except Exception as e:
        print ("error_log",e)
        error_log = e;
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, error_log, "-6", "Database Import Failure")

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
    print(connection)
    mssql_connection = connection.cursor()
    print(mssql_connection)
    # sys.exit();
    # inputFiles =['59-Catalog-20180207.csv']
    # outputFile = ['59-Catalog-20180207_p1.csv']
    
    # for k, fl in enumerate(inputFiles):
        
    # fileName = '/home/merit/OCR/AsinINfo/Catalog_files/'+str(fl)
    fileName=sys.argv[1]
    # outputfileName = '/home/merit/OCR/AsinINfo/Catalog_files/'+str(outputFile[k])
    outputfileName =sys.argv[2]
    # outputfileName

    f1 = open(fileName, 'rt', encoding="ISO-8859-1")
    outf1 = open(outputfileName, 'rt', encoding="ISO-8859-1")

    reader1 = csv.reader(f1, delimiter=',')
    outreader1 = csv.reader(outf1, delimiter=',')

    head, base = os.path.split(fileName)
    cNumber=re.findall(r"^([\d]+)",str(base))[0]

    inputHeader = next(reader1)
    outputHeader = next(outreader1)

    queryForm='Sno int identity(1,1) primary key,CatalogDetailsSno int, '
    outputqueryForm='Sno int identity(1,1) primary key,CatalogDetailsSno int, '

    for vl in inputHeader:
        queryForm = str(queryForm) +"[" + str(vl).replace(" ", "_").replace('Retailer_Item_ID1','Retailer_Item_ID') +"] nvarchar(1000), "
    queryForm = re.sub("\,\s*$", "", str(queryForm))

    for v2 in outputHeader:
        outputqueryForm = str(outputqueryForm) +"[" + str(v2).replace(" ", "_").replace('Retailer_Item_ID1','Retailer_Item_ID') +"] nvarchar(1000), "

    queryForm = re.sub("\,\s*$", "", str(queryForm))
    if 'user_id' in str(outputqueryForm).lower():
        outputqueryForm = re.sub("\,\s*$", "", str(outputqueryForm)) + ", ProcessStatus nvarchar(100),User_Type nvarchar(100),Processed_Date  date"
    else:
        outputqueryForm = re.sub("\,\s*$", "", str(outputqueryForm)) + ", ProcessStatus nvarchar(100),User_Type nvarchar(100),User_Id nvarchar(100),Processed_Date  date"

    inputTableName="["+str(cNumber)+"_Input]"
    outputTableName="["+str(cNumber)+"_Output]"

    queryForm = "create table "+str(inputTableName) +" ("+str(queryForm) +");"
    outputqueryForm = "create table "+str(outputTableName) +" ("+str(outputqueryForm) +");"

    checktable= "select * from "+str(inputTableName)

    tableexist=0

    try:
        print("checktable:",checktable)
        mssql_connection.execute(checktable)
        mssql_connection.execute("truncate table "+str(inputTableName))
        connection.commit()
        mssql_connection.execute("truncate table "+str(outputTableName))
        connection.commit()
    except Exception as e:
        if 'Invalid object name' in str(e):
            tableexist=1
            print ("Table Not Found")
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, "Table Not Found", "-6", "Database Import Failure")
        else:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-6", "Database Import Failure")

    if tableexist == 1:
        print("queryForm ====>",queryForm)
        mssql_connection.execute(queryForm)
        connection.commit()
        print("outputqueryForm =====>",outputqueryForm)
        mssql_connection.execute(outputqueryForm)
        connection.commit()
    print("inputTableName-->"+inputTableName+"fileName-->"+fileName)
    dataInsert(fileName,inputTableName,"1")
    print("outputTableName-->",outputTableName+"outputfileName"+outputfileName)
    dataInsert(outputfileName,outputTableName,"1")
    DB_update(cNumber, "6", "Database Import Success")
