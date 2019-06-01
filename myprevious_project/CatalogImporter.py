#-*- coding: utf-8 -*-
# import pypyodbc
import pymssql
import time
import xlrd
import csv
import re,sys
from click._compat import raw_input
import os
from elasticsearch import Elasticsearch
import elasticsearch,requests
from elasticsearch import helpers
import imp
import configparser
import ftfy
from ftfy import fix_encoding
import ast

dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir+'/AutomationController.py')

from AutomationController import DB_update
from AutomationController import Log_writer
from AutomationController import DB_connection


catalogs_backup_index="catalogs_backup"
config = configparser.ConfigParser()
config.read('Config.ini')
host=config.get("Elastic-Search", "host")
port=config.get("Elastic-Search", "port")

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]

DBImportStarted=config.get("Status", "DBImport-Started")
DBImportFailed=config.get("Status", "DBImport-Failed")
DBImportCompleted=config.get("Status", "DBImport-Completed")


head, base = os.path.split(sys.argv[1])
cNumber = re.findall(r"^([\d]+)", str(base))[0]

try:
    date = re.findall(r"Catalog\-([\d]+)\_", str(base))[0]
except:
    date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

es = Elasticsearch([{'host': host, 'port': port}])
doc_type=cNumber+"_"+date


def reindex_function(old_index,reindexing_index):
    try:
        data_point = {
            "source": {
                "index": old_index
            },
            "dest": {
                "index": reindexing_index
            }
        }
        print(str(data_point).replace("'", "\""))
        reindex = requests.post("http://" + str(host) + ":" + str(port) + "/_reindex",data=str(data_point).replace("'", "\""))
        print(reindex.status_code == 200)
        if reindex.status_code == 200:
            es.indices.delete(index=old_index)
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Index Health is RED."+str(old_index)+" deleted... :  " + str(sys.exc_info()[-1].tb_lineno),"26", "Index Health is RED."+str(old_index)+" deleted...")
            print(str(old_index)+" index deleted.......")
    except Exception as e:
        print(e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e)+" :  " + str(sys.exc_info()[-1].tb_lineno), "28","Issue in ReIndexing...")
        sys.exit(0)


try:
    setting = requests.get("http://" + str(host) + ":" + str(port) + "/_cluster/health/" +str(catalogs_backup_index)+"?level=shards&pretty").json()
    print(setting)
    if setting['indices'][catalogs_backup_index]["status"] == 'red':
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(catalogs_backup_index) + " Index Health is RED.",str(TitleGroupingFailed), str(catalogs_backup_index) + " Index Health is RED.")
        with open("index_red_health.txt","a") as fh:
            fh.write(str(catalogs_backup_index) +"=====>"+ str(cNumber)+"  ======>   " + str(setting['indices'][catalogs_backup_index]["status"])+"\n")
        reindexing_index = str(catalogs_backup_index) + "_reindex"
        reindex_function(catalogs_backup_index,reindexing_index)
        reindex_function(reindexing_index,catalogs_backup_index)
except Exception as e:
    print(e)
    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),str(TitleGroupingFailed), str(catalogs_backup_index)+" Index Health is RED.")
    sys.exit(0)
print(catalogs_backup_index)


def Insert_update(ID_list,sourcecontent,sourcecontent_id,trackItemIndex,reader,headers,tableName,clusterDetailsSno,idIndex,DB_option):
    try:
        header = ''
        tableValue = ''
        recortCount = 0
        totalcount = len(list(reader))
        print(totalcount)
        totalcount1 = len(ID_list)
        query = ''
        headercount = 0
        count = 0

        if DB_option=='update':
            relativeProductCount=10
            update_list = [sourcecontent[sourcecontent_id.index(val)] for val in ID_list if val!='Retailer Item ID' and val!='' ]
            try:
                update_flag = "ALTER TABLE " + str(tableName) + " ADD update_flag nvarchar(max);"
                mssql_connection.execute(update_flag)
                connection.commit()
            except Exception as e:
                if "Column name 'update_flag' in table" in str(e):
                    pass

            header = ''
            for i,data in enumerate(update_list):
                update_flag=True
                for j,val in enumerate(data):
                    dt = headers[j].replace(' ', '_')
                    dt = re.sub(r"([^a-zA-Z,.-\\d])*?\"", "", str(dt))
                    dt = dt.replace('Retailer_Item_ID1', 'Retailer_Item_ID')
                    if 'Retailer_Item_ID'==dt:
                        continue
                    if update_flag:
                        header =header + 'update ' + str(tableName) + " set update_flag='updated'"
                        update_flag=False
                    header = header + ',[' + str(dt) + ']'+" = '" +str(val).replace("'","''")+"'"

                header=header+" where [Retailer_Item_ID]= '"+update_list[i][idIndex].replace("'","")+"';"
                if i==relativeProductCount:
                    mssql_connection.execute(header)
                    connection.commit()
                    header = ''
                #     header+='SAVE transaction update_'+str(relativeProductCount)+';'
                #     header+="insert into save_point_tracker(Catalog_ID,Logs,Date) values("+cNumber+",'update_"+str(relativeProductCount)+"',getdate());"
                # print(header)
            # header=header+"commit;"
            mssql_connection.execute(header)
            connection.commit()
        else:
            for j, dt in enumerate(headers):
                dt = dt.replace(' ', '_')
                dt = re.sub(r"([^a-zA-Z,.-\\d])*?\"", "", str(dt))
                dt = dt.replace('Retailer_Item_ID1', 'Retailer_Item_ID')
                if j == 0:
                    header = 'insert into ' + str(tableName) + ' ([' + str(dt) + ']'
                elif j > 0:
                    header = header + ',[' + str(dt) + ']'
                headercount += 1
            header = header + ',CatalogDetailsSno) values '
            insert_list=[sourcecontent[sourcecontent_id.index(val)] for val in ID_list if val!='Retailer Item ID' and val!='' ]
            for i, data in enumerate(insert_list):
                if recortCount >-1:
                    tableValueTemp=''
                    endloop=0
                    if 'needs review' not in str(re.sub(r"\s+\s*"," ",str(data[trackItemIndex]))).lower() and 'Output' in tableName:
                        totalcount = totalcount
                        if i == (totalcount1):
                            endloop=1
                        else:
                            continue
                    if endloop == 0:
                        for k,dt_val in enumerate(data):
                            dt_val=ftfy.fix_text(str(dt_val))
                            dt_val = fix_encoding(re.sub(r"[^!-~\s]+", "", str(dt_val), re.I))
                            dt_val = dt_val.replace("'","''")
                            if k ==0:
                                tableValueTemp =  "('"+str(dt_val).strip()+ "'"
                            elif k >0:
                                if k == 6:
                                    val=''
                                    val=dt_val
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
                    # print(query)
                    #input()
                    mssql_connection.execute(query.replace('🎎',""))
                    connection.commit()
                    totalcount = totalcount - recortCount
                    tableValue,query='',''
                    recortCount = 1
                elif totalcount < 900:
                    tableValue = re.sub(r"^\,","",str(tableValue))
                    query = (header +" "+tableValue+";")
                elif i == (totalcount1):
                    tableValue = re.sub(r"^\,","",str(tableValue))
                    query = (header +" "+tableValue+";")
            if str(query) !='':
                try:
                    # print(query)
                    #input()
                    mssql_connection.execute(query.replace('🎎',""))
                    connection.commit()
                except Exception as e:
                    print(e)
                    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
                    sys.exit(0)
                tableValue,query='',''
                print ("Query Executed")
    except Exception as e:
        print(e)
        print(str(sys.exc_info()[-1].tb_lineno))
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
        sys.exit(0)
        # input()

def dataInsert(fileName,tableName,clusterDetailsSno):
    try:
        connection = DB_connection()
        mssql_connection = connection.cursor()

        f = open(fileName, 'rt', encoding="ISO-8859-1")
        f1 = open(fileName, 'rt', encoding="ISO-8859-1")
        reader = csv.reader(f, delimiter=',')
        reader1 = csv.reader(f1, delimiter=',')
        inputHeader = next(reader1)
        titleIndex=inputHeader.index("Title")
        idIndex = inputHeader.index("Retailer Item ID")
        trackItemIndex=inputHeader.index("Track Item")
        sourcecontent=[]
        for data in reader:
            sourcecontent.append(data)
    #     print (len(sourcecontent))

        File_new_list = [sourcecontent[index][idIndex].replace("'","") for index in range(0, len(sourcecontent))]
        try:
            MSexistingid = "select [Retailer_Item_ID] from " + str(tableName)
            mssql_connection.execute(MSexistingid)
            MSexistingid_list_tup = mssql_connection.fetchall()
            connection.commit()
            MSexistingid_list = [val[0] for val in MSexistingid_list_tup]
            insert_list = list(set(File_new_list) - set(MSexistingid_list))
            update_list = list(set(File_new_list) - set(insert_list))
        except Exception as e:
            print(e)
            if 'Invalid object name' in str(e):
                insert_list = [sourcecontent[index][idIndex] for index in range(1, len(sourcecontent))]
                update_list=[]

        headers=[val for val in sourcecontent[0]]
        # print(headers)
        Insert_update(insert_list, sourcecontent, File_new_list, trackItemIndex, reader, headers,tableName, clusterDetailsSno,idIndex,'insert')
        Insert_update(update_list, sourcecontent, File_new_list, trackItemIndex, reader, headers, tableName,clusterDetailsSno,idIndex, 'update')

        updatequery="update "+str(tableName)+" set retailer_item_id = replace(retailer_item_id, '''', '') where retailer_item_id like '%''%'"
        mssql_connection.execute(updatequery)
        connection.commit()
    except Exception as e:
        print(e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
        sys.exit(0)


def Elastic_Entry(list, op_type, field_names, doc_type, record_key="_source"):
    records = []
    count = 0
    for index, val1 in enumerate(list):
        doc = {}
        insertFlag = False
        for index1, val2 in enumerate(val1):
            doc[field_names[index1]] = val2
        try:
            Retailer_id = doc['Retailer_Item_ID']
        except Exception as e:
            print(str(e))
            print(str(sys.exc_info()[-1].tb_lineno))
            Retailer_id = doc['Retailer_Item_ID1']

        record = {
            "_op_type": op_type,
            "_index": catalogs_backup_index,
            "_type": doc_type,
            "_id": str(Retailer_id).strip(),
            record_key: doc
        }
        records.append(record)
        count += 1

        if count == 2000 or len(list) - 1 == index:
            print("Flag arised")
            insertFlag = True
            count = 0
        if insertFlag:
            try:
                helpers.bulk(es, records)
                records = []
            except helpers.BulkIndexError as e:
                print(e)
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(DBImportFailed), "DBImport Failed")

def List_Split(sourcecontent_id, sourcecontent, header_list):
    dic = {}
    dic["mappings"] = {}
    dic["mappings"][str(cNumber)] = {}
    dic["mappings"][str(cNumber)]["properties"] = {}
    for val in inputHeader:
        dic["mappings"][str(cNumber)]["properties"][val] = {}
        dic["mappings"][str(cNumber)]["properties"][val]["type"] = "string"

    setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(catalogs_backup_index)+"/", data=dic,headers={"Content-Type": "application/json"})

    # refresh index for every catalogs
    es.indices.refresh(catalogs_backup_index)

    url_1 = "http://"+str(host)+":"+str(port)+"/"+str(catalogs_backup_index)+"/" + str(doc_type) + "/_search"
    print(url_1)
    ping1 = requests.get(url_1).json()
    update_list = []
    insert_list = []
    window_flag=False
    try:
        window_size = {
            "max_result_window": int(ping1['hits']['total'])+10000
        }
        print(window_size)
        setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(catalogs_backup_index)+"/_settings",data=window_size, headers={"Content-Type": "application/json"})
        print(setting.content)
        window_flag=True
    except :
        pass

    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        print(url_2)
        ping2 = requests.get(url_2).json()
        existing_id_list = []
        for val in ping2['hits']['hits']:
            existing_id_list.append(val['_id'])

        insert_id_list = set(sourcecontent_id).difference(set(existing_id_list))
        for val in insert_id_list:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])

        update_id_list = set(sourcecontent_id).difference(set(insert_id_list))
        for val in update_id_list:
            update_list.append(sourcecontent[sourcecontent_id.index(val)])

    except KeyError as e:
        print(e, sys.exc_info()[-1].tb_lineno)
        for val in sourcecontent_id:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])
        # insert_list=source_id
        update_list = []
    except Exception as e:
        print(str(e)+" :  "+str(sys.exc_info()[-1].tb_lineno))
        Log_writer("OCR_ErrorLog_" + str(cNumber) + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
        pass
    if window_flag:
        window_size = {
            "max_result_window": 10000
        }
        setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(catalogs_backup_index)+"/_settings",data=window_size, headers={"Content-Type": "application/json"})

    Elastic_Entry(insert_list, 'create', header_list, doc_type)
    Elastic_Entry(update_list, 'update', header_list, doc_type, 'doc')


def backup_table(table):
    try:
        val1 = ""
        mssql_connection.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+table+"'")
        field_names = mssql_connection.fetchall();
        # field_names = [i[0] for i in mssql_connection.description]
        # print(field_names)
        header_list = []
        for val in field_names:
            val = re.sub(r'^\(\'', '', str(val))
            val = re.sub(r'\'\,\)$', '', str(val))
            header_list.append(val)
        field_names_modified = []
        for val in header_list:
            val1 = val1 + ",[" + str(val) + "]"
        field_names_modified.append(val1)
        Query_1 = re.sub(r'^\,', '', str(val1))
        Query_1 = "select " + (Query_1) + " from " + "[" + table + "]"
        # print(Query_1)
        # input()
        mssql_connection.execute(Query_1)
        Tabledata_list = mssql_connection.fetchall()
        Tabledata_list_id = []
        for val in Tabledata_list:
            Tabledata_list_id.append(val[header_list.index('Retailer_Item_ID')])
        List_Split(Tabledata_list_id, Tabledata_list, header_list)
    except Exception as e:
        print(e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
        sys.exit(0)

if __name__ == "__main__":
    connection = DB_connection()
    mssql_connection = connection.cursor()
    fileName=sys.argv[1]
    outputfileName =sys.argv[2]
    try:
        dropflag=ast.literal_eval(sys.argv[3])
    except:
        dropflag=False

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
        vl=re.sub(r"([^a-zA-Z,.-\\d])*?\"","",str(vl))
        queryForm = str(queryForm) +"[" + str(vl).replace(" ", "_").replace('Retailer_Item_ID1','Retailer_Item_ID') +"] nvarchar(max), "
    queryForm = re.sub("\,\s*$", "", str(queryForm))

    if "comments" not in outputHeader:
        outputHeader.append("comments")
    if "Matched ID" not in outputHeader and "Matched_ID" not in outputHeader:
        outputHeader.append("Matched ID")
    if "Score" not in outputHeader:
        outputHeader.append("Score")
    if "FuzzyScore" not in outputHeader:
        outputHeader.append("FuzzyScore")
    if "Track Item (Y/Z/N)" not in outputHeader and "Track_Item_(Y/Z/N)" not in outputHeader:
        outputHeader.append("Track Item (Y/Z/N)")
    if "Cluster ID" not in outputHeader and "Cluster_ID" not in outputHeader:
        outputHeader.append("Cluster ID")

    for v2 in outputHeader:
        outputqueryForm = str(outputqueryForm) +"[" + str(v2).replace(" ", "_").replace('Retailer_Item_ID1','Retailer_Item_ID') +"] nvarchar(max), "

    queryForm = re.sub("\,\s*$", "", str(queryForm))
    if 'user_id' in str(outputqueryForm).lower():
        outputqueryForm = re.sub("\,\s*$", "", str(outputqueryForm)) + ", ProcessStatus nvarchar(max),User_Type nvarchar(max),Processed_Date  date"
    else:
        outputqueryForm = re.sub("\,\s*$", "", str(outputqueryForm)) + ", ProcessStatus nvarchar(max),User_Type nvarchar(max),User_Id nvarchar(max),Processed_Date  date, Assigned_To nvarchar(max)"

    inputTableName="["+str(cNumber)+"_Input]"
    outputTableName="["+str(cNumber)+"_Output]"

    queryForm = "create table "+str(inputTableName) +" ("+str(queryForm) +");"
    outputqueryForm = "create table "+str(outputTableName) +" ("+str(outputqueryForm) +");"
    checktable= "select * from "+str(inputTableName)
    tableexist=0

    try:
        print("checktable:",checktable)
        mssql_connection.execute(checktable)

        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, "Elastic Search DB backup started","33", "Elastic Search DB backup started")
        backup_table(str(cNumber) + str('_Input'))
        backup_table(str(cNumber)+str('_Output'))
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, "Elastic Search DB backup Completed", "34","Elastic Search DB backup Completed")

        # fileName_test=re.findall(r"\_p.*?\.csv$", fileName, flags=re.I)
        # if fileName_test or dropflag:
        if dropflag:
            print("dropping table..............")
            mssql_connection.execute("drop table " + str(inputTableName))
            connection.commit()
            mssql_connection.execute("drop table " + str(outputTableName))
            connection.commit()
            tableexist=1
    except Exception as e:
        print(e)
        if 'Invalid object name' in str(e):
            tableexist=1
            print ("Table Not Found")


    if tableexist == 1:
        print("queryForm ====>",queryForm)
        try:
            mssql_connection.execute(queryForm)
            connection.commit()
        except Exception as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "InputTable DBImport Failed")
            sys.exit(0)

        print("outputqueryForm =====>",outputqueryForm)
        try:
            mssql_connection.execute(outputqueryForm)
            connection.commit()
        except Exception as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "InputTable DBImport Failed")
            sys.exit(0)

    print("inputTableName-->"+inputTableName+"fileName-->"+str(fileName))
    try:
        dataInsert(fileName,inputTableName,"1")
    except Exception as e:
        print(str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno))
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(DBImportFailed),"InputTable DBImport Failed")
        sys.exit(0)

    print("outputTableName-->",outputTableName+"outputfileName"+str(outputfileName))
    try:
        mssql_connection.execute("truncate table " + str(outputTableName))
        connection.commit()
        dataInsert(outputfileName,outputTableName,"1")
    except Exception as e:
        print(str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno))
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno), str(DBImportFailed),"Output table DBImport Failed")
        sys.exit(0)

    flag = False
    secondflag=False
    val1 = ""
    for val in inputHeader:
        if val.lower()=='track item (y/z/n)' or secondflag:
            secondflag=True
            continue
        if val.lower() == 'title' or flag:
            flag = True
            val1 = val1 + str(val) + ","

    queryForm = re.sub(r",$", "", str(val1))
    Headers = 'Track Item' + "," + str(queryForm)
    req_headers = "Retailer_Item_ID," + Headers + ",Track Item (Y/Z/N)"

    try:
        startQuery = "select count(*) from [" + str(cNumber) + "_Output]"
        mssql_connection.execute(startQuery)
        NR_Count = mssql_connection.fetchone()[0]
        connection.commit()

        TotalcountQuery = "select count(*) from [" + str(cNumber) + "_Input]"
        mssql_connection.execute(TotalcountQuery)
        Totalcount = mssql_connection.fetchone()[0]
        connection.commit()

        RegionQuery="select top 1 TLD from  [" + str(cNumber) + "_Input]"
        mssql_connection.execute(RegionQuery)
        Region = mssql_connection.fetchone()[0]
        connection.commit()
    except Exception as e:
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "InputTable DBImport Failed")
        sys.exit(0)

    DashboardQuery="insert into Catalog_Details (CatalogID,FileName,TotalCount,NR_Count,TaskStatus,Headers,RequiredHeader,Environment,PivotHeaders,Region,CreatedDateTime) values ('"+str(cNumber)+"','"+str(base)+"','"+str(Totalcount)+"','"+str(NR_Count)+"','"+'Uploaded'+"','"+str(Headers)+"','"+str(req_headers)+"','"+'Live'+"','"+'Client Product Group,Category,Subcategory'+"','"+str(Region.lower())+"',GETDATE())"
    try:
        print(DashboardQuery)
        mssql_connection.execute(DashboardQuery)
        connection.commit()
    except Exception as e:
        #Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),"", "")
        DashboardDeleteQuery="delete Catalog_Details where CatalogID="+str(cNumber)
        mssql_connection.execute(DashboardDeleteQuery)
        connection.commit()
        mssql_connection.execute(DashboardQuery)
        connection.commit()
    Log_writer("OCR_generic_error.log", cNumber, "DBImport Completed", str(DBImportCompleted), "DBImportCompleted")


