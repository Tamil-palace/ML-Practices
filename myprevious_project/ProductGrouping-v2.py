# -*- coding: utf-8 -*-
import sys
import requests
import math
import collections
import urllib3
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from elasticsearch import Elasticsearch
import elasticsearch
import pandas as pd
import os
import re
import xlrd
import csv
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
import imp
from elasticsearch import helpers
import json
import redis
import configparser

config = configparser.ConfigParser()
config.read('Config.ini')

dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir + '/AutomationController.py')

from AutomationController import DB_update
from AutomationController import Log_writer
from AutomationController import Start_Process

head, base = os.path.split(sys.argv[1])
cNumber = re.findall(r"^([\d]+)", str(base))[0]

host=config.get("Elastic-Search","host")
port=config.get("Elastic-Search","port")

source_indexing_index="source_indexing"
es = Elasticsearch([{'host': host, 'port': port}],timeout=30)

try:
    date = re.findall(r"Catalog\-([\d]+)\_", str(base))[0]
except:
    date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

r = redis.StrictRedis()


doc_type = cNumber
r.set(str(cNumber)+"_"+str(date)+"_3_progress", 0)
r.set(str(cNumber) + "_" + str(date) + "_3_total", 0)

TitleGroupingStarted=config.get("Status", "Title-Grouping-Started")
TitleGroupingFailed=config.get("Status", "Title-Grouping-Failed")
TitleGroupingCompleted=config.get("Status", "Title-Grouping-Completed")

setting = requests.get("http://"+str(host)+":"+str(port)+"/_cluster/health/?level=shards&pretty").json()
print(setting)
# con=json.load(str(setting.content))
try:
    for val in setting['indices']:
        print(str(val) + "  ======>   " + str(setting['indices'][val]["status"]))
        if val==source_indexing_index:
            source_index=source_indexing_index
            if setting['indices'][val]["status"] == 'red':
                with open("index_red_health.txt","a") as fh:
                    fh.write(str(val) +"=====>"+ str(cNumber)+"  ======>   " + str(setting['indices'][val]["status"])+"\n")
                "http://172.27.138.44:9200/_nodes/stats/jvm?pretty"
                # if re.findall(r'_\d+',source_indexing_index,re.I):
                #     tar_index=re.findall(r'_(\d+)', source_indexing_index, re.I)[0]
                #     tar_index=int(tar_index)+1
                #     source_indexing_index=re.sub(r"_\d+","_"+str(tar_index),str(source_indexing_index),re.I)
                #     es.indices.delete(index=source_indexing_index)
                #     helpers.reindex(es, source_index, source_indexing_index, target_client=es)
                # else:
                source_indexing_index=str(source_indexing_index)+"_temp"
                # es.indices.delete(index=source_indexing_index)
                helpers.reindex(es, source_index, source_indexing_index, target_client=es)
except Exception as e:
    print(e)
    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),str(TitleGroupingFailed), "TitleGrouping Failed")
    sys.exit(0)
print(source_indexing_index)
input()

def word_tokenizer(text):
    tokens = word_tokenize(text)
    try:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(t) for t in tokens if t not in stopwords.words('english')]
        return tokens
    except:
        return tokens


def needs_review_values(sourcecontent, titleIndex, trackIndex, idIndex):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, Ytitle, dataVal = [], [], []
    for row_number in range(1, int(max_row)):
        tempdata = []
        if 'needs review' in str(sourcecontent[row_number][trackIndex]).lower():
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())
        elif 'needs review' not in str(sourcecontent[row_number][trackIndex]).lower():
            # elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
            tempdata.append(str(sourcecontent[row_number][idIndex]).strip())
            tempdata.append(str(sourcecontent[row_number][titleIndex]).strip())
            dataVal.append(tempdata)

    return id, Ytitle, dataVal


def Elastic_Entry(list, op_type, field_names, doc_type, record_key="_source"):
    print(op_type)
    records = []
    count = 0
    for index, val1 in enumerate(list):
        doc = {}
        insertFlag = False
        for index1, val2 in enumerate(val1):
            doc[field_names[index1]] = val2

        try:
            Retailer_id = doc['Retailer Item ID']
        except:
            Retailer_id = doc['Retailer Item ID1']
        record = {
            "_op_type": op_type,
            "_index": source_indexing_index,
            "_type": doc_type,
            "_id": str(Retailer_id).strip(),
            record_key: doc
        }
        # print(records)
        records.append(record)
        count += 1
        print(index)

        if count == 2000 or len(list) - 1 == index:
            print("Flag arised")
            insertFlag = True
            count = 0
        if insertFlag:
            try:
                print(helpers.bulk(es, records))
                records = []

            except helpers.BulkIndexError as e:
                print(e)
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e) + " : " + str(sys.exc_info()[-1].tb_lineno), str(TitleGroupingFailed),"TitleGrouping Failed")
            except Exception as e:
                print(e)
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,str(e) + " : " + str(sys.exc_info()[-1].tb_lineno), str(TitleGroupingFailed),"TitleGrouping Failed")

def List_Split(source, sourcecontent_id, sourcecontent, header_list):
    source_id = [source[i][0] for i, id in enumerate(source)]
    # implementing proper schema using mapping for every catalogs
    dic = {}
    dic["mappings"] = {}
    dic["mappings"][str(cNumber)] = {}
    dic["mappings"][str(cNumber)]["properties"] = {}
    for val in header_list:
        dic["mappings"][str(cNumber)]["properties"][val] = {}
        dic["mappings"][str(cNumber)]["properties"][val]["type"] = "string"

    setting = requests.put("http://" + str(host) + ":" + str(port) + "/" + str(source_indexing_index) + "/", data=dic,headers={"Content-Type": "application/json"})

    # refresh index for every catalogs
    es.indices.refresh(source_indexing_index)

    window_size = {
        "max_result_window": 90000000
    }

    setting = requests.put("http://"+str(host)+":"+str(port)+"/"+str(source_indexing_index)+"/_settings", data=window_size,headers={"Content-Type": "application/json"})
    print(setting.content)
    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"source_indexing Window size was increased : " + str(setting.content), "16","source_indexing Window size was increased")
    url_1 = "http://"+str(host)+":"+str(port)+"/"+str(source_indexing_index)+"/" + str(doc_type) + "/_search"
    print(url_1)
    ping1 = requests.get(url_1).json()
    update_list = []
    insert_list = []
    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        print(url_2)
        ping2 = requests.get(url_2).json()
        existing_id_list = []
        for val in ping2['hits']['hits']:
            existing_id_list.append(val['_id'])

        insert_id_list = set(source_id).difference(set(existing_id_list))
        for val in insert_id_list:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])

        update_id_list = set(source_id).difference(set(insert_id_list))
        for val in update_id_list:
            update_list.append(sourcecontent[sourcecontent_id.index(val)])

    except KeyError as e:
        print(e, sys.exc_info()[-1].tb_lineno)
        for val in source_id:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])
        # insert_list=source_id
        update_list = []
    except Exception as e:
        print(str(e)+" :  "+str(sys.exc_info()[-1].tb_lineno))
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " :  " + str(sys.exc_info()[-1].tb_lineno),str(DBImportFailed), "DBImport Failed")
        pass

    # window_size = {
    #     "max_result_window": 10000
    # }
    # setting = requests.put("http://" + str(host) + ":" + str(port) + "/"+str(source_indexing_index)+"/_settings", data=window_size,headers={"Content-Type": "application/json"})
    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"source_indexing_index Window size was reduced: " + str(setting.content), "17","source_indexing Window size was reduced")
    Elastic_Entry(insert_list, 'create', header_list, doc_type)
    Elastic_Entry(update_list, 'update', header_list, doc_type, 'doc')

# def List_Split(source, sourcecontent_id, sourcecontent, replaceValueHeader):
#
#     source_id = [source[i][0] for i, id in enumerate(source)]
#     url_1 = "http://"+str(host)+":"+str(port)+"/source_indexing_1_1/" + str(doc_type) + "/_search"
#     print(url_1)
#     ping1 = requests.get(url_1).json()
#     update_list = []
#     insert_list = []
#     existing_id_list = []
#     try:
#         relative_count=10000
#         pingcount = int(ping1['hits']['total']) /relative_count
#         pingcount=re.sub(r'\..*', '', str(pingcount))
#         print(int(pingcount))
#         print("pingcount")
#         if int(pingcount) > 0:
#             for i in range(0,int(pingcount)):
#                 url_2 = url_1 + "?size="+str(relative_count)
#                 print(url_2)
#                 ping2 = requests.get(url_2).json()
#                 for val in ping2['hits']['hits']:
#                     existing_id_list.append(val['_id'])
#
#         if int(ping1['hits']['total'])!=0:
#             remaingsize = int(str(ping1['hits']['total'])) % relative_count
#             url_2 = url_1 + "?size=" + str(remaingsize)
#             print(url_2)
#             ping2 = requests.get(url_2).json()
#             for val in ping2['hits']['hits']:
#                 existing_id_list.append(val['_id'])
#
#             insert_id_list = set(source_id).difference((existing_id_list))
#             for val in insert_id_list:
#                 insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#
#             update_id_list = set(source_id).difference(set(insert_id_list))
#             for val in update_id_list:
#                 update_list.append(sourcecontent[sourcecontent_id.index(val)])
#
#         elif int(ping1['hits']['total'])==0 :
#             for val in source_id:
#                 insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#             update_list = []
#     except KeyError as e:
#         Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),"", "")
#         print(e, sys.exc_info()[-1].tb_lineno)
#         for val in source_id:
#             insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#         update_list = []
#     except Exception as e:
#         print(e)
#         Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e)+" : "+str(sys.exc_info()[-1].tb_lineno), str(TitleGroupingFailed), "TitleGrouping Failed")
#
#     Elastic_Entry(insert_list, 'create', replaceValueHeader, doc_type)
#     Elastic_Entry(update_list, 'update', replaceValueHeader, doc_type, 'doc')


if __name__ == "__main__":
    # es = Elasticsearch([{'host': '172.27.138.44', 'port': 9200}])
    #es = Elasticsearch(timeout=30)
    es = Elasticsearch([{'host': host, 'port': port}],timeout=30)
    # try:
    #     es.ping()
    #     print("connected")
    # except urllib3.exceptions.NewConnectionError as e:
    #     # print(e)
    #     print("Failed")
    #     print (e, sys.exc_info()[-1].tb_lineno)
    #     Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-5", "Title Grouping Failure")
    #     sys.exit(1)
    # except Exception as e:
    #     print (e, sys.exc_info()[-1].tb_lineno)
    #     sys.exit(1)
    # sys.exit(1)

    startTime = datetime.now()
    print(startTime)
    fileName = str(sys.argv[1])
    outputfile = ''
    if 'p1.csv' in fileName:
        outputfile = fileName.replace('p1.csv', 'p2.csv')
    else:
        outputfile = fileName.replace('.csv', 'p2.csv')
    head, base = os.path.split(fileName)

    cNumber = re.findall(r"^([\d]+)", str(base))[0]
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    df = pd.read_csv(fileName, encoding='latin1')
    replaceValueHeader = (list(df.columns.values))
    titleIndex = replaceValueHeader.index("Title")
    trackIndex = replaceValueHeader.index("Track Item")

    idIndex = ''
    try:
        idIndex = replaceValueHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex = replaceValueHeader.index("Retailer Item ID1")
        df = df.set_index("Retailer Item ID1")
    indexreference = str(cNumber) + '_catalog'
    print("Processing files::", fileName)
    reader = ''
    sourcecontent = []

    reader = csv.reader(f, delimiter=',')

    for data in reader:
        sourcecontent.append(data)

    sourcecontent_id = [sourcecontent[i][idIndex] for i, id_source in enumerate(sourcecontent)]
    idList, sentences, source = needs_review_values(sourcecontent, titleIndex, trackIndex, idIndex)

    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Source indexing Started" , "21", "Source indexing Started....")
    List_Split(source, sourcecontent_id, sourcecontent, replaceValueHeader)
    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, "Source indexing Completed", "22", "Source indexing Completed")

    for i, title in enumerate(sentences):
        try:

            doc = {"titleText": title}
            print(es.index(index=indexreference, doc_type="retailer", id=idList[i], body=doc))
            #r.incrby(str(cNumber) + "_" + str(date) + "_3_progress", len(sentences))
        except elasticsearch.ConnectionError as e:
            print("Exception::", e)
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleGroupingFailed), "TitleGrouping Failed")
            sys.exit(0)
        except elasticsearch.RequestError as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleGroupingFailed), "TitleGrouping Failed")
            sys.exit(0)

        except elasticsearch.ElasticsearchException as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleGroupingFailed), "TitleGrouping Failed")
            sys.exit(0)
        except Exception as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleGroupingFailed), "TitleGrouping Failed")
            sys.exit(0)

    clusterTime = datetime.now()
    clusterID = 0
    groupID = []
    r.set(str(cNumber) + "_" + str(date) + "_3_total", len(sentences))
    for j, title in enumerate(sentences):
        groupID = list(set(groupID))

        checkList = [x for x in groupID if idList[j] in x]
        if len(checkList) > 0:
            continue
        result = es.search(
            index=indexreference,
            doc_type='retailer',
            body={
                "query": {
                    "more_like_this": {
                        "fields": ["titleText"],
                        "like_text": title,
                        "min_term_freq": 1,
                        "min_doc_freq": 1
                    }
                }
            }
        )
        # for datavalue in list(result['hits']['hits']):
            # fuzzyScore = fuzz.token_set_ratio(
            #     str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(title).lower()))).strip(),
            #     str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(datavalue['_source']['titleText'])))).strip())
            # if float(fuzzyScore) >= 85 and idList[j] != datavalue['_id']:
            #     groupID.append(datavalue['_id'])
            #     df.loc[datavalue['_id'], 'Cluster ID'] = clusterID

        for datavalue in list(result['hits']['hits']):
            fuzzyScore = fuzz.token_set_ratio(
                str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(title).lower()))).strip(),
                str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(datavalue['_source']['titleText'])))).strip())
            docNR = {}
            try:
                if float(fuzzyScore) >= 85 and idList[j] != datavalue['_id']:
                    groupID.append(datavalue['_id'])
                    df.loc[datavalue['_id'], 'Cluster ID'] = clusterID
                    for i, val in enumerate(replaceValueHeader):
                        if val == 'Retailer Item ID' or val == 'Retailer Item ID1':
                            docNR['Retailer Item ID'] = datavalue['_id']
                            continue
                        else:
                            docNR[val] = str(df.loc[(datavalue['_id'], val)]).replace('nan', '')
                    docNR['Cluster ID'] = clusterID
                    print(docNR)
                    data = es.index(index=source_indexing_index, doc_type=doc_type, id=datavalue['_id'], body=docNR)
                    print(data)
                    if not data['created']:
                        print(es.update(index=source_indexing_index, doc_type=doc_type, id=datavalue['_id'], body={"doc": docNR}))

                else:
                    for i, val in enumerate(replaceValueHeader):
                        if val == 'Retailer Item ID' or val == 'Retailer Item ID1':
                            docNR['Retailer Item ID'] = datavalue['_id']
                            continue
                        else:
                            #print(val)
                            try:
                                docNR[val] = str(df.loc[(datavalue['_id'], val)]).replace('nan', '')
                            except Exception as e:
                                print("Exception"+str(e))
                                continue
                    print(docNR)
                    data = es.index(index=source_indexing_index, doc_type=doc_type, id=datavalue['_id'], body=docNR)
                    print(data)
                    if not data['created']:
                        es.update(index=source_indexing_index, doc_type=doc_type, id=datavalue['_id'], body={"doc": docNR})
            except Exception as e:
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleGroupingFailed),"TitleGrouping Failed")
                sys.exit(0)

        df.loc[idList[j], 'Cluster ID'] = clusterID
        clusterID += 1
        r.incrby(str(cNumber) + "_" + str(date) + "_3_progress", 1)
    es.indices.delete(index=indexreference)
    # sys.exit()
    try:
        df.to_csv(outputfile, sep=',', encoding='windows-1254')
    except:
        df.to_csv(outputfile, sep=',', encoding='latin1')
    print("Start Time::", startTime, "Clustering TIme:", clusterTime, "\nEnd Time", datetime.now())

    if len(sys.argv) == 3:
        if sys.argv[2] == "start-single":
            print("Title Grouping Success")
            sys.exit(0)
        else:
            e = "Wrong Argument,need start-single as 3 rd argument"
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-4", "Title Grouping Failure")
            sys.exit(0)

    elif len(sys.argv) == 2:
        db_start_cmd = "CatalogImporter.py " + outputfile.replace('_p2.csv', '.csv') + " " + outputfile
        Start_Process(db_start_cmd)
        # db_start_cmd="DB_Create.py "+outputfile.replace('_p2.csv','.csv')+" "+outputfile
        # Start_Process(db_start_cmd)

