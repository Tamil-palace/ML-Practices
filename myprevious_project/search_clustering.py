# -*- coding: utf-8 -*-
import sys
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
run = imp.load_source('run', '/home/merit/OCR/AsinINfo/run.py')

from run import DB_update
from run import Log_writer
from run import Start_Process

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
# cNumber = re.findall(r"^([\d]+)", str(base))[0]

def word_tokenizer(text):
        # tokenizes and stems the text
        tokens = word_tokenize(text)
        
        try:
            stemmer = PorterStemmer()
            tokens = [stemmer.stem(t) for t in tokens if t not in stopwords.words('english')]
            return tokens
        except:
            return tokens
        



def needs_review_values(sourcecontent,titleIndex,trackIndex,idIndex):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, Ytitle = [], []
    for row_number in range(1, int(max_row)):
        if 'needs review' in  str(sourcecontent[row_number][trackIndex]).lower():
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())    
    return id, Ytitle


if __name__ == "__main__":
    es = Elasticsearch([{'host': '172.27.138.44', 'port': 9200}])
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
    # fileName = '/home/merit/OCR/catalog/' + str(sys.argv[1])
    fileName =  str(sys.argv[1])
    outputfile = ''
    if 'p1.csv' in fileName:
        outputfile = fileName.replace('p1.csv', 'p2.csv')
    else:
        outputfile = fileName.replace('.csv', 'p2.csv')
    head, base = os.path.split(fileName)
    
    cNumber=re.findall(r"^([\d]+)",str(base))[0]
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    df = pd.read_csv(fileName, encoding='latin1')
    replaceValueHeader = (list(df.columns.values))
    titleIndex=replaceValueHeader.index("Title")
    trackIndex=replaceValueHeader.index("Track Item")
    
    idIndex=''
    try:
        idIndex=replaceValueHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex=replaceValueHeader.index("Retailer Item ID1")
        df = df.set_index("Retailer Item ID1")
    indexreference= str(cNumber)+'_catalog'
    print ("Processing files::", fileName)
    reader = ''
    sourcecontent = []
    
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    idList, sentences = needs_review_values(sourcecontent,titleIndex,trackIndex,idIndex)
    # df.loc[id[sentences.index(sentences[sentence])], 'Cluster ID'] = cluster
    for i, title in enumerate(sentences):
        try:
            doc = {"titleText": title}
            # print(doc)
            es.index(index=indexreference, doc_type="retailer", id=idList[i], body=doc)

        except elasticsearch.ConnectionError as e:
            print ("Exception::",e)
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-5", "Title Grouping Failure")
            sys.exit(0)
        except elasticsearch.RequestError as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-5", "Title Grouping Failure")
            sys.exit(0)

        except elasticsearch.ElasticsearchException as e:
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-5", "Title Grouping Failure")
            sys.exit(0)

    clusterTime=datetime.now()
    clusterID=0
    groupID=[]
    for j, title in enumerate(sentences):
        groupID = list(set(groupID))
        
        checkList = [x for x in groupID if idList[j] in x ]
        if len(checkList) > 0 :
            continue    
        result = es.search(
            index=indexreference,
            doc_type='retailer',
            body={
            "query" : {
                       "more_like_this" : {
                                            "fields" : ["titleText"],
                                           "like_text" : title,
                                           "min_term_freq" : 1,
                                           "min_doc_freq" : 1
                                           }
                       }
                  }
         )
        
        for datavalue in list(result['hits']['hits']):
            
            fuzzyScore = fuzz.token_set_ratio(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(title).lower()))).strip(), str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(datavalue['_source']['titleText'])))).strip())
            if float(fuzzyScore) >= 85 and idList[j] != datavalue['_id']:
                groupID.append(datavalue['_id'])
                df.loc[datavalue['_id'], 'Cluster ID'] = clusterID
                
        
        df.loc[idList[j], 'Cluster ID'] = clusterID
        clusterID += 1
    es.indices.delete(index=indexreference)        
        #sys.exit()       
    try:
        df.to_csv(outputfile, sep=',', encoding='windows-1254')
    except:
        df.to_csv(outputfile, sep=',', encoding='latin1')
    print ("Start Time::", startTime,"Clustering TIme:",clusterTime , "\nEnd Time", datetime.now())
    DB_update(cNumber, "5","Title Grouping Success")
    if len(sys.argv)==4:
        if sys.argv[3] =="start-single":
            print("Title Grouping Success")
            sys.exit(0)
        else:
            e="Wrong Argument,need start-single as 3 rd argument"
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-5", "Title Grouping Failure")
            sys.exit(0)

    elif len(sys.argv) == 3:
        db_start_cmd = "DB_Create.py " + outputfile.replace('_p2.csv', '.csv') + " " + outputfile
        Start_Process(db_start_cmd)
    # db_start_cmd="DB_Create.py "+outputfile.replace('_p2.csv','.csv')+" "+outputfile
    # Start_Process(db_start_cmd)

