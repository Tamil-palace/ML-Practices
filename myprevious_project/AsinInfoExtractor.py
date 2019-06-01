# -*- coding: utf-8 -*-
import requests
import re, csv
import ast
import time
import html
import sys
import ssl
import subprocess
from random import randint
from random import uniform
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
from six.moves.html_parser import HTMLParser
from random import randint
import pandas as pd
import os
import imp
import threading,queue
import ftfy,ftplib
import redis,queue
import configparser
from elasticsearch import Elasticsearch
import elasticsearch

dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir+'/AutomationController.py')

from AutomationController import Start_Process
from AutomationController import create_config
from AutomationController import DB_update
from AutomationController import Log_writer
from AutomationController import Progress_Count
from AutomationController import email_sender

config = configparser.ConfigParser()
config.read('Config.ini')


r = redis.StrictRedis()
'''
Possible Region: US, CA, CN, DE,ES, FR, JP, IT, UK,IN
'''

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
try:
   date=re.findall(r"Catalog\-([\d]+)\_",str(base))[0]
except:
   date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

doc_type=cNumber+"_"+date

AsinInfoStarted=config.get("Status", "AsinInfo-Started")
AsinInfoFailed=config.get("Status", "AsinInfo-Failed")
AsinInfoCompleted=config.get("Status", "AsinInfo-Completed")

host=config.get("Elastic-Search","host")
port=config.get("Elastic-Search","port")

es = Elasticsearch([{'host': host, 'port': port}])

print(sys.argv[1])
print(sys.argv[2])
print(sys.argv[3])

asin_args=sys.argv[4].replace("[","").replace("]","").split(",")
print(asin_args)

r.set(str(cNumber)+"_"+str(date)+"_1_progress", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_total", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_total_MissedID", 0)
r.set(str(cNumber) + "_" + str(date) + "_1_progress_MissedID", 0)

h = HTMLParser()
proxies1 = {
     "http": "http://172.27.139.135:3128",
     "https": "https://172.27.139.135:3128",
   }

config = configparser.ConfigParser()
config.read('Config.ini')

#DB
server=config.get("FTP", "server")
username=config.get("FTP", "username")
password=config.get("FTP", "password")
directory=config.get("FTP", "directory")

ftp= ftplib.FTP(server)
ftp.login(username, password)

now = datetime.now()
date_folder=now.strftime("%d_%m_%Y")
ftp_dir=directory+"/"+date_folder+"/"+cNumber
try:
    ftp.cwd(ftp_dir)
except Exception as e:
    print(e)
    try:
        ftp_dir=directory+"/"+date_folder+"/"
        ftp.cwd(ftp_dir)
    except:
        ftp.mkd(ftp_dir)
    ftp_dir=directory+"/"+date_folder+"/"+cNumber	
    ftp.mkd(ftp_dir)
    ftp.cwd(ftp_dir)

def AsinInfo(region, retailerID, missingID):
    print(str(retailerID)+"retailerID")
    retry = 0
    AsinInfoRequest = 0
    idTempList = retailerID.split(sep=',')
    retailerID = retailerID.replace("'", "")
    idListSplit = retailerID.split(sep=',')
    while True:
        error = ""
        titleRegex = '<Title>([\w\W]*?)<\/Title>'
        manufactureRegex = '<Manufacturer>([\w\W]*?)<\/Manufacturer>'
        pgroupRegex = '<ProductGroup>([\w\W]*?)<\/ProductGroup>'
        BrandRegex = '<Brand>([\w\W]*?)<\/Brand>'
        # PlatformRegex='<Platform>[\w\W]*?</Platform>'
        ModelRegex='<Model>([\w\W]*?)</Model>'
        if retry == 0:
            # url1 = "http://ocrapi.meritgroup.com/api/AsinInfo?RetailerItemId=" + str(retailerID)
            url1 = "http://ocrapi.meritgroup.com/api/AsinInfo?Locale=" + str(region).upper() + "&RetailerItemId=" + str(retailerID)
            print(url1)
            try:
                obj = requests.get(url1)
                print(obj.status_code)
                con = obj.text
                productPage = re.findall(r"\"\,\"([^\"]+)\"", str(con))[0]
                AsinInfoRequest = 1
            except:
                error = 'Exception on AsinInfo-API'
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, error, str(AsinInfoFailed),"AsinInfo Failed")
        if int(AsinInfoRequest) == 1:
            time.sleep(randint(3, 5))
            con1 = ''
            try:
                obj1 = requests.get(productPage)
                con1 = obj1.text

                if not os.path.exists(dir + "/temp/"):
                    os.mkdir(dir + "/temp/")
                if not os.path.exists(dir + "/temp/" + date_folder):
                    os.mkdir(dir + "/temp/" + date_folder)
                if not os.path.exists(dir + "/temp/" + date_folder + "/" + cNumber + "/"):
                    os.mkdir(dir + "/temp/" + date_folder + "/" + cNumber )

                with open(dir+"/temp/"+date_folder+"/"+cNumber+"/"+str(retailerID).replace(",","_")+".html", "w") as t:
                    t.write(str(str(con1).encode("utf-8")))

                itemBlock = re.findall(r"<Item>([\w\W]*?)<\/Item>", str(con1))
                tempidList, idList, titleList, manufactureList, pgroupList, BrandList,ModelList= [], [], [], [], [], [],[]
                for itemloop in itemBlock:
                    try:
                        idList.append(idTempList[idListSplit.index(str(re.findall(r"<ASIN>([\w\W]*?)</ASIN>", str(itemloop))[0]))])
                        tempidList.append(ftfy.fix_text(html.unescape(str(re.findall(r"<ASIN>([\w\W]*?)</ASIN>", str(itemloop))[0]))))
                    except:
                        idList.append('')
                        tempidList.append('')
                    try:
                        titleList.append(ftfy.fix_text(html.unescape(str(re.findall(titleRegex, str(itemloop))[0].lower()))))
                    except:
                        titleList.append('')
                    try:
                        manufactureList.append(ftfy.fix_text(html.unescape(str(re.findall(manufactureRegex, str(itemloop))[0]))))
                    except:
                        manufactureList.append('')
                    try:
                        pgroupList.append(ftfy.fix_text(html.unescape(str(re.findall(pgroupRegex, str(itemloop))[0]))))
                    except:
                        pgroupList.append('')
                    try:
                        BrandList.append(ftfy.fix_text(html.unescape(str(re.findall(BrandRegex, str(itemloop))[0]))))
                    except:
                        BrandList.append('')

                    try:
                        ModelList.append(ftfy.fix_text(html.unescape(str(re.findall(ModelRegex, str(itemloop))[0]))))
                    except:
                        ModelList.append('')
                    # try:
                    #     PlatformList.append(str(re.findall(PlatformRegex, str(itemloop))[0]))
                    # except:
                    #     PlatformList.append('')
                    
                checkID = list(set(idListSplit) - set(tempidList))
                for id in checkID:
                    missingID.append(id)

                return (idList, titleList, manufactureList, pgroupList, BrandList,ModelList, missingID)
            except Exception as e:
                print (e, sys.exc_info()[-1].tb_lineno)
                if re.search(r"not\s*a\s*valid\s*value\s*for\s*ItemId", str(con1)) is not None:
                    print ("Invalid Amazon request")
                    error = 'Invalid Amazon request'
                    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed),"AsinInfo Failed")
                elif re.search(r"Max\s*retries\s*exceeded\s*with\s*url", str(con1)) is not None:
                    print ("Proxy Error")
                    error = 'Proxy Error'
                    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed),"AsinInfo Failed")
                else:
                    print ("Amazon request failed - sleep time 10 secs")
                    time.sleep(randint(8, 10))
                    retry += 1
        if (retry == 0) or (retry > 2):
            return "", "", "", ""

def dataclean(replaceValue):
    valTemp = replaceValue.encode('utf-8')
    valTemp = re.sub(r"^b\'", "", str(valTemp))
    valTemp = re.sub(r"^b\"", "", str(valTemp))
    valTemp = re.sub(r"\'$", "", str(valTemp))
    valTemp = re.sub(r"\"$", "", str(valTemp))
    return valTemp.upper()

def dataprocess(df, retailerID, outputFile, region, missingID):
    try:
        idList, title, manufacture, pgroup, Brand, Model,missingID = AsinInfo(region, retailerID, missingID)
        # print("missingID"+str(missingID))
        for j, titlevalue in enumerate(idList):
            print ("retailerID", idList[j])
            try:
                docAsin={}
                orgTitle = df["Title"][idList[j]]
                if "Asin-Title" in asin_args:
                    df.loc[idList[j], 'Title'] = str(title[j]).replace('&AMP;','&')
                    docAsin['Title']=str(title[j]).replace('&AMP;','&')
                fuzzyScore = fuzz.token_set_ratio(str(title[j]),orgTitle)
                if float(fuzzyScore) < 70:
                    df.loc[idList[j],'TitlMatchScore'] = str(fuzzyScore)
                    docAsin['TitlMatchScore'] = str(fuzzyScore)

                if 'Platform' in df.columns:
                    # productGroup_words=dataclean(str(pgroup[j]).replace('&AMP;', '&')).split()
                    # for item in productGroup_words :
                    #     if re.match("pantry$", item, flags=re.I):
                    #         df.loc[idList[j], 'Platform']="Pantry"
                    #     else:
                    #         df.loc[idList[j], 'Platform'] = ".com"
                    productGroup=str(pgroup[j]).replace('&AMP;', '&')
                    if "pantry"==productGroup.lower():
                       df.loc[idList[j], 'Platform'] = "PANTRY"
                       docAsin['Platform'] = "PANTRY"
                    else:
                       df.loc[idList[j], 'Platform'] = "DOT COM"
                       docAsin['Platform'] = "DOT COM"

                if "Asin-M" in asin_args:
                    df.loc[idList[j], 'Manufacturer'] = str(manufacture[j]).replace('&AMP;','&')
                    docAsin['Manufacturer'] =  str(manufacture[j]).replace('&AMP;','&')

                if "Asin-B" in asin_args:
                    df.loc[idList[j], 'Brand'] = str(Brand[j]).replace('&AMP;','&')
                    docAsin['Brand'] = str(Brand[j]).replace('&AMP;','&')

                if "Asin-P" in asin_args:
                    df.loc[idList[j], 'ProductGroup'] = str(pgroup[j]).replace('&AMP;','&')
                    docAsin['ProductGroup'] =  str(pgroup[j]).replace('&AMP;','&')

                if "Asin-IDM" in asin_args:
                    df.loc[idList[j], 'Identifiers Model'] = str(Model[j]).replace('&AMP;','&')
                    docAsin['Identifiers Model'] = str(Model[j]).replace('&AMP;','&')
                # df.loc[idList[j],'Platform']=dataclean(str(pgroup[j]).replace('&AMP;','&'))
                print(docAsin)
                print(es.index(index="asininfo_cache", doc_type=doc_type, id=idList[j], body=docAsin))
            except:
                pass
            outputFile.write(str(idList[j]) + "\t" + str(title[j]) + "\t" + str(manufacture[j]) + "\t" + str(pgroup[j]) + "\t" + str(Brand[j]) + "\n")
        return df, outputFile, missingID
    
    except Exception as e:
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed), "AsinInfo Failed")
        print (e, sys.exc_info()[-1].tb_lineno)
        pass

def input_values(sourcecontent,titleIndex,trackItemIndex,idIndex,IsFulltitleCase):
    max_row = len(sourcecontent)
    print(max_row)
    # input()
    id, Ytitle, dataValue = [], [], []
    for row_number in range(1, int(max_row)):
        tempdata = []
        if (IsFulltitleCase):
            # print("Full title run case")
            # tempdata.append(sourcecontent[row_number][idIndex])
            # tempdata.append(sourcecontent[row_number][titleIndex])
            # dataValue.append(tempdata)
            dataValue.append(sourcecontent[row_number][idIndex])
        else:
            #print("Needs to review count")
            if 'needs review' in str(sourcecontent[row_number][trackItemIndex]).lower():
                # tempdata.append(sourcecontent[row_number][idIndex])
                # tempdata.append(sourcecontent[row_number][titleIndex])
                # dataValue.append(tempdata)
                dataValue.append(sourcecontent[row_number][idIndex])
            elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
                id.append(str(sourcecontent[row_number][idIndex]).strip())
                Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())
    return id, Ytitle,dataValue

# def datasplit(arr, size):
#     arrs = []
#     while len(arr) > size:
#         pice = arr[:size]
#         arrs.append(pice)
#         arr   = arr[size:]
#     arrs.append(arr)
#     return arrs
#
# def Values_Pingsdf(df,inputData,outputFile,region,out_queue,IdsPerPing=10):
#     print("IdsPerPing ............................"+str(IdsPerPing))
#     inputData_ping=[]
#     missingIDTotal=[]
#     for data_list in inputData:
#         # retailerIDInput = datasplit(data_list,5);
#         if type(data_list)==list and IdsPerPing!=1:
#             data_list=data_list
#         inputData_ping.append(data_list)
#     inputData_formed=datasplit(inputData_ping,IdsPerPing)
#     for retailerID in inputData_formed:
#         missingID=[]
#         retailerID=str(retailerID).replace("[","").replace("]","")
#         retailerID = re.sub(r"\s*", "", str(retailerID))
#         df, outputFile, missingID = dataprocess(df, retailerID, outputFile, region, missingID)
#         print(missingID)
#         out_queue.put(missingID)
#     return df ,missingIDTotal

def Values_Ping(df,inputData,outputFile,region,missingID,out_queue,single_ping_flag=False):
    r.incrby(str(cNumber) + "_" + str(date) + "_1_total", len(inputData))
    count = 0
    retailerID = ''
    lenvalue = len(inputData) - 1
    for i, dataVal in enumerate(inputData):
        singleCheck = 0
        if count == 10:
            if lenvalue == 1:
                retailerID = str(retailerID) + "," + str(dataVal)
            retailerID = re.sub(r"^\,", "", str(retailerID))
            retailerID = re.sub(r"\,$", "", str(retailerID))
            try:
                df, outputFile, missingID = dataprocess(df, retailerID, outputFile, region, missingID)
                if not single_ping_flag:
                    print("Pushing into queue.......................")
                    out_queue.put(missingID)
                count = 1
                retailerID = str(dataVal)
                lenvalue = lenvalue - 1
                singleCheck = 1
            except:
                retailerID = str(dataVal)
                lenvalue = lenvalue - 1
                pass
        else:
            retailerID = str(retailerID) + "," + str(dataVal)
            count += 1

            if i == len(inputData) - 1:
                #print ("retailerID_Else",retailerID)
                retailerID = re.sub(r"^\,", "", str(retailerID))
                retailerID = re.sub(r"\,$", "", str(retailerID))
                try:
                    df, outputFile, missingID = dataprocess(df, retailerID, outputFile, region, missingID)
                    if not single_ping_flag:
                        print("Pushing into queue.......................")
                        out_queue.put(missingID)
                    count = 1
                    retailerID = str(dataVal)
                    lenvalue = lenvalue - 1
                except:
                    retailerID = str(dataVal)
                    lenvalue = lenvalue - 1
                    pass

        if i == len(inputData) - 1 and str(retailerID) != '' and singleCheck == 1:
            retailerID = re.sub(r"^\,", "", str(retailerID))
            retailerID = re.sub(r"\,$", "", str(retailerID))
            try:
                df, outputFile, missingID = dataprocess(df, retailerID, outputFile, region, missingID)
                if not single_ping_flag:
                    print("Pushing into queue.......................")
                    out_queue.put(missingID)
            except Exception as e:
                print(e, sys.exc_info()[-1].tb_lineno)
                Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed), "AsinInfo Failed")

        # if i == len(inputData) - 1 and len(missingID) > 0:
        #     retry = 0
        #     while True:
        #         retryMissedID = []
        #         print("MissedID")
        #         print(missingID)
        #         r.incrby(str(cNumber) + "_" + str(date) + "_1_total_MissedID", len(list(set(missingID))))
        #         for mID in missingID:
        #             try:
        #                 df, outputFile, retryMissedID = dataprocess(df, mID, outputFile, region, retryMissedID)
        #             except Exception as e:
        #                 print(e, sys.exc_info()[-1].tb_lineno)
        #                 Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed),"AsinInfo Failed")
        #                 # print("Asin Info process Failed")
        #                 pass
        #             print(r.incrby(str(cNumber) + "_" + str(date) + "_1_progress_MissedID", 1))
        #         missingID = retryMissedID
        #         retry += 1
        #         if retry > 2:
        #             break
        # print(missingID)
        print(r.get(str(cNumber)+"_"+str(date)+"_1_progress"))
        print(r.incrby(str(cNumber)+"_"+str(date)+"_1_progress",1))
        print(r.get(str(cNumber)+"_"+str(date)+"_1_progress"))
        # # Progress_Count(len(inputData), i, len(missingID), cNumber)

    return missingID

def split_list(inputData):
    inputData_part1 = []
    inputData_part2 = []
    inputData_part3 = []
    inputData_part4 = []
    for i in range(0, round(len(inputData) / 4)):
        inputData_part1.append(inputData[i])

    for i in range(round(len(inputData) / 4), round(len(inputData) / 2)):
        inputData_part2.append(inputData[i])

    for i in range(round(len(inputData) / 2), round(len(inputData) / 4) * 3):
        inputData_part3.append(inputData[i])

    for i in range(round(round(len(inputData) / 4) * 3), len(inputData) - 1):
        inputData_part4.append(inputData[i])
    return inputData_part1,inputData_part2,inputData_part3,inputData_part4

def initiate_threads(inputData):

    out_queue1 = queue.Queue()
    out_queue2 = queue.Queue()
    out_queue3 = queue.Queue()
    out_queue4 = queue.Queue()

    inputData_part1, inputData_part2, inputData_part3, inputData_part4 = split_list(inputData)
    t1 = threading.Thread(target=Values_Ping, args=(df, inputData_part1, outputFile, region, missingID, out_queue1))
    t2 = threading.Thread(target=Values_Ping, args=(df, inputData_part2, outputFile, region, missingID, out_queue2))
    t3 = threading.Thread(target=Values_Ping, args=(df, inputData_part3, outputFile, region, missingID, out_queue3))
    t4 = threading.Thread(target=Values_Ping, args=(df, inputData_part4, outputFile, region, missingID, out_queue4))

    if t1.isAlive():
        print(t1.getName() + " is alive.")
    else:
        print(t1.getName() + " is dead.")
    if t2.isAlive():
        print(t2.getName() + " is alive.")
    else:
        print(t2.getName() + " is dead.")
    if t3.isAlive():
        print(t3.getName() + " is alive.")
    else:
        print(t3.getName() + " is dead.")

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    total_missedID = list(set(out_queue1.get() + out_queue2.get() + out_queue3.get() + out_queue4.get()))

    return total_missedID

if __name__ == "__main__":
    
    startTime = datetime.now()
    region = sys.argv[2]
    fileName =  str(sys.argv[1])
    IsFulltitleCase=sys.argv[3]
    print(IsFulltitleCase)
    IsFulltitleCase=ast.literal_eval(IsFulltitleCase)
    print(IsFulltitleCase)
    print ("fileName", fileName)
    df = pd.read_csv(fileName, encoding='latin1')
    inputHeader = (list(df.columns.values))
    titleIndex=inputHeader.index("Title")
    trackItemIndex=inputHeader.index("Track Item")
    idIndex=''
    try:
        idIndex=inputHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex=inputHeader.index("Retailer Item ID1")
        df = df.set_index("Retailer Item ID1")
    df.head()
    df['title_old'] = df['Title']
    df['ProductGroup'] = ''
    df['TitlMatchScore'] = ''
    outputfilecsv = fileName.replace('.csv', '_p0.csv')
    outputfile = fileName.replace('.csv', '_p0.txt')
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    
    sourcecontent = []
    
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    outputFile = open(outputfile, "a")
    outputFile.write("retailerID" + "\t" + "title" + "\t" + "manufacture" + "\t" + "pgroup" + "\t" + "Brand" + "\n")
    count = 0
    retailerID = ''
    id, Ytitle, inputData_temp = input_values(sourcecontent,titleIndex,trackItemIndex,idIndex,IsFulltitleCase)
    missingID = []
    existing_id_list = []

    url_1 = "http://" + str(host) + ":" + str(port) + "/asininfo_cache/" + str(doc_type) + "/_search"
    print(url_1)
    ping1 = requests.get(url_1).json()
    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        ping2 = requests.get(url_2).json()
        for val in ping2['hits']['hits']:
            existing_id_list.append(val['_id'])
            print("Cache data.............")
            # for exist_record in existing_id_list:
            print(val['_source'])
            if 'Title' in val['_source']:
                print(val['_source']['Title'])
                df.loc[val['_id'], 'Title'] = val['_source']['Title']
            else:
                print("else val['_source']['Title']")
                df.loc[val['_id'], 'Title'] = ""

            if 'TitlMatchScore' in val['_source']:
                print(val['_source']['TitlMatchScore'])
                df.loc[val['_id'], 'TitlMatchScore'] = val['_source']['TitlMatchScore']
            else:
                df.loc[val['_id'], 'TitlMatchScore'] = ""

            if 'Brand' in val['_source']:
                print(val['_source']['Brand'])
                df.loc[val['_id'], 'Brand'] = val['_source']['Brand']
            else:
                df.loc[val['_id'], 'Brand'] = ""

            if 'Manufacturer' in val['_source']:
                print(val['_source']['Manufacturer'])
                df.loc[val['_id'], 'Manufacturer'] = val['_source']['Manufacturer']
            else:
                print("val['_source']['Manufacturer']")
                df.loc[val['_id'], 'Manufacturer'] = ""

            if 'Platform' in val['_source']:
                print(val['_source']['Platform'])
                df.loc[val['_id'], 'Platform'] = val['_source']['Platform']
            else:
                df.loc[val['_id'], 'Platform'] = ""

            if 'ProductGroup' in val['_source']:
                df.loc[val['_id'], 'ProductGroup'] = val['_source']['ProductGroup']
            else:
                df.loc[val['_id'], 'ProductGroup'] = ""

            if 'Identifiers Model' in val['_source']:
                df.loc[val['_id'], 'Identifiers Model'] = val['_source']['Identifiers Model']
            else:
                df.loc[val['_id'], 'Identifiers Model'] = ""
            # input()
            # df.loc[idList[j], 'Title'] = val
            # df.loc[idList[j], 'TitlMatchScore'] = str(fuzzyScore)
            # df.loc[idList[j], 'Platform'] = "PANTRY"
    except Exception as e:
        print(e)
        # input()

    inputData = list(set(inputData_temp).difference(existing_id_list))
    # print(inputData)
    # sys.exit(0)

    if len(inputData)>=1000:
        print("Parallel run case")
        total_missedID=initiate_threads(inputData)
        single_ping_missingID = []
        retry=0
        while len(total_missedID)>1 and retry < 2:
            print("Retring ....................."+str(retry))
            single_ping_missingID=initiate_threads(total_missedID)
            print("single_ping_missingID")
            print(single_ping_missingID)
            retry += 1

        retryMissedID = []
        r.incrby(str(cNumber) + "_" + str(date) + "_1_total_MissedID", len(list(set(single_ping_missingID))))
        for mID in single_ping_missingID:
            df, outputFile, retryMissedID = dataprocess(df, mID, outputFile, region, retryMissedID)
            print(r.incrby(str(cNumber) + "_" + str(date) + "_1_progress_MissedID", 1))
        # input()
    else:
        print("Single Ping")
        total_missedID=Values_Ping(df,inputData,outputFile,region,missingID,"",single_ping_flag=True)
        total_missedID=set(total_missedID)
        print("Retring .................0")
        print(total_missedID)
        total_missedID = Values_Ping(df, total_missedID, outputFile, region, missingID, "", single_ping_flag=True)
        print("Retring .................1")
        print(total_missedID)
        total_missedID = set(total_missedID)
        single_ping_missingID = Values_Ping(df, total_missedID, outputFile, region, missingID, "", single_ping_flag=True)

        retryMissedID=[]
        r.incrby(str(cNumber) + "_" + str(date) + "_1_total_MissedID", len(list(set(single_ping_missingID))))
        for mID in single_ping_missingID:
            df, outputFile, retryMissedID = dataprocess(df, mID, outputFile, region, retryMissedID)
            print(r.incrby(str(cNumber) + "_" + str(date) + "_1_progress_MissedID", 1))

    outputFile.close()
    inputHeader = (list(df.columns.values))
    for item in inputHeader:
        if re.match("Unnamed\:.*?", item, flags=re.I):
            # print(re.match("(Unnamed\:.*)", item, flags=re.I)[0])
            unamedword = re.findall(r'(Unnamed\:.*)', item, flags=re.IGNORECASE)[0]
            unamedIndex = inputHeader.index(unamedword)
            df = df.drop(df.columns[[unamedIndex]], axis=1)

    try:
        df.to_csv(outputfilecsv, sep=',', encoding='latin1')
    except:
        df.to_csv(outputfilecsv, sep=',', encoding='windows-1254')
    print ("File Name" + str(fileName) + "Start Time::" + str(startTime) + "\nEnd Time" + str(datetime.now()))
    TEXT = "Hello everyone,\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Catalog " + str(cNumber) + " AsinInfo Extraction is processed successfully \n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Thanks,\n"
    TEXT = TEXT + "OCR IT Team"
    email_sender(TEXT,"AsinInfo for Catalog "+cNumber)
    try:
        if os.path.exists(dir+"/temp/"+date_folder+"/"+cNumber+"/"):
            for root, dirs, files in os.walk(dir + "/temp/" + date_folder + "/" + cNumber + "/"):
                print(files)
                for fname in files:
                    full_fname = os.path.join(root, fname)
                    ftp.storbinary('STOR ' + str(ftp_dir) + fname, open(full_fname, 'rb'))
                # os.remove(dir + "/temp/" + date_folder + "/" + cNumber + "/" + str(file))

            # for file in os.listdir(dir+"/temp/"+date_folder+"/"+cNumber+"/"):
            #     print(dir+"/temp/"+date_folder+"/"+cNumber+"/"+ str(file))
            #     try:
            #         fh = open(dir+"/temp/"+date_folder+"/"+cNumber+"/"+str(file), 'rb')
            #         ftp.storbinary('STOR '+ str(file), fh)
            #         fh.close()
            #     except Exception as e:
            #         Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed), "AsinInfo Failed")
            #         # sys.exit(0)
            #     os.remove(dir+"/temp/"+date_folder+"/"+cNumber+"/" +str(file))
        else:
            print("mentioned dir not exists")
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, "mentioned dir not exists", str(AsinInfoFailed), "AsinInfo Failed")
    except Exception as e:
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(AsinInfoFailed),"AsinInfo Failed")
    # os.rmdir(dir+"/temp/"+date_folder+"/"+cNumber+"/" )
    es.indices.delete(index='asininfo_cache')
    if len(sys.argv)==5:
        if sys.argv[4] =="start-single":
            print("Asin Info process Success")
            # sys.exit(0)
        else:
            e="Wrong Argument,need start-single as 3 rd argument"
            # print(e, sys.exc_info()[-1].tb_lineno)
            # Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-2", "Asin Info process is Failed")

    elif len(sys.argv) == 4:
        print("Asin Info process Success")
        create_config(True,sys.argv[1],cNumber)
        data_parse="ProductClassifier.py "+outputfilecsv
        Start_Process(data_parse)


