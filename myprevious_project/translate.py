#-*- coding: utf-8 -*-
import gensim
import pandas as pd
from nltk.tokenize import word_tokenize
import operator
import sys,re
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
import os
import xlrd
import requests
import csv
import urllib
# import amazon_breadCrumbs
import unicodedata
now = datetime.now()

import imp
run = imp.load_source('run', '/home/merit/OCR/AsinINfo/run.py')
import run

if __name__ == "__main__":
    
    # fileList=["350-Catalog-20180207_p0.csv"]
    # print(fileList[0])
    # regionList=['FR']

# for i, fl in enumerate(fileList):

    # path='D:\\OCR\\09_02_2018\\Catalog_files\\'
    fl=sys.argv[1]
    startTime = datetime.now()

    fileName =str(fl)
    region=sys.argv[2]

    outputfile = fileName.replace('_p0.csv', '_pt.csv')

    dfexcel = pd.read_csv(fileName, encoding='latin1')
    inputHeader = (list(dfexcel.columns.values))
    excelValueHeader = (list(dfexcel.columns.values))

    try:
        idIndex=inputHeader.index("Retailer Item ID")
        dfexcel = dfexcel.set_index("Retailer Item ID")
    except:
        idIndex=inputHeader.index("Retailer Item ID1")
        dfexcel = dfexcel.set_index("Retailer Item ID1")
    dfexcel.head()
    dfexcel['Translated Title']=''
    logFile = open(run.CurrentPath()+str(region)+".txt","w")
    for index, row in dfexcel.iterrows():
        title = dfexcel["Title"][index]
        trackItem = dfexcel["Track Item"][index]

        if re.match(r"^needs\s*review", trackItem, re.IGNORECASE) is not None:
#         if True:
            print ("title",title)
            try:
                tempTitle=title.replace("#"," ")
                tempTitle = urllib.parse.quote(tempTitle.encode('utf-8'), ':/')
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logFile.write(str(index)+"\t"+str(e)+"\t"+str(exc_tb.tb_lineno)+"\n")
                continue
            print('https://www.googleapis.com/language/translate/v2?key=AIzaSyCazj_28EuOnxft2tdK0n1sSewrloBPStA&q='+str(tempTitle)+'&source='+str(region)+'&target=en')
            translateResponse= requests.get('https://www.googleapis.com/language/translate/v2?key=AIzaSyCazj_28EuOnxft2tdK0n1sSewrloBPStA&q='+str(tempTitle)+'&source='+str(region)+'&target=en')
            try:
                val = re.findall(r'\"translatedText\"\:\s*\"([\w\W]*?)\"\s*\}', str(translateResponse.text))
                replaceValue=val[0]
#                 replaceValue=title
                valTemp=replaceValue.encode('utf-8')
                valTemp=re.sub(r"^b\'", "", str(valTemp))
                valTemp=re.sub(r"\'$", "", str(valTemp))
                dfexcel.loc[index,'Translated Title'] = valTemp
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logFile.write(str(index)+"\t"+str(e)+"\t"+str(exc_tb.tb_lineno)+"\n")
#                 input("e")

    #dfexcel.reset_index(excelValueHeader[3])
    try:
        dfexcel.to_csv(outputfile, sep=',', encoding ='windows-1254')
    except:
        dfexcel.to_csv(outputfile, sep=',', encoding ='latin1')
    else:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print ("Error on file writing",exc_tb.tb_lineno)
    print ("Start Time::",startTime ,"\nEnd Time",datetime.now())

