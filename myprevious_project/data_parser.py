# -*- coding: utf-8 -*-
import gensim
import pandas as pd
from nltk.tokenize import word_tokenize
import operator
import sys, re
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
import os
import xlrd
import csv
from collections import defaultdict
import imp
run = imp.load_source('run', '/home/merit/OCR/AsinINfo/run.py')

from run import Start_Process
from run import DB_update
from run import Log_writer
from run import CurrentPath
import run

now = datetime.now()
stoplist_old = set('for a of the and to in & - , ( ) : * \'s Etc'.split())
stoplist = 'for a of the and to in & - , ( ) : * \'s Etc'.split()


head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
# cNumber = re.findall(r"^([\d]+)", str(base))[0]
# [^!-~\s]
def validcorpus(dictionary):
    try:
        gen_docs = [[str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(word).lower()))).strip() for word in document.lower().split() if word not in stoplist] for document in dictionary]
        dictionary = gensim.corpora.Dictionary(gen_docs)
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
        tf_idf = gensim.models.TfidfModel(corpus)
        s = 0
        for i in corpus:
            s += len(i)
        sims = gensim.similarities.Similarity(run.CurrentPath()+'/Catalog_files/', tf_idf[corpus], num_features=len(dictionary))
        return sims, dictionary, tf_idf
    except Exception as e:
        print (e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-4","Title Classification Failure")


def input_values(sourcecontent,titleIndex,trackItemIndex,idIndex):
    max_row = len(sourcecontent)
    id, Ytitle, dataValue = [], [], []
    for row_number in range(1, int(max_row)):
        tempdata = []
        if 'needs review' in  str(sourcecontent[row_number][trackItemIndex]).lower():
            tempdata.append(sourcecontent[row_number][idIndex])
            tempdata.append(sourcecontent[row_number][titleIndex])
            dataValue.append(tempdata)
        elif 'need review' in  str(sourcecontent[row_number][trackItemIndex]).lower():
            tempdata.append(sourcecontent[row_number][idIndex])
            tempdata.append(sourcecontent[row_number][titleIndex])
            dataValue.append(tempdata)
        elif 'needs review' not in  str(sourcecontent[row_number][1]).lower():
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())    
    return id, Ytitle,dataValue



def keywordList(filename, sheetName):
    book = xlrd.open_workbook(str(filename))
    sheet = book.sheet_by_name(sheetName)
    row_end = int(sheet.nrows)
    pKeywordList = []
    for row in range(1, int(row_end)):
        pKeywordList.append(str(sheet.cell(row, 0).value))
    return pKeywordList

def keywordCheck(kList, text):
    emptyList = []
    for keyword in kList:
        splitkList = keyword.split('|')
        cValue = []
        for sValue in splitkList:
            if re.search("\\b" + str(re.sub("\W+", " ", str(str(sValue).strip())).lower()) + "\\b", str(re.sub("\W+", " ", text.lower()))) is not None:
                cValue.append(sValue)
        if len(cValue) == len(splitkList):
            emptyList.append(keyword)
    if len(emptyList) > 0:
        data = (max(enumerate(emptyList), key=lambda tup: len(tup[1])))
        value = []
        value.append(data[1])
        return (value)
    else:
        return emptyList
def excludebycolumns(configFile, sheetName, excludeDict):
    book = xlrd.open_workbook(str(configFile))
    sheet = book.sheet_by_name(sheetName)
    
    for col in range(0, int(sheet.ncols)):
        for row in range(1, int(sheet.nrows)):
            if str(sheet.cell(row, col).value) != "":
                excludeDict.setdefault(str(sheet.cell(0, col).value), []).append(str(sheet.cell(row, col).value))
    return excludeDict
    
if __name__ == "__main__":
    startTime = datetime.now()
    fileName =  str(sys.argv[1])
    
    outputfile = fileName.replace('_asin_output', '')
    outputfile = re.sub(r"_[\w][\d]+\.", r".", str(outputfile))
    outputfile = outputfile.replace('.csv', '_p1.csv')
    head, base = os.path.split(fileName)
    cNumber=re.findall(r"^([\d]+)",str(base))[0]
    print ("base",cNumber)
    configFile = run.CurrentPath()+'/Catalog_files/' + str(cNumber) + '-Config.xlsx'
#     configFile = 'D:\\MuthuBabu\\OCR\\' + str(sys.argv[2]) + '-Config.xlsx'
    
    excludewords = keywordList(configFile, 'Exclude')
    includewords = keywordList(configFile, 'Include')
    notTrackWords = keywordList(configFile, 'N-Nontrack')
    headers = keywordList(configFile, 'Header')
    
    
    excludeDict = defaultdict(list)
    excludeDict = excludebycolumns(configFile, 'TrackItemExclude', excludeDict)
    
    # Data Frame
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
    
    
    
    dfexcelinc = pd.read_excel(configFile, sheetname='Include')
    incexcelHeader = (list(dfexcelinc.columns.values))
    dfexcelinc = dfexcelinc.set_index(incexcelHeader[0])
    incexcelHeader.pop(0)
    
    
    df.head()
    df['Track Item (Y/Z/N)'] = ''
    df['Score'] = ''
    df['Matched ID'] = ''
    df['FuzzyScore'] = ''
    df['comments'] = ''
   
    
    f = open(fileName, 'rt', encoding="ISO-8859-1")
    
    reader = ''
    sourcecontent = []
    
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    
    id, Ytitle, inputData = input_values(sourcecontent,titleIndex,trackItemIndex,idIndex)
#     print ("id",id)
#     sys.exit()
    validSims, dictionary, tf_idf = validcorpus(Ytitle)
    for i, dataVal in enumerate(inputData):
        inputid = dataVal[0].strip()
        text = dataVal[1].strip()
        
        text1 = text.split(' + ')
        text2 = text.split(' | ')
        xpValue, tnValue, ntValue, incValue, notTrackValue = [], [], [], [], []
       
        xnValue = keywordCheck(excludewords, text)
        incValue = keywordCheck(includewords, text)
        notTrackValue = keywordCheck(notTrackWords, text)
        processedTrack=0
        for key in excludeDict:
            for xml_val_list in excludeDict[key]:
                if str(xml_val_list) == str(df[key][inputid]):
                    df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Exclude - '+str(key)
                    processedTrack=1
                    break
            if processedTrack == 1:
                break
        if processedTrack == 1:
            df.loc[inputid, 'comments'] = 'P1'        
        elif str(text) == '':
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Empty Title'
            df.loc[inputid, 'comments'] = 'P1'
        elif len(notTrackValue) > 0 :
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'N-NOT TRACKED - Keyword'
            df.loc[inputid, 'comments'] = 'P1'
        elif len(incValue) > 0 or len(xnValue) == 0 :
            query_doc = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(w).lower()))).strip() for w in word_tokenize(text) if w not in stoplist]
            query_doc_bow = dictionary.doc2bow(query_doc)
            query_doc_tf_idf = tf_idf[query_doc_bow]
            value, index = '', ''
            index, value = max(enumerate(validSims[query_doc_tf_idf]), key=operator.itemgetter(1))

            if float(value) >= 0.0:
                try:
                    fuzzyScore = fuzz.token_set_ratio(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(text).lower()))).strip(), str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(df[inputHeader[titleIndex]][id[index]])))).strip())
                    if float(fuzzyScore) >= 85:
                        if 'n-' in str(df[inputHeader[trackItemIndex]][id[index]]).lower():
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = df[inputHeader[trackItemIndex]][id[index]]
                        elif 'z-' in str(df[inputHeader[trackItemIndex]][id[index]]).lower():
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Z-EXCLUDE'
                        elif 'y' in str(df[inputHeader[trackItemIndex]][id[index]]).lower():
                            for hd in headers:
                                df.loc[inputid, hd] = df[hd][id[index]]
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Y'
                        else:
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = df[inputHeader[trackItemIndex]][id[index]]
                        
                        df.loc[inputid, 'Score'] = value
                        df.loc[inputid, 'Matched ID'] = id[index]
                        df.loc[inputid, 'FuzzyScore'] = fuzzyScore
                        
                        df.loc[inputid, 'comments'] = 'P1'
                        fuzzyScore = ''
                    elif len(incValue) > 0 :
                        for hd in incexcelHeader:
                            df.loc[inputid, hd] = dfexcelinc[hd][incValue[0]]
                        df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Include - Keyword'
                        df.loc[inputid, 'comments'] = 'P1'
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-4", "Title Classification Failure")
                    print ("exc_tb.tb_lineno", exc_tb.tb_lineno, "Error", e)
                    #input("data")

        else:
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Z-EXCLUDE- Keyword'
            df.loc[inputid, 'comments'] = 'P1'
           
        print("Processed data", i + 1, "out of", len(inputData))
    df.reset_index(inputHeader[idIndex])
    df.to_csv(outputfile, sep=',', encoding='latin1')
    
    print ("Start Time::", startTime , "\nEnd Time", datetime.now())
    DB_update(cNumber, "4","Title Classification Success")
    if len(sys.argv)==4:
        if sys.argv[3] =="start-single":
            print("Title Classification Success")
            sys.exit(0)
        else:
            e="Wrong Argument,need start-single as 3 rd argument"
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-4", "Title Classification Failure")

    elif len(sys.argv) == 3:
        search_cluster = "search_clustering.py " + outputfile
        print("******************************")
        print(search_cluster)
        print("******************************")
        Start_Process(search_cluster)

