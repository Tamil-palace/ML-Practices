# -*- coding: utf-8 -*-
import gensim
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk import pos_tag
import operator
import sys, re
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date
import os
import xlrd
import csv
import xlwt
from collections import Counter
from textblob import TextBlob
import configparser

import imp
dir = os.path.dirname(__file__)
AutomationController = imp.load_source('AutomationController', dir+'/AutomationController.py')

from AutomationController import Log_writer

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]

config = configparser.ConfigParser()
config.read('Config.ini')

KeywordExtractionStarted=config.get("Status", "Keyword-Extraction-Started")
KeywordExtractionFailed=config.get("Status", "Keyword-Extraction-Failed")
KeywordExtractionCompleted=config.get("Status", "Keyword-Extraction-Completed")


stopwords = 'for a of the and to an this is into ounce count ml ounces in & - , ( ) : * \'s Etc'.split()
def needsreview(sourcecontent,idIndex , track_index, title_index, comments):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, ntitle = [], []
    for row_number in range(1, int(max_row)):
        if 'needs review' in  str(sourcecontent[row_number][track_index]).lower() and 'P1' not in str(sourcecontent[row_number][comments]):
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            ntitle.append(str(sourcecontent[row_number][title_index]).strip())    
    return id, ntitle

def yValue(sourcecontent, idIndex, track_index, title_index):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, Ytitle = [], []
    for row_number in range(1, int(max_row)):
        if 'y' == str(sourcecontent[row_number][track_index]).lower():
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            Ytitle.append(str(sourcecontent[row_number][title_index]).strip())  
        elif re.search(r"^y", str(sourcecontent[row_number][track_index]).lower()) is not None:
            id.append(str(sourcecontent[row_number][3]).strip())
            Ytitle.append(str(sourcecontent[row_number][title_index]).strip())    
    return id, Ytitle
def zValue(sourcecontent, idIndex, track_index, title_index):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, ztitle = [], []
    for row_number in range(1, int(max_row)):
        if re.search(r"^z", str(sourcecontent[row_number][track_index]).lower()) is not None:
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            ztitle.append(str(sourcecontent[row_number][title_index]).strip())    
    return id, ztitle

def excelupdate(ncounts,zcounts,ycounts,sheet1,sheet2):
    nrSet = list(set(ncounts))
    zSet = list(set(zcounts))
    yset = list(set(ycounts))
    list4 = sorted(set(nrSet) & set(zSet), key = lambda k : nrSet.index(k))
    list5 =list(set(list4) - set(yset))
    list6 = sorted(set(nrSet) & set(yset), key = lambda k : nrSet.index(k))
    list7 =list(set(list6) - set(zSet))
    count_exclude, count_include,count_exc_noun, count_inc_noun=1,1,1,1
    
    for kw in list5:
        if re.search(r"[0-9]+", str(kw)) is None and re.search(r"\-ounce", str(kw)) is None and re.search(r"count", str(kw)) is None and re.search(r"[\d]+ml", str(kw)) is None and re.search(r"^(?:\W|[\d]+)\s*pack\s*(?:\W|[\d]+)$", str(kw)) is None and re.search(r"^[\d\W]+$", str(kw)) is None and re.search(r"^[\d]+$", str(kw).strip()) is None and len(kw) > 3 :
            kw = re.sub(r"\W+$", "",str(kw))
            nouns = [token for token, pos in pos_tag(word_tokenize(kw)) if pos.startswith('N')]
            
            if len(nouns) > 0:
                occurence_count = ncounts[str(kw).strip()]
                sheet1.write(count_exc_noun, 1, str(kw).strip())
                sheet1.write(count_exc_noun, 2, str(occurence_count))
                count_exc_noun +=1
            
            sheet1.write(count_exclude, 0, str(kw).strip())
            count_exclude +=1
            
    for kw1 in list7:
        if re.search(r"[0-9]+", str(kw1)) is None and re.search(r"\-ounce", str(kw1)) is None and re.search(r"count", str(kw1)) is None and re.search(r"[\d]+ml", str(kw1)) is None and re.search(r"^(?:\W|[\d]+)\s*pack\s*(?:\W|[\d]+)$", str(kw1)) is None and re.search(r"^[\d\W]+$", str(kw1)) is None and re.search(r"^[\d]+$", str(kw1).strip()) is None and len(kw1) > 3 :
            kw1 = re.sub(r"\W+$", "",str(kw1))
            nouns = [token for token, pos in pos_tag(word_tokenize(kw1)) if pos.startswith('N')]
            if len(nouns) > 0:
                occurence_count = ncounts[str(kw1).strip()]
                sheet2.write(count_inc_noun, 1, str(kw1).strip())
                sheet2.write(count_inc_noun, 2, str(occurence_count))
                count_inc_noun += 1
            sheet2.write(count_include, 0, str(kw1).strip())
            count_include +=1
    

if __name__ == "__main__":
    
    # fileList = ["71-Catalog-20180206_p1.csv"]
# for fList in fileList:
    startTime = datetime.now()
#         fileName = 'D:\\MuthuBabu\\OCR\\' + str(sys.argv[1])
    fileName =  str(sys.argv[1])
    outputfile = ''
    if 'p1.csv' in fileName:
        outputfile = fileName.replace('p1.csv', 'pkey.xls')
    else:
        outputfile = fileName.replace('.csv', 'pkey.xls')
    #outputfile = fileName.replace('.csv', '_pkey.xls')
    f = open(fileName, 'rt', encoding="ISO-8859-1")

    reader = ''
    sourcecontent = []
    df = pd.read_csv(fileName, encoding='latin1')
    incexcelHeader = (list(df.columns.values))
    idIndex=''
    try:
        idIndex=incexcelHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex=incexcelHeader.index("Retailer Item ID1")
        df = df.set_index("Retailer Item ID1")

    df.head()
    book = xlwt.Workbook(encoding="utf-8")
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    id, ntitle = needsreview(sourcecontent,idIndex,incexcelHeader.index("Track Item"),incexcelHeader.index("Title"),incexcelHeader.index("comments"))
    id, Ytitle = yValue(sourcecontent,idIndex,incexcelHeader.index("Track Item"),incexcelHeader.index("Title"))
    id, ztitle = zValue(sourcecontent,idIndex,incexcelHeader.index("Track Item"),incexcelHeader.index("Title"))
    ycounts,ncounts,zcounts,ynouncounts,nnouncounts,znouncounts = Counter(),Counter(),Counter(), Counter(),Counter(),Counter()

    sheet1 = book.add_sheet("Exclude")
    sheet2 = book.add_sheet("Include")
    for sentence1 in ntitle:
        blob = TextBlob(sentence1)
        # print(blob)
        # input("Stop 2")
        value = blob.noun_phrases
        sentence_noun = ' '.join(value)
        nresultwords  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence1.split() if word.lower() not in stopwords]
        # print(nresultwords)
        ncounts.update(nresultwords)
        # print(ncounts)
        # input("Stop 1")
#         nresultnoun  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence_noun.split() if word.lower() not in stopwords]
#         nnouncounts.update(nresultnoun)
#         sentence_noun,value,blob,nresultnoun='','','',''
    for sentence2 in Ytitle:
        blob = TextBlob(sentence2)
        value = blob.noun_phrases
        sentence_noun = ' '.join(value)
        yresultwords  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence2.split() if word.lower() not in stopwords]
        ycounts.update(yresultwords)

#         nresultnoun  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence_noun.split() if word.lower() not in stopwords]
#         ynouncounts.update(nresultnoun)
#         sentence_noun,value,blob,nresultnoun='','','',''
    for sentence3 in ztitle:
        blob = TextBlob(sentence3)
        value = blob.noun_phrases
        sentence_noun = ' '.join(value)
        zresultwords  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence3.split() if word.lower() not in stopwords]
        zcounts.update(zresultwords)

#         nresultnoun  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence_noun.split() if word.lower() not in stopwords]
#         znouncounts.update(nresultnoun)
#         sentence_noun,value,blob,nresultnoun='','','',''
    sheet1.write(0, 0, "Exclude_keywords")
    sheet2.write(0, 0, "Include_keywords")

    sheet1.write(0, 1, "Exclude_keywords_Noun")
    sheet2.write(0, 1, "Include_keywords_Noun")

    sheet1.write(0, 2, "Exclude_keywords_Noun occurrence")
    sheet2.write(0, 2, "Include_keywords_Noun occurrence")
    excelupdate(ncounts,zcounts,ycounts,sheet1, sheet2)
#     excelupdate(nnouncounts,znouncounts,ynouncounts,sheet1, sheet2)
#     excelupdate(ncounts,zcounts,ycounts,sheet1, sheet2,"noun")

    book.save(outputfile)
    Log_writer("OCR_generic_error.log", cNumber, "KeywordExtraction Completed", str(KeywordExtractionCompleted),"KeywordExtraction Completed")
    print ("Start Time::", startTime , "\nEnd Time", datetime.now())


