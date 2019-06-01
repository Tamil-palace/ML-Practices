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
import configparser
import redis
import requests
from gensim.models import KeyedVectors

config = configparser.ConfigParser()
config.read('Config.ini')

dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir + '/AutomationController.py')

from AutomationController import Start_Process
from AutomationController import DB_update
from AutomationController import Log_writer
from AutomationController import CurrentPath
from AutomationController import Progress_Count

head, base = os.path.split(sys.argv[1])
cNumber = re.findall(r"^([\d]+)", str(base))[0]

try:
    date = re.findall(r"Catalog\-([\d]+)\_", str(base))[0]
except:
    date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

r = redis.StrictRedis()

doc_type = cNumber
r.set(str(cNumber) + "_" + str(date) + "_2_progress", 0)
r.set(str(cNumber) + "_" + str(date) + "_2_total", 0)

TitleClassificationStarted = config.get("Status", "Title-Classification-Started")
TitleClassificationFailed = config.get("Status", "Title-Classification-Failed")
TitleClassificationCompleted = config.get("Status", "Title-Classification-Completed")

model = KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin.gz',binary=True, limit=500000)
now = datetime.now()
stoplist_old = set('for a of the and to in & - , ( ) : * \'s Etc'.split())
stoplist = 'for a of the and to in & - , ( ) : * \'s Etc'.split()

host = config.get("Elastic-Search", "host")
port = config.get("Elastic-Search", "port")


# cNumber = re.findall(r"^([\d]+)", str(base))[0]
# [^!-~\s]
def validcorpus(dictionary):
    try:
        gen_docs = [[str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(word).lower()))).strip() for word in
                     document.lower().split() if word not in stoplist] for document in dictionary]
        # print(gen_docs)
        dictionary = gensim.corpora.Dictionary(gen_docs)
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
        tf_idf = gensim.models.TfidfModel(corpus)
        s = 0
        for i in corpus:
            s += len(i)
        sims = gensim.similarities.Similarity(dir + '/Catalog_files/', tf_idf[corpus], num_features=len(dictionary))
        return sims, dictionary, tf_idf
    except Exception as e:
        print(e)
        Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleClassificationFailed),
                   "Title Classification Failed")


def input_values(sourcecontent, titleIndex, trackItemIndex, idIndex, brandIndex, ManufactureIndex):
    try:
        max_row = len(sourcecontent)
        id, Ytitle, dataValue, BrandList, ManufactureList, OtherthanNR = [], [], [], [], [], []
        for row_number in range(1, int(max_row)):
            tempdata = []
            if 'needs review' in str(sourcecontent[row_number][trackItemIndex]).lower():
                tempdata.append(sourcecontent[row_number][idIndex])
                tempdata.append(sourcecontent[row_number][titleIndex])
                tempdata.append(sourcecontent[row_number][brandIndex])
                tempdata.append(sourcecontent[row_number][ManufactureIndex])
                dataValue.append(tempdata)
            elif 'need review' in str(sourcecontent[row_number][trackItemIndex]).lower():
                tempdata.append(sourcecontent[row_number][idIndex])
                tempdata.append(sourcecontent[row_number][titleIndex])
                tempdata.append(sourcecontent[row_number][brandIndex])
                tempdata.append(sourcecontent[row_number][ManufactureIndex])
                dataValue.append(tempdata)
            elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
                # print(sourcecontent[row_number])
                id.append(str(sourcecontent[row_number][idIndex]).strip())
                Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())
                BrandList.append(str(sourcecontent[row_number][brandIndex]).strip())
                ManufactureList.append(str(sourcecontent[row_number][ManufactureIndex]).strip())
                OtherthanNR.append(sourcecontent[row_number])
                # print(BrandList)

        return id, Ytitle, dataValue, BrandList, ManufactureList, OtherthanNR
    except Exception as e:
        print(str(e) + " : " + str(sys.exc_info()[-1].tb_lineno))
        sys.exit(0)


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
            if re.search("\\b" + str(re.sub("\W+", " ", str(str(sValue).strip())).lower()) + "\\b",
                         str(re.sub("\W+", " ", text.lower()))) is not None:
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
    fileName = str(sys.argv[1])

    outputfile = fileName.replace('_asin_output', '')
    outputfile = re.sub(r"_[\w][\d]+\.", r".", str(outputfile))
    outputfile = outputfile.replace('.csv', '_p1.csv')
    head, base = os.path.split(fileName)
    cNumber = re.findall(r"^([\d]+)", str(base))[0]
    print("base", cNumber)
    configFile = dir + '/Catalog_files/' + str(cNumber) + '-Config.xlsx'
    #     configFile = 'D:\\MuthuBabu\\OCR\\' + str(sys.argv[2]) + '-Config.xlsx'

    excludewords = keywordList(configFile, 'Exclude')
    includewords = keywordList(configFile, 'Include')
    notTrackWords = keywordList(configFile, 'N-Nontrack')
    headers = keywordList(configFile, 'Header')

    excludeDict = defaultdict(list)
    excludeDict = excludebycolumns(configFile, 'TrackItemExclude', excludeDict)

    # Data Frame
    df = pd.read_csv(fileName, encoding='latin1', error_bad_lines=False)

    inputHeader = (list(df.columns.values))
    titleIndex = inputHeader.index("Title")
    brandIndex = inputHeader.index("Brand")
    ManufactureIndex = inputHeader.index("Manufacturer")

    trackItemIndex = inputHeader.index("Track Item")
    idIndex = ''
    try:
        idIndex = inputHeader.index("Retailer Item ID")
        df = df.set_index("Retailer Item ID")
    except:
        idIndex = inputHeader.index("Retailer Item ID1")

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
    df['Brand Flag'] = ''

    f = open(fileName, 'rt', encoding="ISO-8859-1")

    reader = ''
    sourcecontent = []
    temp_dic = {}
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)
    existing_source = []
    url_1 = "http://" + str(host) + ":" + str(port) + "/source_indexing/" + str(doc_type) + "/_search"
    print(url_1)
    ping1 = requests.get(url_1).json()
    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        ping2 = requests.get(url_2).json()
        headers_1 = list(ping2['hits']['hits'][0]['_source'].keys())
        idIndex_exist = headers_1.index('Retailer Item ID')
        brandIndex_exist = headers_1.index('Brand')
        ManufactureIndex_exist = headers_1.index('Manufacturer')
        titleIndex_exist = headers_1.index('Title')
        trackItemIndex_exist = headers_1.index('Track Item')

        for val in ping2['hits']['hits']:
            if "needs review" not in val['_source']['Track Item'].lower():
                source_temp = []
                for key in list(val['_source'].keys()):
                    source_temp.append(val['_source'][key])
                existing_source.append(source_temp)
        id_exist, Ytitle_exist, inputData_exist, BrandList_exist, ManufactureList_exist, Source_total_exist = input_values(
            existing_source, titleIndex_exist, trackItemIndex_exist, idIndex_exist, brandIndex_exist,
            ManufactureIndex_exist)

        for i, id in enumerate(id_exist):
            temp_exist = []
            temp_exist.append(Ytitle_exist[i])
            temp_exist.append(BrandList_exist[i])
            temp_exist.append(ManufactureList_exist[i])
            temp_exist.append(Source_total_exist[i])
            temp_dic[id] = temp_exist
    except Exception as e:
        print(str(e) + "  :  " + str(sys.exc_info()[-1].tb_lineno))
        pass

    print("Before" + str(len(temp_dic)))
    id, Ytitle, inputData, BrandList, ManufactureList, Source_total = input_values(sourcecontent, titleIndex,
                                                                                   trackItemIndex, idIndex, brandIndex,
                                                                                   ManufactureIndex)
    for i, single_id in enumerate(id):
        temp_exist = []
        temp_exist.append(Ytitle[i])
        temp_exist.append(BrandList[i])
        temp_exist.append(ManufactureList[i])
        temp_exist.append(Source_total[i])
        temp_dic[single_id] = temp_exist
    print("after" + str(len(temp_dic)))

    id_updated, Ytitle_updated, BrandList_updated, ManufactureList_updated, SourceList_updated = [], [], [], [], []
    for key, val in temp_dic.items():
        Ytitle_updated.append(val[0])
        BrandList_updated.append(val[1])
        ManufactureList_updated.append(val[2])
        SourceList_updated.append(val[3])
        id_updated.append(key)

    validSims, dictionary, tf_idf = validcorpus(Ytitle_updated)
    validSims_brand, brand_dictionary, brand_tf_idf = validcorpus(BrandList_updated)
    validSims_manufacture, manufacture_dictionary, manufacture_tf_idf = validcorpus(ManufactureList_updated)

    for i, dataVal in enumerate(inputData):
        inputid = dataVal[0].strip()
        text = dataVal[1].strip()
        brand = dataVal[2].strip()
        manufacture = dataVal[3].strip()

        text1 = text.split(' + ')
        text2 = text.split(' | ')
        xpValue, tnValue, ntValue, incValue, notTrackValue = [], [], [], [], []

        xnValue = keywordCheck(excludewords, text)
        incValue = keywordCheck(includewords, text)
        notTrackValue = keywordCheck(notTrackWords, text)
        processedTrack = 0
        for key in excludeDict:
            for xml_val_list in excludeDict[key]:
                if str(xml_val_list) == str(df[key][inputid]):
                    df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Exclude - ' + str(key)
                    processedTrack = 1
                    break
            if processedTrack == 1:
                break
        if processedTrack == 1:
            df.loc[inputid, 'comments'] = 'P1'
        elif str(text) == '':
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Empty Title'
            df.loc[inputid, 'comments'] = 'P1'
        elif len(notTrackValue) > 0:
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'N-NOT TRACKED - Keyword'
            df.loc[inputid, 'comments'] = 'P1'

        elif len(incValue) > 0 or len(xnValue) == 0:
            # title
            query_doc = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(w).lower()))).strip() for w in
                         word_tokenize(text) if w not in stoplist]
            query_doc_bow = dictionary.doc2bow(query_doc)
            query_doc_tf_idf = tf_idf[query_doc_bow]
            value, index = '', ''
            index, value = max(enumerate(validSims[query_doc_tf_idf]), key=operator.itemgetter(1))

            # brand
            query_doc_brand = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(w).lower()))).strip() for w in
                               word_tokenize(brand) if w not in stoplist]
            query_doc_bow_brand = brand_dictionary.doc2bow(query_doc_brand)
            query_doc_tf_idf_brand = brand_tf_idf[query_doc_bow_brand]
            value_brand, brand_index = '', ''
            brand_index, value_brand = max(enumerate(validSims_brand[query_doc_tf_idf_brand]),key=operator.itemgetter(1))

            # manufacture
            query_doc_manufacture = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(w).lower()))).strip() for w in
                                     word_tokenize(manufacture) if w not in stoplist]
            query_doc_bow_manufacture = manufacture_dictionary.doc2bow(query_doc_manufacture)
            query_doc_tf_idf_manufacture = manufacture_tf_idf[query_doc_bow_manufacture]
            value_manufacture, manufacture_index = '', ''
            manufacture_index, value_manufacture = max(enumerate(validSims_manufacture[query_doc_tf_idf_manufacture]),key=operator.itemgetter(1))

            if float(value) >= 0.0:
                try:
                    fuzzyScore = fuzz.token_set_ratio(
                        str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(text).lower()))).strip(), str(
                            re.sub(r"[^!-~\s]", "",
                                   re.sub(r"\W+", " ", str(SourceList_updated[index][titleIndex])))).strip())
                    brand_fuzzyScore = fuzz.token_set_ratio(
                        str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(brand).lower()))).strip(), str(
                            re.sub(r"[^!-~\s]", "",
                                   re.sub(r"\W+", " ", str(SourceList_updated[brand_index][brandIndex])))).strip())
                    manufacture_fuzzyScore = fuzz.token_set_ratio(
                        str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(manufacture).lower()))).strip(), str(
                            re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(
                                SourceList_updated[manufacture_index][ManufactureIndex])))).strip())

                    s1 = str(SourceList_updated[index][titleIndex]).split()
                    s2 = str(text).split()
                    distance = model.wmdistance(s1, s2)
                    print(distance)

                    if float(distance) <= 3:
                        if 'n-' in str(SourceList_updated[index][trackItemIndex]).lower():
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = SourceList_updated[index][trackItemIndex]
                        elif 'z-' in str(SourceList_updated[index][trackItemIndex]).lower():
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Z-EXCLUDE'
                        elif 'y' in str(SourceList_updated[index][trackItemIndex]).lower():
                            for hd in headers:
                                df.loc[inputid, hd] = SourceList_updated[index][inputHeader.index(hd)]
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Y'
                        else:
                            df.loc[inputid, 'Track Item (Y/Z/N)'] = SourceList_updated[index][trackItemIndex]

                        df.loc[inputid, 'Score'] = value
                        df.loc[inputid, 'Matched ID'] = id_updated[index]
                        df.loc[inputid, 'FuzzyScore'] = fuzzyScore

                        if manufacture_fuzzyScore >= 95 and brand_fuzzyScore >= 95:
                            df.loc[inputid, 'Brand'] = str(SourceList_updated[index][brandIndex])
                            df.loc[inputid, 'Brand Flag'] = 'Brand Flag Arised'

                        df.loc[inputid, 'comments'] = 'P1'
                        fuzzyScore = ''
                    elif len(incValue) > 0:
                        for hd in incexcelHeader:
                            df.loc[inputid, hd] = dfexcelinc[hd][incValue[0]]
                        df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Include - Keyword'
                        df.loc[inputid, 'comments'] = 'P1'
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleClassificationFailed),
                               "Title Classification Failed")
                    print("exc_tb.tb_lineno", exc_tb.tb_lineno, "Error", e)

        else:
            df.loc[inputid, 'Track Item (Y/Z/N)'] = 'Z-EXCLUDE- Keyword'
            df.loc[inputid, 'comments'] = 'P1'

        print("Processed data", i + 1, "out of", len(inputData))
        r.incrby(str(cNumber) + "_" + str(date) + "_2_progress", 1)
        # Progress_Count(len(inputData), i, 0, cNumber)
    df.reset_index(inputHeader[idIndex])

    inputHeader = (list(df.columns.values))
    for item in inputHeader:
        # print(item)
        if re.match("Unnamed\:.*?", item, flags=re.I):
            # print(re.match("(Unnamed\:.*)", item, flags=re.I)[0])
            unamedword = re.findall(r'(Unnamed\:.*)', item, flags=re.IGNORECASE)[0]
            unamedIndex = inputHeader.index(unamedword)
            df = df.drop(df.columns[[unamedIndex]], axis=1)

    df.to_csv(outputfile, sep=',', encoding='latin1')

    print("Start Time::", startTime, "\nEnd Time", datetime.now())
    print(len(sys.argv))
    if len(sys.argv) == 3:
        if sys.argv[2] == "start-single":
            sys.exit(0)
        else:
            e = "Wrong Argument,need start-single as 3 rd argument"
            Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, str(TitleClassificationFailed),
                       "Title Classification Failed")

    elif len(sys.argv) == 2:
        search_cluster = "ProductGrouping.py " + outputfile
        Start_Process(search_cluster)

