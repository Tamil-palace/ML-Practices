# -*- coding: utf-8 -*-
import gensim
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
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
from nltk.stem.porter import PorterStemmer


newStopWords = 'for a of the and to an this is into ounce count ml ounces in & - , ( ) : * \'s Etc'.split()
stopwords=list(set(stopwords.words('english')))
stopwords.extend(newStopWords)


def needsreview(sourcecontent, idIndex, track_index, title_index, comments):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, ntitle = [], []
    for row_number in range(1, int(max_row)):
        if 'needs review' in str(sourcecontent[row_number][track_index]).lower() and 'P1' not in str(
                sourcecontent[row_number][comments]):
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


def excelupdate(ncounts, zcounts, ycounts, sheet1, sheet2):
    nrSet = list(set(ncounts))
    zSet = list(set(zcounts))
    yset = list(set(ycounts))
    # print(yset)
    # print(zSet)
    # print(nrSet)
    # input("sourcecontent ")
    list4 = sorted(set(nrSet) & set(zSet), key=lambda k: nrSet.index(k))
    list5 = list(set(list4) - set(yset))
    list6 = sorted(set(nrSet) & set(yset), key=lambda k: nrSet.index(k))
    list7 = list(set(list6) - set(zSet))
    count_exclude, count_include, count_exc_noun, count_inc_noun = 1, 1, 1, 1

    for kw in list5:
        if re.search(r"[0-9]+", str(kw)) is None and re.search(r"\-ounce", str(kw)) is None and re.search(r"count", str(
                kw)) is None and re.search(r"[\d]+ml", str(kw)) is None and re.search(
                r"^(?:\W|[\d]+)\s*pack\s*(?:\W|[\d]+)$", str(kw)) is None and re.search(r"^[\d\W]+$",
                                                                                        str(kw)) is None and re.search(
                r"^[\d]+$", str(kw).strip()) is None and len(kw) > 3:
            kw = re.sub(r"\W+$", "", str(kw))
            nouns = [token for token, pos in pos_tag(word_tokenize(kw)) if pos.startswith('N')]

            if len(nouns) > 0:
                occurence_count = ncounts[str(kw).strip()]
                sheet1.write(count_exc_noun, 1, str(kw).strip())
                sheet1.write(count_exc_noun, 2, str(occurence_count))
                count_exc_noun += 1

            sheet1.write(count_exclude, 0, str(kw).strip())
            count_exclude += 1

    for kw1 in list7:
        if re.search(r"[0-9]+", str(kw1)) is None and re.search(r"\-ounce", str(kw1)) is None and re.search(r"count",
                                                                                                            str(
                                                                                                                    kw1)) is None and re.search(
                r"[\d]+ml", str(kw1)) is None and re.search(r"^(?:\W|[\d]+)\s*pack\s*(?:\W|[\d]+)$",
                                                            str(kw1)) is None and re.search(r"^[\d\W]+$", str(
                kw1)) is None and re.search(r"^[\d]+$", str(kw1).strip()) is None and len(kw1) > 3:
            kw1 = re.sub(r"\W+$", "", str(kw1))
            nouns = [token for token, pos in pos_tag(word_tokenize(kw1)) if pos.startswith('N')]
            if len(nouns) > 0:
                occurence_count = ncounts[str(kw1).strip()]
                sheet2.write(count_inc_noun, 1, str(kw1).strip())
                sheet2.write(count_inc_noun, 2, str(occurence_count))
                count_inc_noun += 1
            sheet2.write(count_include, 0, str(kw1).strip())
            count_include += 1
            
is_noun = lambda pos: pos== 'NN'

def include_excludewordcount(zset,sourcecontent):
    TotalwordCount=0
    Totalkeywords=""
    stemmed_words=[]
    for keyword in zset:
        if re.search(r"[0-9]+", str(keyword)) is None and re.search(r"\-ounce", str(keyword)) is None and re.search(
                r"count", str(keyword)) is None and re.search(r"[\d]+ml", str(keyword)) is None and re.search(
                r"^(?:\W|[\d]+)\s*pack\s*(?:\W|[\d]+)$", str(keyword)) is None and re.search(r"^[\d\W]+$", str(
            keyword)) is None and re.search(r"^[\d]+$", str(keyword).strip()) is None and len(keyword) > 3:
            if not keyword in stopwords:
                nouns = [token for token, pos in pos_tag(word_tokenize(keyword)) if is_noun(pos)]
                if len(nouns) > 0:
                    excludeCount = sourcecontent.count(keyword)
                    if excludeCount >=1:
                        # print(sourcecontent)
                        print("noun:", nouns[0])
                        TotalwordCount += excludeCount
                        Totalkeywords+=keyword+" "
                        print(Totalkeywords)
            # else:
            #     print("stemmed words",stemmed_words)
            #     porter = PorterStemmer()
            #     stemmed = [porter.stem(word) for word in nouns]
            #     stemmed_words.append(keyword)

    return (TotalwordCount,Totalkeywords,stemmed_words)


def excel_keywordcount(sourcecontent,titleindex,sheet3,sheet4,sheet5,sheet6,ycounts, zcounts):
    yset = list(set(ycounts))
    zset = list(set(zcounts))
    max_row = len(sourcecontent)

    exclude_cell_count=1
    include_cell_count=1
    ex_in_cell_count=1
    stoppedword_cellcount=1

    for row_num in range(1,max_row):
        TotalexcludeCount=0
        TotalincludeCount = 0

        TotalincludeCount, Totalincludewords,stemmed_words=include_excludewordcount(yset,sourcecontent[row_num][titleindex])
        TotalexcludeCount, Totalexcludewords,stemmed_words = include_excludewordcount(zset, sourcecontent[row_num][titleindex])

        if TotalincludeCount > TotalexcludeCount:
            sheet3.write(exclude_cell_count, 0, str(sourcecontent[row_num][titleindex]).strip())
            sheet3.write(exclude_cell_count, 1, TotalincludeCount)
            sheet3.write(exclude_cell_count, 2, Totalincludewords)
            exclude_cell_count+=1

        elif TotalincludeCount < TotalexcludeCount:
            sheet4.write(include_cell_count, 0, str(sourcecontent[row_num][titleindex]).strip())
            sheet4.write(include_cell_count, 1, TotalexcludeCount)
            sheet4.write(include_cell_count, 2, Totalexcludewords)
            include_cell_count+=1

        elif TotalincludeCount == TotalexcludeCount :
            sheet5.write(ex_in_cell_count, 0, str(sourcecontent[row_num][titleindex]).strip())
            sheet5.write(ex_in_cell_count, 1, TotalexcludeCount)
            sheet5.write(ex_in_cell_count, 2, Totalexcludewords)
            ex_in_cell_count += 1

        # if len(stemmed_words):
        #     sheet6.write(stoppedword_cellcount, 0, str(sourcecontent[row_num][titleindex]).strip())
            # sheet6.write(stoppedword_cellcount, 1, ' '.join(map(str, stemmed_words)))
            # sheet6.write(stoppedword_cellcount, 2, len(stemmed_words))
            # stoppedword_cellcount+=1

if __name__ == "__main__":
    fileList = ["224-Catalog-20180119_p1.csv"]
    for fList in fileList:
        startTime = datetime.now()
        #         fileName = 'D:\\MuthuBabu\\OCR\\' + str(sys.argv[1])
        fileName = 'D:\\OCR\\29_01_2018\\Catalog_files\\' + str(fList)
        outputfile = ''
        if 'p1.csv' in fileName:
            outputfile = fileName.replace('p1.csv', 'pkey.xls')
        else:
            outputfile = fileName.replace('.csv', 'pkey.xls')
        # outputfile = fileName.replace('.csv', '_pkey.xls')
        f = open(fileName, 'rt', encoding="ISO-8859-1")

        reader = ''
        sourcecontent = []
        df = pd.read_csv(fileName, encoding='latin1')
        incexcelHeader = (list(df.columns.values))
        idIndex = ''
        try:
            idIndex = incexcelHeader.index("Retailer Item ID")
            df = df.set_index("Retailer Item ID")
        except:
            idIndex = incexcelHeader.index("Retailer Item ID1")
            df = df.set_index("Retailer Item ID1")

        df.head()
        book = xlwt.Workbook(encoding="utf-8")
        reader = csv.reader(f, delimiter=',')
        for data in reader:
            sourcecontent.append(data)
        id, ntitle = needsreview(sourcecontent, idIndex, incexcelHeader.index("Track Item"),
                                 incexcelHeader.index("Title"), incexcelHeader.index("comments"))
        # print(ntitle)
        id, Ytitle = yValue(sourcecontent, idIndex, incexcelHeader.index("Track Item"), incexcelHeader.index("Title"))
        # print(Ytitle)
        id, ztitle = zValue(sourcecontent, idIndex, incexcelHeader.index("Track Item"), incexcelHeader.index("Title"))
        # print(ztitle)
        ycounts, ncounts, zcounts, ynouncounts, nnouncounts, znouncounts = Counter(), Counter(), Counter(), Counter(), Counter(), Counter()


        sheet1 = book.add_sheet("Exclude")
        sheet2 = book.add_sheet("Include")
        sheet3 = book.add_sheet("ExcludeByKeyword")
        sheet4 = book.add_sheet("IncludeByKeyword")
        sheet5 = book.add_sheet("Export_Include_Keyword")
        sheet6 = book.add_sheet("Stopwords")

        for sentence1 in ntitle:
            blob = TextBlob(sentence1)
            # print("blob",blob)
            # input("Stop 2")
            value = blob.noun_phrases
            # print("noun phrases",value)

            sentence_noun = '  '.join(value)
            nresultwords = [re.sub(r"^\W", "", re.sub(r"\W$", "", word.strip().lower())) for word in sentence1.split() if word.lower() not in stopwords]
            # print(nresultwords)
            ncounts.update(nresultwords)

        for sentence2 in Ytitle:
            blob = TextBlob(sentence2)
            value = blob.noun_phrases
            sentence_noun = ' '.join(value)
            yresultwords = [re.sub(r"^\W", "", re.sub(r"\W$", "", word.strip().lower())) for word in sentence2.split()
                            if word.lower() not in stopwords]
            ycounts.update(yresultwords)
            #         nresultnoun  = [re.sub(r"^\W","",re.sub(r"\W$","",word.strip().lower())) for word in sentence_noun.split() if word.lower() not in stopwords]
            #         ynouncounts.update(nresultnoun)
            #         sentence_noun,value,blob,nresultnoun='','','',''
        for sentence3 in ztitle:
            blob = TextBlob(sentence3)
            value = blob.noun_phrases
            sentence_noun = ' '.join(value)
            zresultwords = [re.sub(r"^\W", "", re.sub(r"\W$", "", word.strip().lower())) for word in sentence3.split()
                            if word.lower() not in stopwords]
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
        excel_keywordcount(sourcecontent,incexcelHeader.index("Title"), sheet3,sheet4,sheet5,sheet6, ycounts, zcounts)
        excelupdate(ncounts, zcounts, ycounts, sheet1, sheet2)
        #     excelupdate(nnouncounts,znouncounts,ynouncounts,sheet1, sheet2)
        #     excelupdate(ncounts,zcounts,ycounts,sheet1, sheet2,"noun")

        book.save(outputfile)
        print ("Start Time::", startTime, "\nEnd Time", datetime.now())

