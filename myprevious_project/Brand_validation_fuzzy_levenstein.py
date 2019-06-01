#!/usr/bin/env python
# -*- coding: utf-8 -*-
import distance
import re,sys
import operator
import pandas as pd
import csv
import gensim
from fuzzywuzzy import fuzz
from nltk.tokenize import word_tokenize
import xlwt
from pyjarowinkler import distance as dis
import jellyfish
import math
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

stop_words = set(stopwords.words('english'))

def preprocess(text):
    tokenizer = RegexpTokenizer(r'\w+')
    print(text)
    text1 = text.lower()
    text1 = tokenizer.tokenize(text1)
    text = " ".join(text1)
    # word_tokens = word_tokenize(text)
    filtered_sentence = [w for w in text.split() if not w in stop_words]
    text1 = " ".join(filtered_sentence)
    return text1

try:
    filepath="Brand Automation-Walmart.csv"
    df = pd.read_csv(filepath, encoding='latin1', error_bad_lines=False)
    coloumns= (list(df.columns.values))
    print(coloumns)
    # input()
    print(coloumns.index("name"))
    print(coloumns.index("status"))

    f = open("Brand Automation-Walmart.csv", 'rt', encoding="ISO-8859-1")
    reader =  list(csv.reader(f, delimiter=','))
    print(type(reader))
    f1 = open("Brand Automation-Walmart_needs.csv", 'rt', encoding="ISO-8859-1")
    reader1 = list(csv.reader(f1, delimiter=','))
    print(type(reader1))
    data_list=[]
    source=[]

    for index,val in enumerate(list(reader)):
        if index==0:
            continue
        temp=[]
        temp.append(str(val[coloumns.index("name")])+" "+str(val[coloumns.index("synonyms")]))
        temp.append(val[coloumns.index("status")])
        temp.append(val[coloumns.index("id")])
        source.append(temp)
    print(len(source))
    for index, val in enumerate(list(reader1)):
        if index==0:
            continue
        temp1 = []
        temp1.append(val[coloumns.index("name")]+" "+str(val[coloumns.index("synonyms")]))
        temp1.append(val[coloumns.index("status")])
        temp1.append(val[coloumns.index("id")])
        data_list.append(temp1)
    print(len(data_list))
    # input()
    suffix_match=""
    prefix_match=""
    # validSims, dictionary, tf_idf, lsi=validcorpus(source)
    with open("brand_validataion_fuzzy_1.txt", "a", encoding="utf-8") as fh:
        fh.write("index\t" + "Product Brand" + "\t" + "Status" + "\t" + "ID" + "\t" + "Matching Brand" + "\t" + "Corpus Status" + "\t" "Index"+ "\t" + "Levenstein" + "\t"  + "Levenstein 1" + "\t" + "Jaro Winkler" + "\t" + "FuzzyScore" + "\t" + "Jelly Levenstein" + "\t" + "Damerau Levenstein" + "\t" + "Jaccard Score" + "\t" + "Soresen" + "\t" + "Processed Text"  + "\t" + "Processed Corpus Text" +"\t" + "suffix match"  + "\t" + "prefix match" + "\n")
    try:
        for index, data in enumerate(data_list):
            text = data[0].strip()
            print(text)
            # input()
            status = data[1].strip()
            inputid = data[2].strip()
            try:
                for index1, data_corpus in enumerate(source):
                    print(str(index)+" -> "+str(index1))
                    corpus_text = data_corpus[0].strip()
                    corpus_status = data_corpus[1].strip()
                    corpus_id = data_corpus[2].strip()
                    try:
                        if text == "" or corpus_text == "":
                            continue
                        processed_text=preprocess(text)
                        processed_corpus_text = preprocess(corpus_text)
                        # lev = distance.nlevenshtein(text,corpus_text, method=2)
                        lev = distance.nlevenshtein(processed_text, processed_corpus_text, method=1)
                        lev1 = distance.nlevenshtein(processed_text, processed_corpus_text, method=2)
                        lev_jelly = jellyfish.levenshtein_distance(processed_text, processed_corpus_text)
                        dam_lev = jellyfish.damerau_levenshtein_distance(processed_text, processed_corpus_text)
                        jaccard = distance.jaccard(processed_text, processed_corpus_text)
                        soresen = distance.sorensen(processed_text, processed_corpus_text)
                        fuzzy = fuzz.ratio(processed_text, processed_corpus_text)
                        print(str(processed_text)+" *******"+str(processed_corpus_text))
                        jaro = dis.get_jaro_distance(processed_text, processed_corpus_text, winkler=True, scaling=0.1)
                        Ngram = ngram.NGram.compare(text, corpus_text, N=1) * 100

                        word_div=len(processed_text.split()) / 2
                        if (float(word_div) % 1) >= 0.5:
                            x = math.ceil(word_div)
                            print(x)
                        else:
                            x=round(word_div)
                            print(x)
                        # if x!='':
                        #     print(x)
                        # if re.findall(".5", str(word_div), re.I):
                            # input()
                        x=x-1
                        flag=True
                        print(processed_text.split()[x:len(processed_text.split())])
                        if len(processed_text.split()[x:len(processed_text.split())])==1:
                            flag=False
                        if flag:
                            count=0
                            for val in processed_text.split()[x:len(processed_text.split())]:
                                for val_inner in processed_corpus_text.split()[x:len(processed_corpus_text.split())]:
                                    if val_inner.strip()==val.strip():
                                        count=count+1
                                        print(count)
                            suffix_match=count/len(processed_text.split()[x:len(processed_text.split())])
                            count1 = 0
                            # print(re.findall(".5", str(word_div), re.I)[0])
                            # if re.findall(".5", str(word_div), re.I):
                                # input(word_div)
                            x = x + 1
                            for val in processed_text.split()[0:x]:
                                print(val)
                                for val_inner in processed_corpus_text.split()[0:x]:
                                    if val_inner.strip() == val.strip():
                                        count1 = count1 + 1
                                        print(count1)
                            print(len(processed_text.split()[0:x]))
                            prefix_match = count1 / len(processed_text.split()[0:x])
                            print(prefix_match)
                            # input()
                        # hamming = distance.hamming(external=False)
                        # hamming=hamming(text, corpus_text, normalized=True)
                        # dam_lev=jellyfish.damerau_levenshtein_distance(text, corpus_text)
                        # soresen=distance.sorensen(text, corpus_text)
                        # jaccard=distance.jaccard(text, corpus_text)
                        # lev_jelly=jellyfish.levenshtein_distance(text, corpus_text)
                        # if fuzzy > 60:
                        with open("brand_validataion_fuzzy_1.txt","a",encoding="utf-8") as fh:
                            # fh.write(str(index)+"\t"+str(text)+"\t"+str(status)+"\t"+str(inputid)+"\t"+str(index1)+"\t"+str(corpus_text)+"\t"+str(corpus_status)+"\t"+str(corpus_id)+"\t"+str(lev)+"\t"+str(jaro)+"\t"+str(fuzzy)+"\t"+str(soresen)+"\t"+str(jaccard)+"\t"+str(dam_lev)+"\t"+str(lev_jelly)+"\n")
                            fh.write(
                                str(index) + "\t" + str(text) + "\t" + str(status) + "\t" + str(
                                    inputid) + "\t"  + str(corpus_text) + "\t" + str(corpus_status) + "\t" + str(
                                    corpus_id) + "\t" + str(lev) + "\t"+ str(lev1) + "\t" + str(jaro) + "\t" + str(
                                    fuzzy) + "\t" + str(lev_jelly) + "\t" + str(dam_lev) + "\t" + str(
                                    jaccard) + "\t" + str(soresen) + "\t" + str(processed_text) + "\t" + str(processed_corpus_text) +"\t"+str(suffix_match)+ "\t"+str(prefix_match)+ "\n")
                    except Exception as e:
                            print(e)
                            print(sys.exc_info()[-1].tb_lineno)
                            pass
            except Exception as e:
                print(sys.exc_info()[-1].tb_lineno)
                print(e)
                pass
    except Exception as e:
        print(sys.exc_info()[-1].tb_lineno)
        print(e)
        pass

except Exception as e:
    print(e)
    print(sys.exc_info()[-1].tb_lineno)
    input()
# df.reset_index(coloumns[0],drop=True)
# try:
#     df.to_csv("new.csv", sep=',', encoding ='latin1')
# except Exception as e:
#     df.to_csv("new.csv", sep=',', encoding ='utf-8')
