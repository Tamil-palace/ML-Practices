#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re,sys
import operator
import pandas as pd
import csv
import gensim
from fuzzywuzzy import fuzz
from nltk.tokenize import word_tokenize
import xlwt


# book=xlwt.Workbook("gensim_needsreviewkeywords.xls")
# sheet1=book.add_sheet("sheet1")
# sheet1.write(0,0,"")


stoplist = 'for a of the and to in & - , ( ) : * \'s Etc'.split()

def validcorpus(dictionary):
    try:
        # dictionary=[val[0] for val in dictionary]
        gen_docs = [[str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", re.sub(r"¶"," ",str(word).lower())))).strip() for word in document[0].lower().replace("¶"," ").split() if word not in stoplist] for document in dictionary]
        # input(gen_docs)
        dictionary = gensim.corpora.Dictionary(gen_docs)
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
        tf_idf = gensim.models.TfidfModel(corpus)
        s = 0
        for i in corpus:
            s += len(i)
        lsi = gensim.models.LsiModel(tf_idf[corpus], id2word=dictionary, num_topics=300)
        print(lsi.print_topics(300))
        # transformed_lsi =lsi[tf_idf[corpus]]
        sims = gensim.similarities.Similarity('./', lsi[tf_idf[corpus]],num_features=len(dictionary))
        return sims, dictionary, tf_idf,lsi
    except Exception as e:
        print (e)
        print(sys.exc_info()[-1].tb_lineno)

try:
    filepath="Products_skus_new.csv"
    df = pd.read_csv(filepath, encoding='latin1', error_bad_lines=False)
    coloumns= (list(df.columns.values))
    print(coloumns)
    input()
    print(coloumns.index("name"))
    print(coloumns.index("P.name"))
    df.head()
    df['Score'] = ''
    try:
        df = df.set_index("product_gold_key")
    except:
        pass

    f = open("Products_skus_new.csv", 'rt', encoding="ISO-8859-1")
    reader =  list(csv.reader(f, delimiter=','))
    print(type(reader))
    data_list=[]
    source=[]

    for val in list(reader):
        temp=[]
        temp.append(val[coloumns.index("name")])
        temp.append(val[coloumns.index("reference_id")])
        source.append(temp)
    print(source)
    for val in list(reader):
        temp1 = []
        temp1.append(val[coloumns.index("P.name")])
        temp1.append(val[coloumns.index("Goldkey")])
        data_list.append(temp1)
    validSims, dictionary, tf_idf, lsi=validcorpus(source)
    try:
        for index,data in enumerate(data_list):
            print(index)

            text=data[0].strip()
            inputid=data[1].strip()
            print(inputid)
            print(text)
            # with open("test.txt","a") as fh:
            #         fh.write(str(text)+"\n")
            try:
                query_doc = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", re.sub(r"¶", " ", str(word).lower())))).strip() for word in text.lower().replace("¶", " ").split() if word not in stoplist]
                # query_doc = [str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(w).lower()))).strip() for w in word_tokenize(text)if w not in stoplist]
                query_doc_bow = dictionary.doc2bow(query_doc)
                query_doc_tf_idf = tf_idf[query_doc_bow]
                if validSims.sort(key=sortSecond,reverse=True) :
                    # index_2nd, value_2nd = sorted(enumerate(validSims[lsi[query_doc_tf_idf]]), key=operator.itemgetter(1))[-2]
                    # index_3rd, value_3rd = sorted(enumerate(validSims[lsi[query_doc_tf_idf]]), key=operator.itemgetter(1))[-3]
                    index1, value = max(enumerate(validSims[lsi[query_doc_tf_idf]]), key=operator.itemgetter(1))
                    print(value)
                    input()


            #     if float(value) >= 0.0:
            #         fuzzyScore = fuzz.token_set_ratio(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(text).lower()))).strip(),str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(source[index1])))).strip())
            #         # if fuzzyScore>=50:
            #         print(text)
            #         print(index1)
            #         print(source[index1])
            #         with open("similarproducts.txt","a") as fh:
            #             fh.write(str(index)+"\t"+str(text)+"\t"+str(source[index1][0])+"\t"+str(value)+"\n")
            #         # df.loc[inputid, 'Automation Records'] = source[index1][0]
            #         # df.loc[inputid, 'Matched ID'] =source[index1][0]
            #         # df.loc[inputid, 'Score'] = value
            # except Exception as e:
            #         print(e)
            #         print(sys.exc_info()[-1].tb_lineno)
            #         with open("failed_text.txt","a") as fh:
            #             fh.write(str(e)+"  -  "+str(text))
            #         input()
            #         pass
                        # df['Automation Records'] = source[index]
                    # df['Matched ID'] = ''
                    # df['Track Item'] = ''
                    # df['Needs review title'] = text
    except Exception as e:
        print(sys.exc_info()[-1].tb_lineno)
        print(e)
        input()
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
