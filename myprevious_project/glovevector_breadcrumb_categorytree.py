#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
word2vec embeddings start with a line with the number of lines (tokens?) and
the number of dimensions of the file. This allows gensim to allocate memory
accordingly for querying the model. Larger dimensions mean larger memory is
held captive. Accordingly, this line has to be inserted into the GloVe
embeddings file.
"""

import os
import re
import shutil
import hashlib
from sys import platform
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import pymssql
import gensim
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd
import csv

def prepend_line(infile, outfile, line):
    """
    Function use to prepend lines using bash utilities in Linux.
    (source: http://stackoverflow.com/a/10850588/610569)
    """
    with open(infile, 'r') as old:
        with open(outfile, 'w') as new:
            new.write(str(line) + "\n")
            shutil.copyfileobj(old, new)

def prepend_slow(infile, outfile, line):
    """
    Slower way to prepend the line by re-creating the inputfile.
    """
    with open(infile, 'r') as fin:
        with open(outfile, 'w') as fout:
            fout.write(line + "\n")
            for line in fin:
                fout.write(line)

def checksum(filename):
    """
    This is to verify the file checksum is the same as the glove files we use to
    pre-computed the no. of lines in the glove file(s).
    """
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

# Pre-computed glove files values.
pretrain_num_lines = {"glove.840B.300d.txt": 2196017, "glove.42B.300d.txt" :1917494}

pretrain_checksum = {
    "glove.6B.300d.txt" :"b78f53fb56ec1ce9edc367d2e6186ba4",
    "glove.twitter.27B.50d.txt" :"6e8369db39aa3ea5f7cf06c1f3745b06",
    "glove.42B.300d.txt" :"01fcdb413b93691a7a26180525a12d6e",
    "glove.6B.50d.txt" :"0fac3659c38a4c0e9432fe603de60b12",
    "glove.6B.100d.txt" :"dd7f3ad906768166883176d69cc028de",
    "glove.twitter.27B.25d.txt" :"f38598c6654cba5e6d0cef9bb833bdb1",
    "glove.6B.200d.txt" :"49fa83e4a287c42c6921f296a458eb80",
    "glove.840B.300d.txt" :"eec7d467bccfa914726b51aac484d43a",
    "glove.twitter.27B.100d.txt" :"ccbdddec6b9610196dd2e187635fee63",
    "glove.twitter.27B.200d.txt" :"e44cdc3e10806b5137055eeb08850569",
}

def check_num_lines_in_glove(filename, check_checksum=False):
    if check_checksum:
        assert checksum(filename) == pretrain_checksum[filename]
    if filename.startswith('glove.6B.'):
        return 400000
    elif filename.startswith('glove.twitter.27B.'):
        return 1193514
    else:
        return pretrain_num_lines[filename]

def w2v(s1, s2, wordmodel):
    if s1 == s2:
        return 1.0

    s1words = s1.split()
    s2words = s2.split()
    s1wordsset = set(s1words)
    s2wordsset = set(s2words)
    vocab = wordmodel.vocab  # the vocabulary considered in the word embeddings
    # if len(s1wordsset & s2wordsset)==0:
    #         return 0.0
    # for word in s1wordsset.copy(): #remove sentence words not found in the vocab
    #         try:
    #                 if (word not in vocab):
    #                         s1words.remove(word)
    #         except:
    #                 continue
    # for word in s2wordsset.copy(): #idem
    #         try:
    #                 if (word not in vocab):
    #                         s2words.remove(word)
    #         except:
    #                 continue
    s1w = filter(lambda x: x in wordmodel.vocab, s1wordsset)
    # print(s1w)
    s2w = filter(lambda x: x in wordmodel.vocab, s2wordsset)
    # print(s2w)
    # return wordmodel.n_similarity(s1w, s2w)
    return wordmodel.wmdistance(s1w,s2w)

# Input: GloVe Model File
# More models can be downloaded from http://nlp.stanford.edu/projects/glove/
# glove_file= "glove.840B.300d.txt"
glove_file= "glove.6B.50d.txt"
_, tokens, dimensions, _ = glove_file.split('.')
num_lines = check_num_lines_in_glove(glove_file)
dims = int(dimensions[:-1])

# Output: Gensim Model text format.
gensim_file = 'glove_model.txt'
gensim_first_line = "{} {}".format(num_lines, dims)

# Prepends the line.
if platform == "linux" or platform == "linux2":
    prepend_line(glove_file, gensim_file, gensim_first_line)
else:
    prepend_slow(glove_file, gensim_file, gensim_first_line)

# Demo: Loads the newly created glove_model.txt into gensim API.
# model = gensim.models.Word2Vec.load_word2vec_format(gensim_file, binary=False)  # GloVe Model
model = gensim.models.KeyedVectors.load_word2vec_format(gensim_file, binary=False)  # GloVe Model

# print model.most_similar(positive=['australia'], topn=10)
# print model.similarity('woman', 'man')

sentencee = []

stop_words = set(stopwords.words('english'))

def word_tok(sent):
    try:
        word_tokens_s2 = word_tokenize(sent.decode(errors='ignore'))
        filtered_sentence_s2 = [w for w in word_tokens_s2 if not w in stop_words]
        regex = re.compile(r'[a-zA-Z]+')
        selected_files_s2 = filter(regex.search, filtered_sentence_s2)
        # print(selected_files_s2)
        return selected_files_s2
    except Exception as e:
        print(e)
        print(sys.exc_info()[-1].tb_lineno)
        input()


# for i in range(len(sentencee)):
    # stop_words = stop_words.add('a')

# def DB_connection():
#     try:
#         # live
#         # connection = pymssql.connect(host=host, user=user, password=password, database=database)
#         # staging
#         connection = pymssql.connect(host='CH1DEVBD02', user='User2', password='Merit456', database='OCR_Staging')
#         return connection
#
#     except Exception as e:
#         print("error_log", e)
#
# connection = DB_connection()
# cursor = connection.cursor()
#
# # sourceCountQuery = "select * from [MICE_Lab_table] where CatalogID=538"
# # sourceCountQuery = "select [Retailer Item Id],Productgroup+'¶'+Breadcrumb+'¶'+Categorytree+'¶'+Binding as Merged_Col from [MICE_Lab_table_filename] where CatalogID=547"
# sourceCountQuery = "select [Retailer Item Id],Title+'¶'+Description+'¶'+Features as Merged_Col from [MICE_Lab_table_filename] where CatalogID=50"
# cursor.execute(sourceCountQuery)
# all = cursor.fetchall()
# connection.commit()
# print(all)
# with open("PR_General_Input_Desc_4000.txt") as f:
fileName="604-Catalog-20180802_p0.csv"
df = pd.read_csv(fileName, encoding='latin1', error_bad_lines=False)
inputHeader = (list(df.columns.values))
titleIndex=inputHeader.index("Title")
brandIndex = inputHeader.index("Brand")
breadcrumbIndex = inputHeader.index("BreadCrumb")
CategoryIndex = inputHeader.index("CategoryTree")
ProductGroupIndex = inputHeader.index("ProductGroup")
trackItemIndex=inputHeader.index("Track Item")
idIndex=''
try:
    idIndex=inputHeader.index("Retailer Item ID")
    df = df.set_index("Retailer Item ID")
except:
    idIndex=inputHeader.index("Retailer Item ID1")



all=[]
test_list=[]

openedfile=open(fileName,"rt")
reader=list(csv.reader(openedfile,delimiter=','))

for index,data in enumerate(reader):
    print(index)
    temp=[]
    temp1=[]
    if not re.findall(r"Needs", str(reader[index][inputHeader.index("Track Item")]), re.I):
        print("True"+str(index))
        data=str(reader[index][titleIndex])+"¶"+str(reader[index][breadcrumbIndex])+"¶"+str(reader[index][CategoryIndex])+"¶"+str(reader[index][ProductGroupIndex])
        id=str(reader[index][idIndex])
        temp.append(id)
        temp.append(data)
        all.append(temp)
        continue
    data_1 = str(reader[index][titleIndex]) + "¶" + str(reader[index][breadcrumbIndex]) + "¶" + str(reader[index][CategoryIndex]) + "¶" + str(reader[index][ProductGroupIndex])
    id_1 = str(reader[index][idIndex])
    temp1.append(id_1)
    temp1.append(data_1)
    test_list.append(temp1)


for line in all:
    text1 = re.sub(r'[^!-~\s]', "", line[1])
    text1 = re.sub(r'\W', " ", text1)
    text1 = re.sub(r'\s\s+', " ", text1)
    text1 = re.sub("¶", " ", str(text1), re.MULTILINE)
    sentencee.append(text1)
# f.close

for i in range(len(sentencee)):
    print(i)
    s1 = word_tok(sentencee[i].lower())
    s1 = ' '.join([str(elem) for elem in s1])
    s1 = re.sub(r'[^!-~\s]', "", s1)
    s1 = re.sub("\W+",' ',s1)
    s1 = re.sub("\s\s+",' ',s1)
    # s2 = ''
    for j in range(len(test_list)):
        print("sdfsdf"+str(j))
        try:
            if (j < len(test_list)):
                try:
                    s2 = word_tok(test_list[j][1].lower())
                except Exception as e:
                    print(sys.exc_info()[-1].tb_lineno)
                    pass
                try:
                    s2 = ' '.join([str(elem) for elem in s2])
                    s2 = re.sub(r'[^!-~\s]', "", s2)
                    s2 = re.sub("\W+", ' ', s2)
                    s2 = re.sub("\s\s+", ' ', s2)
                    s2=re.sub("¶", " ", str(s2), re.MULTILINE)
                    # if((len(s1) > 1 and len(s2) > 1) or (s1 != s2) or (i<j)):
                    #  print ("sim(s"+str(i), ",s"+str(j), ") = ", w2v(s1, s2, wordmodel), "/1.")
                    k1 = w2v(s1, s2, model)
                    # writ = str(i)+"\t"+str(j)+"\t"+sentencee[i]+"\t"+sentencee[j]+"\t"+ str(k1)+"\n"
                    # writ = str(i)+"\t"+str(j)+"\t"+ str(k1)+ "\t"+ str(sentencee[i].lower())+"\t"+ str(sentencee[j].lower())+"\n"
                    writ = str(all[i][0])+"\t"+str(all[i][1]).split("¶")[0]+"\t"+str(all[i][1]).split("¶")[1]+"\t"+str(all[i][1]).split("¶")[2]+"\t"+str(all[i][1]).split("¶")[3]+"\t"+str(test_list[j][0])+"\t"+str(test_list[i][1]).split("¶")[0]+"\t"+str(test_list[i][1]).split("¶")[1]+"\t"+str(test_list[i][1]).split("¶")[2]+"\t"+str(test_list[i][1]).split("¶")[3]+"\t"+ str(k1)+ "\n"
                    if (k1 <= 3.5):
                        print(k1)
                    with open('Output_26_09_breadcrumb_category.txt', 'ab') as f:
                        f.write(str(writ))
                    # else:
                    #     with open('Invalid_Output_25_09_desc_feature.txt', 'ab') as f:
                    #         f.write(str(writ))
                except Exception as e:
                    print(e)
                    print(sys.exc_info()[-1].tb_lineno)
                    pass
        except Exception as e:
            print(e)
            input()
            pass
