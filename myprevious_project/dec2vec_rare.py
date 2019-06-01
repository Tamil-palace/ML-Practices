import gensim
import os
import collections
import smart_open
import random
import csv
import re
import sys


# Set file names for train and test data
# test_data_dir = '{}'.format(os.sep).join([gensim.__path__[0], 'test', 'test_data'])
# lee_train_file = test_data_dir + os.sep + 'lee_background.cor'
# lee_test_file = test_data_dir + os.sep + 'lee.cor'
source=[]
nrreview=[]
def read_corpus(fname, tokens_only=False):
    try:
        f = open(fname, 'rt', encoding="utf8",errors='ignore')
        reader = iter(list(csv.reader(f, delimiter=',')))
        headers = next(reader)
        # with smart_open.smart_open(fname, encoding="iso-8859-1") as f:
        for i, line in enumerate(reader):
            # print(line)
            temp = []
            temp1 = []
            if tokens_only:
                # read_str=str(line[headers.index("Title")]) + " " + str(line[headers.index("BreadCrumb")]) + " " + str(line[headers.index("CategoryTree")])
                # read_str=str(line[headers.index("Title")])
                # temp1.append(read_str)
                # temp1.append(str(line[headers.index("Retailer Item ID")]))
                temp1.append(str(line[headers.index("name")]) + " " + str(line[headers.index("synonyms")]))
                temp1.append(line[headers.index("status")])
                temp1.append(line[headers.index("id")])
                nrreview.append(temp1)
                yield gensim.utils.simple_preprocess(str(line[headers.index("name")]) + " " + str(line[headers.index("synonyms")]))
            else:
                # For training data, add tags
                # temp.append(str(line[headers.index("Title")]) + " " + str(line[headers.index("BreadCrumb")]) + " " + str(line[headers.index("CategoryTree")]))
                # temp.append(str(line[headers.index("Title")]))
                # temp.append(str(line[headers.index("Retailer Item ID")]))
                temp.append(str(line[headers.index("name")]) + " " + str(line[headers.index("synonyms")]))
                temp.append(line[headers.index("status")])
                temp.append(line[headers.index("id")])
                source.append(temp)
                yield gensim.models.doc2vec.TaggedDocument(str(line[headers.index("name")]) + " " + str(line[headers.index("synonyms")]), [i])
    except Exception as e:
        print(e)
        print(sys.exc_info()[-1].tb_lineno)
        pass
train_file="Brand Automation-Walmart.csv"
test_file="Brand Automation-Walmart_needs.csv"
# print(headers.index("Retailer Item ID"))
# print(headers.index("Title"))
# print(headers.index("BreadCrumb"))
# print(headers.index("CategoryTree"))
# print(headers.index("ProductGroup"))
# src_corpus=[]
# nr_corpus=[]
# src_corpus_original=[]
# for index,line in enumerate(list(reader)):
#     temp=[]
#     temp1 = []
#     if not re.findall(r"Needs", str(line[headers.index("Track Item")]), re.I):
#         temp.append(str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶" + str(line[headers.index("ProductGroup")]))
#         temp.append(str(line[headers.index("Retailer Item ID")]))
#         # temp1=preprocess_1(temp)
#         src_corpus.append(temp)
#         # src_corpus_original.append(temp)
#         continue
#
#     temp1.append(str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶"+str(line[headers.index("ProductGroup")]))
#     temp1.append(str(line[headers.index("Retailer Item ID")]))
#     nr_corpus.append(temp1)
#

train_corpus = list(read_corpus(train_file))
test_corpus = list(read_corpus(test_file, tokens_only=True))
print(test_corpus)

print(train_corpus[:2])
model = gensim.models.doc2vec.Doc2Vec(size=50, min_count=2,alpha= 0.025,min_alpha=0.00025)
# model.init_sims(replace=True)
model.build_vocab(train_corpus)
# model.init_sims(replace=True)
model.train(train_corpus, total_examples=model.corpus_count,epochs=model.iter)
# model.infer_vector(test_corpus)
ranks = []
second_ranks = []
for doc_id in range(len(test_corpus)):
    print(test_corpus[doc_id])
    inferred_vector = model.infer_vector(test_corpus[doc_id])
    sims = model.docvecs.most_similar([inferred_vector], topn=len(model.docvecs))
    print(sims)
    for val in range(10):
        with open("similarity_brand_3.txt","a") as fh:
            fh.write(str(source[doc_id][0])+"\t"+str(source[doc_id][1])+"\t"+str(sims[val][1])+"\t"+str(source[sims[val][0]][0])+"\t"+str(source[sims[val][0]][1])+"\n")
        # with open("similarity_brand_2.txt","a") as fh:
            # fh.write(str(test_corpus[doc_id][0])+"\t"+
            #          str(train_corpus[doc_id][1])+"\t"+
            #          str(sims[val][1])+"\t"+
            #          str(train_corpus[sims[val][0]][0])+"\t"+
            #          str(train_corpus[sims[val][0]][1])+"\n")

            # rank = [docid for docid, sim in sims].index(doc_id)
            # print(rank)
            # ranks.append(rank)
            # print(sims[1])
            # second_ranks.append(sims[1])
            #
