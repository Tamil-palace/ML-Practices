import csv,re
from nltk.corpus import stopwords
from nltk import word_tokenize
import gensim
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

stopwords = list(set(stopwords.words('english')))
stoplist = 'for a of the and to in & - , ( ) : * \'s Etc'.split()
stopwords.extend(stoplist)

def preprocess(docs):
    # doc_list=[]
    # for doc in docs:
    #     doc_list.append(word_tokenize(doc[0].lower().replace("¶"," ")))
    # doc = [w for w in doc_list if not w in stopwords]  # Remove stopwords.
    # doc = [w for w in doc if w.isalpha()]  # Remove numbers and punctuation.
    # print(doc)
    list = []
    for document in docs:
        for word in document[0].lower().replace("¶", " ").split():
            text=""
            if word not in stopwords:
                if word.isalpha():
                    text=str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", re.sub(r"¶", " ", str(word).lower())))).strip()
            list.append(text)
    # gen_docs = []
    return list

def preprocess_1(doc):
    doc=doc[0]
    doc = doc.lower().replace("¶", " ")  # Lower the text.
    doc = word_tokenize(doc)  # Split into words.
    doc = [w for w in doc if not w in stopwords]  # Remove stopwords.
    doc = [w for w in doc if w.isalpha()]  # Remove numbers and punctuation.
    return doc

fileName="604-Catalog-20180802_p0.csv"
f = open(fileName, 'rt', encoding="utf8")
reader = iter(list(csv.reader(f, delimiter=',')))
headers=next(reader)
print(headers)
# print(headers.index("Retailer Item ID"))
# print(headers.index("Title"))
# print(headers.index("BreadCrumb"))
# print(headers.index("CategoryTree"))
# print(headers.index("ProductGroup"))
src_corpus=[]
src_corpus_original=[]
nr_corpus=[]
for index,line in enumerate(list(reader)):
    temp=[]
    temp1 = []
    if not re.findall(r"Needs", str(line[headers.index("Track Item")]), re.I):
        temp.append(str(line[headers.index("Title")]) + "¶" + str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶" + str(line[headers.index("ProductGroup")]))
        temp.append(str(line[headers.index("Retailer Item ID")]))
        # tmp=preprocess_1(temp)
        src_corpus.append(tmp)
        src_corpus_original.append(temp)
        continue

    temp1.append(str(line[headers.index("Title")])+ "¶" + str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶"+str(line[headers.index("ProductGroup")]))
    temp1.append(str(line[headers.index("Retailer Item ID")]))
    nr_corpus.append(temp1)
w2v_src_corpus=preprocess(src_corpus)
w2v_nr_corpus=preprocess(nr_corpus)

# Taggeddocuments = [TaggedDocument(words=doc, tags=[i]) for i, doc in enumerate(w2v_src_corpus)]
# max_epochs = 100
# vec_size = 20
# alpha = 0.025
#
# model = Doc2Vec(size=vec_size,alpha=alpha,min_alpha=0.00025,min_count=1,dm=1)
# model.build_vocab(aggeddocuments)
# for epoch in range(max_epochs):
#     print('iteration {0}'.format(epoch))
#     model.train(tagged_data,
#                 total_examples=model.corpus_count,
#                 epochs=model.iter)
#     # decrease the learning rate
#     model.alpha -= 0.0002
#     # fix the learning rate, no decay
#     model.min_alpha = model.alpha
#
# model.save("d2v.model")
# print("Model Saved")
# model= Doc2Vec.load("d2v.model")
# test_data = word_tokenize("I love chatbots".lower())

model = gensim.models.Word2Vec(min_count=1, workers=3, size=100)
model.build_vocab(w2v_src_corpus)
model = gensim.models.Word2Vec(w2v_src_corpus, workers=3, size=100)
model.train(w2v_src_corpus, total_examples=model.corpus_count, epochs=model.iter)
num_best = 10
instance = gensim.similarities.WmdSimilarity(w2v_src_corpus, model, num_best=10)

for query in w2v_nr_corpus:
    print(query)
    sims = instance[query]
    print(sims[0])
    print(w2v_src_corpus[sims[0][0]])

# # print(instance)