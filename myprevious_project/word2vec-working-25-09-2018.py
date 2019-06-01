import csv,re
from nltk.corpus import stopwords
from nltk import word_tokenize
import gensim

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
    temp = []
    list = []
    for document in docs:
        # print(document)
        for word in word_tokenize(document[0].lower().replace("¶", " ")):
            if word not in stopwords:
                if word.isalpha():
                    temp.append(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", re.sub(r"¶", " ", str(word).lower())))).strip())
        list.append(temp)

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
# print(headers.index("Retailer Item ID"))
# print(headers.index("Title"))
# print(headers.index("BreadCrumb"))
# print(headers.index("CategoryTree"))
# print(headers.index("ProductGroup"))
src_corpus=[]
nr_corpus=[]
src_corpus_original=[]
for index,line in enumerate(list(reader)):
    temp=[]
    temp1 = []
    if not re.findall(r"Needs", str(line[headers.index("Track Item")]), re.I):
        temp.append(str(line[headers.index("Title")]) + "¶" + str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶" + str(line[headers.index("ProductGroup")]))
        temp.append(str(line[headers.index("Retailer Item ID")]))
        temp1=preprocess_1(temp)
        print(temp1)
        src_corpus.append(temp1)
        src_corpus_original.append(temp)
        continue

    temp1.append(str(line[headers.index("Title")])+ "¶" + str(line[headers.index("BreadCrumb")]) + "¶" + str(line[headers.index("CategoryTree")]) + "¶"+str(line[headers.index("ProductGroup")]))
    temp1.append(str(line[headers.index("Retailer Item ID")]))
    nr_corpus.append(temp1)

# w2v_src_corpus=preprocess(src_corpus)
# w2v_nr_corpus=preprocess(nr_corpus)

model = gensim.models.Word2Vec(min_count=1, workers=3, size=100)
model.build_vocab(src_corpus)
# model = gensim.models.Word2Vec(w2v_src_corpus, workers=3, size=100)
model.train(src_corpus, total_examples=model.corpus_count, epochs=model.iter)
num_best = 10
instance = gensim.similarities.WmdSimilarity(src_corpus, model, num_best=10)

# for query in w2v_nr_corpus:
for index,temp1 in enumerate(nr_corpus):
    print(temp1)
    query_corpus= preprocess_1(temp1)
    print(query_corpus)
    # query_corpus = preprocess(query)
    sims = instance[query_corpus]
    # print(src_corpus[sims[0][0]])
    for i in range(num_best):
        # print('sim = %.4f' % sims[i])
        print(str(sims[i])+" =====> "+str(src_corpus_original[sims[i][0]]))
        with open("similarity.txt","a") as fh:
            fh.write(str(nr_corpus[index][1])+"\t"+str(sims[i])+"\t"+str(src_corpus_original[sims[i][0]])+"\n")
