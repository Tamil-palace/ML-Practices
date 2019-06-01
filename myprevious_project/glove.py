import gensim.downloader as api

word_vectors = api.load("glove-wiki-gigaword-100")  # load pre-trained word-vectors from gensim-data
with open("glove-wiki-gigaword-100.txt","a") as fh:
    fh.write(str(word_vectors))

print(word_vectors.similarity('woman', 'man'))
