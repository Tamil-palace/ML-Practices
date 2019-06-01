from gensim.models import KeyedVectors

# Load vectors directly from the file
# model = KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin', binary=True)

# Access vectors for specific words with a keyed lookup:
# vector = model['easy']
# see the shape of the vector (300,)
# vector.shape

# Processing sentences is not as simple as with Spacy:
# vectors = [model[x] for x in "This is some text I am processing with Spacy".split(' ')]
# from gensim.models import Word2Vec
model = KeyedVectors.load_word2vec_format('GoogleNews-vectors-negative300.bin.gz',binary=True, limit=500000)
s1 = 'THE FIRST YEARS JOHN DEERE MASSAGING CORN TEETHER'.split()
s2 = 'Nature\'s Way St. John\'s Wort Herb 350mg, 180 VCaps'.split()
distance_prenorming = model.wmdistance(s1, s2)
model.init_sims()  # calc unit-normed vectors alongside original raw vectors
distance_postnorming = model.wmdistance(s1, s2)
model.init_sims(replace=True)  # replace raw vectors in-place with unit-normed ones
distance_norms_only = model.wmdistance(s1, s2)
print(distance_prenorming, distance_postnorming, distance_norms_only)
# shows: (1.8161385991456382, 1.8161385991456382, 0.8207403953201577)
# print(model.most_similar(positive=['woman']))
print(model.similar_by_vector("woman", topn=10, restrict_vocab=None))