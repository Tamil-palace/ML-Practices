import ftfy,html,re
from fuzzywuzzy import fuzz

target_text=" sdfsdf sdfadf THE FIRST YEARS sdfsdf  sdfsdf sdfsdf JOHN DEERE MASSAGING CORN TEETHER sdfsdf"
source_text="THE FIRST YEARS MASSAGING ACTION TEETHER"

print(fuzz.token_set_ratio(source_text,target_text))
print(fuzz.partial_token_set_ratio(source_text,target_text))

print(fuzz.QRatio(source_text,target_text))
print(fuzz.partial_token_set_ratio("React.js framework", "Reactjs"))
print(fuzz.token_set_ratio("React.js framework", "Reactjs"))


from gensim.models import Word2Vec
sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
model = Word2Vec(min_count=1)
model.build_vocab(sentences)
model.train(sentences, total_examples=model.corpus_count, epochs=model.iter)