# import spacy
# nlp = spacy.load('en')
# tweet_doc = self.nlp("Here goes your some input text")
# print(tweet_doc.vector)
# from pandas import read_csv
# from sklearn.feature_selection import RFE
# from sklearn.linear_model import LogisticRegression
# # load data
# url = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"
# names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
# dataframe = read_csv(url, names=names)
# array = dataframe.values
# X = array[:,0:8]
# Y = array[:,8]
# # feature extraction
# model = LogisticRegression()
# rfe = RFE(model, 3)
# fit = rfe.fit(X, Y)
# print("Num Features: "+str(fit.n_features_))
# print("Selected Features:"+str(fit.support_))
# print("Feature Ranking: "+str( fit.ranking_))

cat brand_validataion.txt |awk -F $'\t'  '{if($10>0.90 && $9<0.2)  print $0} ' >> brand_validation_jaro_90_lev_01.txt

arr=[[21,12,4,42,1],
    [1,2,24,242,11],
    [1,2,3,4,5]]
# print(arr[0:1])
# print(arr[:1])
print(arr[:1:2])