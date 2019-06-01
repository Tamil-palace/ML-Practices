# -*- coding: utf-8 -*-
import sys
import collections
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from elasticsearch import Elasticsearch
import pandas as pd
import os
import re
import xlrd
import csv
import datetime
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta, date

def word_tokenizer(text):
        # tokenizes and stems the text
        tokens = word_tokenize(text)
        
        try:
            stemmer = PorterStemmer()
            tokens = [stemmer.stem(t) for t in tokens if t not in stopwords.words('english')]
            return tokens
        except:
            return tokens
        



def needs_review_values(sourcecontent,titleIndex,trackIndex,idIndex):
    max_row = len(sourcecontent)
    max_col = max(len(l) for l in sourcecontent)
    id, Ytitle = [], []
    for row_number in range(1, int(max_row)):
        if 'needs review' in  str(sourcecontent[row_number][trackIndex]).lower():
            id.append(str(sourcecontent[row_number][idIndex]).strip())
            Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())    
    return id, Ytitle


if __name__ == "__main__":
    
	es = Elasticsearch([{'host': '172.27.138.44', 'port': 9200}])
	fileList = ["273-Catalog-20180207_p1.csv"]

	for fList in fileList:
		startTime = datetime.now()
		# fileName = '/home/merit/OCR/catalog/' + str(sys.argv[1])
		fileName = 'D:\\OCR\\09_02_2018\\Catalog_files\\' + str(fList)
		print(fileName)
		outputfile = ''
		if 'p1.csv' in fileName:
			outputfile = fileName.replace('p1.csv', 'p2.csv')
		else:
			outputfile = fileName.replace('.csv', 'p2.csv')
		head, base = os.path.split(fileName)
		
		cNumber=re.findall(r"^([\d]+)",str(base))[0]
		f = open(fileName, 'rt', encoding="ISO-8859-1")
		df = pd.read_csv(fileName, encoding='latin1')
		replaceValueHeader = (list(df.columns.values))
		titleIndex=replaceValueHeader.index("Title")
		trackIndex=replaceValueHeader.index("Track Item")
		
		idIndex=''
		try:
			idIndex=replaceValueHeader.index("Retailer Item ID")
			df = df.set_index("Retailer Item ID")
		except:
			idIndex=replaceValueHeader.index("Retailer Item ID1")
			df = df.set_index("Retailer Item ID1")
		indexreference= str(cNumber)+'_catalog'
		print ("Processing files::", fileName)
		reader = ''
		sourcecontent = []
		
		reader = csv.reader(f, delimiter=',')
		for data in reader:
			sourcecontent.append(data)
		idList, sentences = needs_review_values(sourcecontent,titleIndex,trackIndex,idIndex)
		# df.loc[id[sentences.index(sentences[sentence])], 'Cluster ID'] = cluster
		for i, title in enumerate(sentences):
			try:
				doc = {"titleText": title}
				es.index(index=indexreference, doc_type="retailer", id=idList[i], body=doc)
			except Exception as e:
				print ("Exception::",e)
		clusterTime=datetime.now()
		clusterID=0
		groupID=[]
		for j, title in enumerate(sentences):
			groupID = list(set(groupID))
			
			checkList = [x for x in groupID if idList[j] in x ]
			if len(checkList) > 0 :
				continue    
			result = es.search(
				index=indexreference,
				doc_type='retailer',
				body={
				"query" : {
						   "more_like_this" : {
												"fields" : ["titleText"],
											   "like_text" : title,
											   "min_term_freq" : 1,
											   "min_doc_freq" : 1
											   }
						   }
					  }
			 )
			print(list(result['hits']['hits']))
			print("title==========>"+title)
			for datavalue in list(result['hits']['hits']):
				
				fuzzyScore = fuzz.token_set_ratio(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(title).lower()))).strip(), str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(datavalue['_source']['titleText'])))).strip())
				print(fuzzyScore)
				if float(fuzzyScore) >= 85 and idList[j] != datavalue['_id']:
					print("datavalue['_id']===>",datavalue['_id'])
					groupID.append(datavalue['_id'])
					print("groupID=====>",groupID)
					df.loc[datavalue['_id'], 'Cluster ID'] = clusterID
			print(clusterID)
			print(idList[j])
			# input("Stop")
			df.loc[idList[j], 'Cluster ID'] = clusterID
			clusterID += 1

		# es.indices.delete(index=indexreference)
			#sys.exit()       
		try:
			df.to_csv(outputfile, sep=',', encoding='windows-1254')
		except:
			df.to_csv(outputfile, sep=',', encoding='latin1')
		print ("Start Time::", startTime,"Clustering TIme:",clusterTime , "\nEnd Time", datetime.now())






