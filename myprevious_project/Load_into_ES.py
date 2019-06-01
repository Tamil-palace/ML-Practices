import xlrd
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': "172.27.138.44", 'port': 9200}],timeout=30)

book=xlrd.read_csv("468-Catalog-20180828_p1 .csv")
print(book.sheet_names())
# print(book.nrows)
# print(book.ncols)
input()
doc = {"titleText": title}
print(es.index(index=indexreference, doc_type="retailer", id=idList[i], body=doc))