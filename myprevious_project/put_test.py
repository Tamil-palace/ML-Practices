import requests
from elasticsearch import Elasticsearch
import json

# window_size={
#         "max_result_window": 90000000

#     }
# setting = requests.put("http://172.27.138.44:9200/asininfo_cache/_settings", data=window_size)
# print(setting.content)
# print(setting.status_code)
#
# es = Elasticsearch([{'host': '172.27.138.44', 'port': 9200}], timeout=30)
# # for index in es.indices.get('*'):
# #   print(index)
# window_size={"max_result_window": 90000000}
# headers={"Content-Type": "application/json"}
#
# # ="http://172.27.138.44:9200/source_indexing/_settings"
# "http://172.27.138.44:9203/_cluster/health/?level=shards&pretty"
# setting = requests.put("http://172.27.138.44:9200/source_indexing/_settings", data=window_size,headers=headers)
# print(setting.content)
# setting = requests.get("http://172.27.138.44:9200/_cluster/health/?level=shards&pretty").json()
# print(setting)
# # con=json.load(str(setting.content))
# os
# for val in setting['indices']:
#     print(str(val)+"  ======>   "+str(setting['indices'][val]["status"]))
#     if setting['indices'][val]["status"]:
#         print(os.stat().st_size)


# def List_Split(source, sourcecontent_id, sourcecontent, replaceValueHeader):
#
#     source_id = [source[i][0] for i, id in enumerate(source)]
#     url_1 = "http://"+str(host)+":"+str(port)+"/source_indexing_1/" + str(doc_type) + "/_search"
#     print(url_1)
#     ping1 = requests.get(url_1).json()
#     update_list = []
#     insert_list = []
#     existing_id_list = []
#     try:
#         relative_count=10000
#         pingcount = int(ping1['hits']['total']) /relative_count
#         pingcount=re.sub(r'\..*', '', str(pingcount))
#         print(int(pingcount))
#         print("pingcount")
#         if int(pingcount) > 0:
#             for i in range(0,int(pingcount)):
#                 url_2 = url_1 + "?size="+str(relative_count)
#                 print(url_2)
#                 ping2 = requests.get(url_2).json()
#                 for val in ping2['hits']['hits']:
#                     existing_id_list.append(val['_id'])
#
#         if int(ping1['hits']['total'])!=0:
#             remaingsize = int(str(ping1['hits']['total'])) % relative_count
#             url_2 = url_1 + "?size=" + str(remaingsize)
#             print(url_2)
#             ping2 = requests.get(url_2).json()
#             for val in ping2['hits']['hits']:
#                 existing_id_list.append(val['_id'])
#
#             insert_id_list = set(source_id).difference((existing_id_list))
#             for val in insert_id_list:
#                 insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#
#             update_id_list = set(source_id).difference(set(insert_id_list))
#             for val in update_id_list:
#                 update_list.append(sourcecontent[sourcecontent_id.index(val)])
#
#         elif int(ping1['hits']['total'])==0 :
#             for val in source_id:
#                 insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#             update_list = []
#     except KeyError as e:
#         Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e) + " : " + str(sys.exc_info()[-1].tb_lineno),"", "")
#         print(e, sys.exc_info()[-1].tb_lineno)
#         for val in source_id:
#             insert_list.append(sourcecontent[sourcecontent_id.index(val)])
#         update_list = []
#     except Exception as e:
#         print(e)
#         Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, str(e)+" : "+str(sys.exc_info()[-1].tb_lineno), str(TitleGroupingFailed), "TitleGrouping Failed")
#
#     Elastic_Entry(insert_list, 'create', replaceValueHeader, doc_type)
#     Elastic_Entry(update_list, 'update', replaceValueHeader, doc_type, 'doc')
import os,csv

f="D:/OCR/05/24/50-Catalog-20180521.csv"
reader1=csv.reader(f,delimiter=',')
val=len(next(reader1))
print(val)
# print(os.stat(filepath).st_size)

# "http://172.27.138.44:9200/source_indexing/_mapping/Mapping_Name"
# {
#             "type_1" : {
#                         "properties" : {
#                                     "field1" : {"type" : "string"}
#                         }
#             }
# }