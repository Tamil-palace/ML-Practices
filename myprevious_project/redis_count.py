import redis,json,ast

try:
    # conn = redis.StrictRedis(
    #     host='#####.publb.rackspaceclouddb.com',
    #     port=6380,
    #     password='YOUR_PASSWORD',
    #     ssl=True,
    #     ssl_ca_certs='LOCAL/PATH/TO/rackspace-ca-2016.pem')
    r = redis.StrictRedis(host="172.27.138.44", port=6379, db=0)
    # print(r.keys())
    for key in r.scan_iter("445*"):
        r.delete(key)
        print(key)

    # r = redis.StrictRedis()
    # r.set("counter", 19)
    # print(r.get("counter"))
    # print(r.incrby("counter", 23))
    # print(r.get("counter"))
    # # conn.ping()
    # print('Connected!')
    # # r.rpush("sdfsdf",["sdf","sdf"],[])
    # print((r.lrange("sdfsdf",0,-1)))
    # r.lpop("sdfsdf")
    # for  va in (r.lrange("sdfsdf",0,-1)):
    #     print(va.decode("utf-8"))
except Exception as ex:
    print('Error:', ex)
    exit('Failed to connect, terminating.')
# import requests
#
# url_1="http://172.27.138.44:9200/source_indexing/443_20180424/_search"
# # url_1 = "http://" + str(host) + ":" + str(port) + "/asininfo_cache/" + str(doc_type) + "/_search"
# print(url_1)
# existing_id_list=[]
# ping1 = requests.get(url_1).json()
# try:
#     url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
#     ping2 = requests.get(url_2).json()
#     print(ping2)
#     for val in ping2['hits']['hits']:
#         # print(val)
#         existing_id_list.append(val['_id'])
#         print(val['_source']['Title'])
#
#         # for exist_record in existing_id_list:
#         # df.loc[idList[j], 'Title'] = val['_source']['Title']
#         if 'Title' in val['_source']:
#             print(val['_source']['Title'])
#             # df.loc[idList[j], 'Title'] = val['_source']['Title']
#         else:
#             # df.loc[idList[j], 'Title'] = ""
#             print("val['_source']['Title']")
#         if 'FuzzyScore' in val['_source']:
#             print(val['_source']['FuzzyScore'])
#             # df.loc[idList[j], 'Title'] = val['_source']['Title']
#         else:
#             # df.loc[idList[j], 'Title'] = ""
#             print("val['_source']['FuzzyScore']")
#         # df.loc[idList[j], 'Platform'] = "PANTRY"
# except Exception as e:
#     print(e)