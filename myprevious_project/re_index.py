import re,requests


# index="asdfa.csv"
# if re.findall(r'_\d+',index,re.I):
#     tar_index=re.findall(r'_(\d+)', index, re.I)[0]
#     tar_index=int(tar_index)+1
#     targetindex=re.sub(r"_\d+","_"+str(tar_index),str(index),re.I)
#     # helpers.reindex(es_src, 'source_indexing_backup', 'source_indexing_backup', target_client=es_src)
# else:
#     targetindex=str(index.replace(".csv",""))+"_1.csv"
# print(targetindex)


def reindex_function(source_index,reindexing_index):
    try:
        data_point = {
            "source": {
                "index": source_index
            },
            "dest": {
                "index": reindexing_index
            }
        }
        print(str(data_point).replace("'", "\""))
        reindex = requests.post("http://172.27.138.44:9200/_reindex",data=str(data_point).replace("'", "\""))
        print(reindex.status_code == 200)
        if reindex.status_code == 200:
            # source_index_counturl = "http://" + str(host) + ":" + str(port) + "/" + source_index + "/_count"
            # source_indexing_reindex_counturl = "http://" + str(host) + ":" + str(port) + "/" + source_reindexing_index + "/_count"
            # reindex_count = requests.get(source_indexing_reindex_counturl).json()
            # index_count = requests.get(source_index_counturl).json()
            # print(index_count)
            # print(reindex_count)
            # if 95 < index_count['count'] / reindex_count['count']:
            #     print("reindex_count")
            #     input()
            es.indices.delete(index=source_index)
            print(str(source_index)+" index deleted.......")
            # con=json.load(str(setting.content))
    except Exception as e:
        print(e)
        sys.exit(0)
source_index="asininfo_cache"
try:
    # setting = requests.get("http://" + str(host) + ":" + str(port) + "/_cluster/health/" +str(source_index)+"?level=shards&pretty").json()
    setting = requests.get("http://172.27.138.44:9200/_cluster/health/" +str(source_index)+"?level=shards&pretty").json()
    print(setting)
    if setting['indices'][source_index]["status"] == 'yellow':
        with open("index_red_health.txt","a") as fh:
            fh.write(str(source_index) +"=====> ======>   " + str(setting['indices'][source_index]["status"])+"\n")
        reindexing_index = str(source_index) + "_reindex"
        reindex_function(source_index,reindexing_index)
        reindex_function(reindexing_index,source_index)
except Exception as e:
    print(e)
    sys.exit(0)
print(source_index)