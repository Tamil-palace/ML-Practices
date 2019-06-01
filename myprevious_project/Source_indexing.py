import sys,re,requests
import pandas as pd
from elasticsearch import Elasticsearch
import elasticsearch,csv
from elasticsearch import helpers
import configparser

import os
for fileName in os.listdir("/home/merit/OCR/AsinINfo/Feb Source Files/Downloaded"):
    print(fileName)
    if fileName.endswith(".csv"):
        config = configparser.ConfigParser()
        config.read('Config.ini')

        host=config.get("Elastic-Search","host")
        port=config.get("Elastic-Search","port")

        #fileName=str("443-Catalog-20180424_p1.csv")
        cNumber=re.findall(r"^([\d]+)",str(fileName))[0]
        doc_type=cNumber
        es = Elasticsearch([{'host': host, 'port': port}])


        def needs_review_values(sourcecontent, titleIndex, trackIndex, idIndex):
            max_row = len(sourcecontent)
            max_col = max(len(l) for l in sourcecontent)
            id, Ytitle, dataVal = [], [], []
            for row_number in range(1, int(max_row)):
                tempdata = []
                if 'needs review' in str(sourcecontent[row_number][trackIndex]).lower():
                    id.append(str(sourcecontent[row_number][idIndex]).strip())
                    Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())
                elif 'needs review' not in str(sourcecontent[row_number][trackIndex]).lower():
                    # elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
                    tempdata.append(str(sourcecontent[row_number][idIndex]).strip())
                    tempdata.append(str(sourcecontent[row_number][titleIndex]).strip())
                    dataVal.append(tempdata)

            return id, Ytitle, dataVal

        def Elastic_Entry(list, op_type, field_names, doc_type, record_key="_source"):
            records = []
            count = 0
            for index, val1 in enumerate(list):
                doc = {}
                insertFlag = False
                for index1, val2 in enumerate(val1):
                    doc[field_names[index1]] = val2

                try:
                    Retailer_id = doc['Retailer Item ID']
                except:
                    Retailer_id = doc['Retailer Item ID1']
                record = {
                    "_op_type": op_type,
                    "_index": "source_indexing",
                    "_type": doc_type,
                    "_id": str(Retailer_id).strip(),
                    record_key: doc
                }
                # print(records)
                records.append(record)
                count += 1
                print(index)

                if count == 2000 or len(list) - 1 == index:
                    print("Flag arised")
                    insertFlag = True
                    count = 0
                if insertFlag:
                    try:
                        print(helpers.bulk(es, records))
                        records = []

                    except helpers.BulkIndexError as e:
                        print(e)


        def List_Split(source, sourcecontent_id, sourcecontent, replaceValueHeader):
            source_id = [source[i][0] for i, id in enumerate(source)]
            url_1 = "http://" + str(host) + ":" + str(port) + "/source_indexing/" + str(doc_type) + "/_search"
            print(url_1)
            ping1 = requests.get(url_1).json()
            update_list = []
            insert_list = []
            existing_id_list=[]
            try:
                url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
                print(url_2)
                ping2 = requests.get(url_2).json()
                # print(ping2)
                for val in ping2['hits']['hits']:
                    existing_id_list.append(val['_id'])

                insert_id_list = set(source_id).difference((existing_id_list))
                for val in insert_id_list:
                    insert_list.append(sourcecontent[sourcecontent_id.index(val)])

                update_id_list = set(source_id).difference(set(insert_id_list))
                for val in update_id_list:
                    update_list.append(sourcecontent[sourcecontent_id.index(val)])

            except KeyError as e:
                print(e, sys.exc_info()[-1].tb_lineno)
                for val in source_id:
                    insert_list.append(sourcecontent[sourcecontent_id.index(val)])
                update_list = []
            except Exception as e:
                print(e)

            Elastic_Entry(insert_list, 'create', replaceValueHeader, doc_type)
            Elastic_Entry(update_list, 'update', replaceValueHeader, doc_type, 'doc')


        reader = ''
        sourcecontent = []

        f = open(fileName, 'rt', encoding="ISO-8859-1")
        reader = csv.reader(f, delimiter=',')
        df = pd.read_csv(fileName, encoding='latin1',error_bad_lines=False)
        replaceValueHeader = (list(df.columns.values))
        titleIndex = replaceValueHeader.index("Title")
        trackIndex = replaceValueHeader.index("Track Item")
        idIndex = ''
        try:
            idIndex = replaceValueHeader.index("Retailer Item ID")
            df = df.set_index("Retailer Item ID")
        except:
            idIndex = replaceValueHeader.index("Retailer Item ID1")
            df = df.set_index("Retailer Item ID1")

        indexreference = str(cNumber) + '_catalog'
        print("Processing files::",fileName)
        # reader = ''

        for data in reader:
            sourcecontent.append(data)

        sourcecontent_id = [sourcecontent[i][idIndex] for i, id_source in enumerate(sourcecontent)]
        idList, sentences, source = needs_review_values(sourcecontent, titleIndex, trackIndex, idIndex)
        # Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber,"Source indexing Started" , "SourceIndexing", "Source indexing Started....")
        List_Split(source, sourcecontent_id, sourcecontent, replaceValueHeader)
    else:
        print("Files skipped")
        print(fileName)
        pass
