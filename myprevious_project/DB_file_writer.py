import pymssql,csv,re,os,sys,ast
from elasticsearch import Elasticsearch
import requests, json
from elasticsearch import helpers


head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
# cNumber=re.findall(r"^([\d]+)",str(fileName))[0]
try:
    date=re.findall(r"Catalog\-([\d]+)\_",str(base))[0]
except:
    date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

host = "172.27.138.44"
es = Elasticsearch([{'host': host, 'port': 9200}])
#'create'
def Elastic_Entry(list,op_type,field_names,doc_type,record_key="_source"):
    records=[]
    count = 0
    for index, val1 in enumerate(list):
        doc = {}
        insertFlag = False
        # print(val1)
        # input("each row")
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
                # print(records)
                # input()
                print(helpers.bulk(es, records))
                input()
                records=[]

            except helpers.BulkIndexError as e:
                print(e)
                input("Eroro")


def List_Split(sourcecontent_id,sourcecontent,replaceValueHeader):
    # source_id = [source[i][0] for i, id in enumerate(source)]
    # for i, id in enumerate(source):
    #     with open("file1.txt", "a") as f:
    #         f.write(str(id[0]) + "\n")
    url_1 = "http://172.27.138.44:9200/source_indexing/" + str(doc_type) + "/_search"
    print(url_1)
    input()
    ping1 = requests.get(url_1).json()
    update_list = []
    insert_list = []
    try:
        url_2 = url_1 + "?size=" + str(ping1['hits']['total'])
        ping2 = requests.get(url_2).json()
        existing_id_list = []
        for val in ping2['hits']['hits']:
            existing_id_list.append(val['_id'])
            # with open("file.txt", "a") as f:
            #     f.write(str(val['_id']) + "\n")
        print("source_id" + str(len(source_id)))

        print("existing_id_list")
        print(len(existing_id_list))

        insert_id_list = set(sourcecontent_id).difference(set(existing_id_list))
        for val in insert_id_list:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])

        print("insert_list")
        input(len(insert_list))

        update_id_list = set(sourcecontent_id).difference(set(insert_id_list))
        for val in update_id_list:
            update_list.append(sourcecontent[sourcecontent_id.index(val)])
        print("update_list")
        # print(insert_list)
        input(len(update_list))
        input()
    except KeyError as e:
        print(e, sys.exc_info()[-1].tb_lineno)
        for val in source_id:
            insert_list.append(sourcecontent[sourcecontent_id.index(val)])
        # insert_list=source_id
        update_list = []
    except Exception as e:
        print(e)
        sys.exit(0)

    Elastic_Entry(insert_list, 'create', replaceValueHeader, doc_type)
    Elastic_Entry(update_list, 'update', replaceValueHeader, doc_type, 'doc')



def data_insert_table(Query,table):
    doc_type = cNumber + "_" + date
    val1 = ""
    print(Query)
    mssql_connection.execute(Query)
    print(mssql_connection.description)
    input("mssql_connection")
    field_names = [i[0] for i in mssql_connection.description]
    print(field_names)
    input("field_names")
    # str(Tabledata_list)
    header_list=[]
    for val in field_names:
        val=re.sub(r'^\(\'','',str(val))
        val = re.sub(r'\'\,\)$', '', str(val))
        header_list.append(val)
    input(header_list)
    input()
    field_names_modified=[]

    for val in field_names:
        print("["+val+"]")
        val1=val1+",[" + str(val) +"]"
    print(val1)
    field_names_modified.append(val1)
    Query_1 = re.sub(r'^\,', '', str(val1))
    Query_1="select * "+(Query_1) +" from "+"["+table+"]"
    input(Query_1)
    mssql_connection.execute(Query_1)
    Tabledata_list = mssql_connection.fetchall()
    Tabledata_list_id=[]
    print('header_list')
    print(field_names)
    print(header_list)
    for val in Tabledata_list:
        Tabledata_list_id.append(val[header_list.index('Retailer_Item_ID')])
    print(Tabledata_list_id)

    List_Split(Tabledata_list_id, Tabledata_list, field_names)

connection = pymssql.connect(host='CH1DEVBD02', user='User2', password='Merit456', database='OCR_Staging')
mssql_connection = connection.cursor()

with open('Catalog_backup_input.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    InTableHeadersQuery="SELECT top 0 * from [336_Output]"
    # data_insert_table(InTableHeadersQuery,True)
    InTableData="select * from [336_Output]"
    # data_insert_table(InTableData,False)

with open('Catalog_backup_output.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    # OutTableHeadersQuery = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('336_Output')"
    OutTableHeadersQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '336_Input'"
    # print(OutTableHeadersQuery)
    # print(OutTableHeadersQuery)
    # data_insert_table(InTableHeadersQuery,True,"")
    # # OutTableData = "select * from [336_Output]"
    data_insert_table(OutTableHeadersQuery,'336_Output')

