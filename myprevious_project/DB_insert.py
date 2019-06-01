import pymssql,csv,re,os,sys,ast

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
# cNumber=re.findall(r"^([\d]+)",str(fileName))[0]
try:
    date=re.findall(r"Catalog\-([\d]+)\_",str(base))[0]
except:
    date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

def data_insert_table(Query,table):
    val1 = ""
    print(Query)
    mssql_connection.execute(Query)
    print(mssql_connection.description)
    field_names = [i[0] for i in mssql_connection.description]
    print(field_names)
    # str(Tabledata_list)
    header_list=[]
    for val in field_names:
        val=re.sub(r'^\(\'','',str(val))
        val = re.sub(r'\'\,\)$', '', str(val))
        header_list.append(val)
    field_names_modified=[]

    for val in field_names:
        print("["+val+"]")
        val1=val1+",[" + str(val) +"]"
    print(val1)
    field_names_modified.append(val1)
    Query_1 = re.sub(r'^\,', '', str(val1))
    Query_1="select * "+(Query_1) +" from "+"["+table+"]"
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

