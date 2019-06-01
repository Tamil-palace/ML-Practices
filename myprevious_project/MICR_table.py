import pandas as pd
import os,re
import csv,pymssql


def DB_connection():
    try:
        # live
        # connection = pymssql.connect(host=host, user=user, password=password, database=database)
        # staging
        connection = pymssql.connect(host='CH1DEVBD02', user='User2', password='Merit456', database='OCR_Staging')
        return connection

    except Exception as e:
        print("error_log", e)


# "insert into [All_Inputs] (CataglogID,Retailer_Item_ID,Line) select '229'as CataglogId, Retailer_Item_ID,line from [223_input]"
connection = DB_connection()
print(connection)
# input()
mssql_connection = connection.cursor()

inputHeaders=[]
for index,fileName in enumerate(os.listdir("/home/merit/OCR/AsinINfo/Staging/Catalog_files/Files/")):
# fileName="547-Catalog-20180426_p0.csv"
    print(fileName)
    print(index)
    cNumber=re.findall(r"^([\d]+)",str(fileName))[0]
    df = pd.read_csv("/home/merit/OCR/AsinINfo/Staging/Catalog_files/Files/"+str(fileName), encoding='latin1')
    f = open("/home/merit/OCR/AsinINfo/Staging/Catalog_files/Files/"+str(fileName), 'rt', encoding="ISO-8859-1")
    inputHeader = (list(df.columns.values))
    reader = csv.reader(f, delimiter=',')
    # print(str(inputHeader).replace("[","").replace("]",""))
    val1=""
    insert_query=""
    for val in inputHeader:
        val1=str(val1)+"["+str(val)+"],"
    print(val1)
    sourcecontent = []
    insert_query="insert into MICE_Lab_table("+re.sub(",$", "", str(val1), re.I)+",[CatalogID]"+") values "
    print(insert_query)
    val3 = ""
    for data in reader:
        sourcecontent.append(data)
    for index,val in enumerate(sourcecontent[1:]):
        val2=""
        print((len(sourcecontent[1:])-1)-index)
        for valz in val:
            val2=val2+"'"+str(valz.replace("'","''"))+"',"
        val3=val3+"("+str(re.sub(",$", "", str(val2), re.I))+","+str(cNumber)+"),"
        if index!=0 and (index%900==0 or index==len(sourcecontent[1:])-1):
            val3 = str(re.sub(",$", "", str(val3), re.I))
            # print(val3)
            # print(insert_query +str(val3))
            mssql_connection.execute(insert_query +str(val3))
            connection.commit()
            # input("stop")
            val3 = ""
        insert_query = ""
    for val in inputHeader:
        inputHeaders.append(val.lower().title())
print(list(set(inputHeaders)))
val1=""
# for val in ['Brand', 'Phrm_Health Segment', 'Sub Manufacturer', 'Categorytree', 'Producttype', 'Protein Type', 'Productgroup', 'Aaa', 'Pack Of', 'Features', 'Titlmatchscore', 'Type', 'Sub Segment', 'Notes', 'Binding', 'Retailer', 'Segment', 'Upc', 'Platform', 'Title_Old', 'Sub Brand', 'Description', 'Breadcrumb', 'Manufacturer', 'Images', 'Retailer Item Id', 'Track Item', 'Subbrand', 'Product Type', 'An Brand Family', 'Title', 'Similar Asin Grouping', 'Protein Source', 'Failed Ids', 'Usage', 'Category', 'Date Added', 'Tld', 'An Subbrand', 'Vat Rate Id', 'Subcategory', 'Form', 'Base Size', 'Amazon Sub Category', 'Infant Stage', 'Sns', 'Type Of Vitamin', 'Identifiers Model', 'Client Product Group']:
#     val1=str(val1)+"["+str(val)+"] nvarchar(max),"
# print(re.sub(",$","",str(val1),re.I))
# table_create="create table MICE_Lab_table_filename_import("+str(re.sub(",$","",str(val1),re.I))+")"
# mssql_connection.execute(table_create)
# connection.commit()
