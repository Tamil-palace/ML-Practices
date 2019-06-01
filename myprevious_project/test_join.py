import pandas as pd
import csv,sys

fileName="71-Catalog-20180206.csv"
df=pd.read_csv(fileName, encoding='latin1',low_memory=False)
outputfilecsv="test_join.csv"
df1=pd.read_csv(outputfilecsv, encoding='latin1',low_memory=False)
# print(df.info(memory_usage='deep'))
# sys.exit(1)
inputHeader = (list(df.columns.values))
print(inputHeader)
sourcecontent=[]
f = open(fileName, 'rt', encoding="ISO-8859-1")
reader = csv.reader(f, delimiter=',')
sourcecontent_tokenized=[]
idIndex = inputHeader.index("Retailer Item ID")
#{'col2': {0: 'a', 1: 2, 2: np.nan}, 'col1': {0: 'w', 1: 1, 2: 2}}

for i,data in enumerate(reader):
    print(i)
    if i==0:
        print("asdfadf")
        continue
    sourcecontent.append(data)
    titleIndex=inputHeader.index("Title")
    # print(sourcecontent[i-1][titleIndex])
    sourcecontent_tokeniz=sourcecontent[i-1][titleIndex].split()
    sourcecontent_tokeniz_id=sourcecontent[i-1][idIndex].split()
    dic = {}
    for id in sourcecontent_tokeniz_id:
        dic[id] = {}
        for keyword in sourcecontent_tokeniz:
            #sourcecontent_tokenized.append(keyword)
            # dic[id][str(keyword)]=1
            df1.loc[str(keyword),id]=1
    # print(dic)
    # print(sourcecontent_tokenized)
    if i==1000:
        break
# print(dic)
# df=pd.DataFrame(data =dic)
print(df1.info(memory_usage='deep'))
print(df1)
# df1_a,df1
outputfilecsv="test_join1.csv"
df1.to_csv(outputfilecsv, sep=',', encoding='latin1')
    # print(sourcecontent)
# print(sourcecontent)

# pd.merge(df_a, df_b, on='subject_id', how='inner')
# raw_data = {
#         'subject_id': ['1', '2', '3', '4', '5'],
#         'first_name': ['Alex', 'Amy', 'Allen', 'Alice', 'Ayoung'],
#         'last_name': ['Anderson', 'Ackerman', 'Ali', 'Aoni', 'Atiches']}
# df_a = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name'])
# # print(df_a)
# raw_data = {
#         'subject_id': ['4', '5', '6', '7', '8'],
#         'first_name': ['Billy', 'Brian', 'Bran', 'Bryce', 'Betty'],
#         'last_name': ['Bonder', 'Black', 'Balwner', 'Brice', 'Btisan']}
# df_b = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name'])
# # print(df_b)
# raw_data = {
#         'subject_id': ['1', '2', '3', '4', '5', '7', '8', '9', '10', '11'],
#         'test_id': [51, 15, 15, 61, 16, 14, 15, 1, 61, 16]}
# df_n = pd.DataFrame(raw_data, columns = ['subject_id','test_id'])
# # print(df_n)
# df_new = pd.concat([df_a, df_b])
# print(pd.merge(df_a, df_b, on='subject_id', how='inner'))
# # print(pd.concat([df_a, df_b], axis=1))
# # print(pd.merge(df_new, df_n, on='subject_id'))
# # print(pd.merge(df_new, df_n, left_on='subject_id', right_on='subject_id'))
