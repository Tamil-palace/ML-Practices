import sys,pymssql,csv,os,datetime,re
import pandas as pd
import subprocess
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import configparser
import ast
import redis
import os
from elasticsearch import Elasticsearch
import elasticsearch

r = redis.StrictRedis()

config = configparser.ConfigParser()
config.read('Config.ini')

host=config.get("Elastic-Search", "host")
port=config.get("Elastic-Search", "port")
es = Elasticsearch([{'host': host, 'port': port}])

#DB
user=config.get("Database", "user")
host=config.get("Database", "host")
password=config.get("Database", "password")
database=config.get("Database", "database")

#Email
login=config.get("Email", "login")
sender=config.get("Email", "sender")
receivers=config.get("Email", "receivers").split(',')
Email_passwd=config.get("Email", "password")

#status
ToStart=config.get("Status", "To-Start")

AsinInfoStarted=config.get("Status", "AsinInfo-Started")
AsinInfoFailed=config.get("Status", "AsinInfo-Failed")
AsinInfoCompleted=config.get("Status", "AsinInfo-Completed")

EmptyConfigCreated=config.get("Status", "Empty-Config-Created")
EmptyConfigFailed=config.get("Status", "Empty-Config-Failed")

KeywordExtractionStarted=config.get("Status", "Keyword-Extraction-Started")
KeywordExtractionFailed=config.get("Status", "Keyword-Extraction-Failed")
KeywordExtractionCompleted=config.get("Status", "Keyword-Extraction-Completed")

TitleClassificationStarted=config.get("Status", "Title-Classification-Started")
TitleClassificationFailed=config.get("Status", "Title-Classification-Failed")
TitleClassificationCompleted=config.get("Status", "Title-Classification-Completed")

TitleGroupingStarted=config.get("Status", "Title-Grouping-Started")
TitleGroupingFailed=config.get("Status", "Title-Grouping-Failed")
TitleGroupingCompleted=config.get("Status", "Title-Grouping-Completed")

TranslationStarted=config.get("Status", "Translation-Started")
TranslationFailed=config.get("Status", "Translation-Failed")
TranslationCompleted=config.get("Status", "Translation-Completed")

DBImportStarted=config.get("Status", "DBImport-Started")
DBImportFailed=config.get("Status", "DBImport-Failed")
DBImportCompleted=config.get("Status", "DBImport-Completed")

def DB_connection():
    try:
        # live
        connection = pymssql.connect(host=host, user=user, password=password, database=database)
        # staging
        # connection = pymssql.connect(host='CH1DEVBD01', user='User2', password='Merit456', database='OCR_Staging')
        return connection

    except Exception as e:
        print("error_log", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(str(exc_tb.tb_lineno))
        error_log = e;
        #Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, error_log, "-6", "Database DB connection failure")

def CurrentPath():
    process = subprocess.Popen(['pwd'], stdout=subprocess.PIPE)
    out, err = process.communicate()
    current_path = out.decode("utf-8")
    current_path = re.sub("\s*", "", str(current_path))
    print(current_path)
    return (str(current_path))

def Automated_table_creation():
    try:
        connection = DB_connection()
        cursor = connection.cursor()
        query = "SELECT * FROM [Catalog_Automation_Status]"
        cursor.execute(query)
        connection.commit()
        print("table exist")
        # cursor.execute("drop table Catalog_Automation_Status")
        # connection.commit()
    except:
        print("table not exist")
        tabl_create_query="CREATE TABLE Catalog_Automation_Status (SoNo int not null identity(1,1) ,Catalog_Id int primary key, Raw_File  nvarchar(1000),Steps  nvarchar(1000),Catalog_Status int,Uploaded_Date_Time datetime,SingleStepProcess nvarchar(MAX))"
        cursor.execute(tabl_create_query)
        connection.commit()

    try:
        query = "SELECT * FROM [OCR_Log_Tracker]"
        cursor.execute(query)
        connection.commit()
        print("table exist")
        # cursor.execute("drop table OCR_Log_Tracker")
        # connection.commit()
    except:
        print("table not exist")
        log_create_query="CREATE TABLE OCR_Log_Tracker (Id int identity(1,1),Catalog_Id INT NOT NULL,Logs VARCHAR(MAX))"
        cursor.execute(log_create_query)
        connection.commit()
    return

def Log_writer(logfile_name,cat_id,error_log,catalog_status,steps):
    connection = DB_connection()
    cursor = connection.cursor()
    catalog_status_query = "update Catalog_Automation_Status set Catalog_Status=" + catalog_status + ",steps='"+steps+"' where Catalog_Id=" + str(cat_id)
    print(catalog_status_query)
    cursor.execute(catalog_status_query)
    connection.commit()
    current_time = str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    log=str(str(current_time) + " - " + str(cat_id) + " - " + str(error_log)) + "\n"
    log = re.sub("\'", "", str(log))
    log_query="insert into OCR_Log_Tracker values("+str(cat_id)+", '"+str(log)+"','"+str(current_time)+"')"
    print(log_query)
    #input()
    cursor.execute(log_query)
    connection.commit()
    with open(CurrentPath()+"/Logfiles/"+logfile_name, "a") as log_filehandler:
        log_filehandler.write(log)
    return


def Start_Process(script_with_args):
    start_cmd="/usr/bin/python3  " +CurrentPath()+"/"+script_with_args
    print("***************")
    print(start_cmd)
    print("***************")
    os.system(start_cmd)
    return

def create_config(flag,raw_filename,cat_id):
    if flag:
        config_create_cmd="config_creation.py "+raw_filename
        Start_Process(config_create_cmd)
        DB_update(cat_id, str(EmptyConfigCreated),"","Empty Config Created")
    else:
        #config file already available
        # head, base = os.path.split(sys.argv[1])
        cNumber = re.findall(r"^([\d]+)", str(raw_filename))[0]
        if os.path.exists(cNumber+"-Config.xlsx"):
            config_status="Config file available already"
        else:
            config_status="Config fila not available"
        Log_writer("OCR_generic_error.log", cat_id, config_status, "-1", "Asin Info process is failed before start")

    return

def Progress_Count(inputData,currentCount,missingID,cat_id):
    if round(inputData/ 4) == currentCount:
        DB_update(cat_id, str(currentCount),25, "progress")

    elif round(inputData/ 2) == currentCount:
        DB_update(cat_id, str(currentCount),50, "progress")

    elif round(inputData/ 4) * 3 == currentCount:
        DB_update(cat_id, str(currentCount),75, "progress")

    elif inputData-1 == currentCount and missingID != 0:
        DB_update(cat_id, str(currentCount) + "-M" + str(missingID),'100+MIDs', "progress")

    elif inputData-1 == currentCount and missingID == 0:
        DB_update(cat_id, str(currentCount) + "-M" + str(missingID),'100', "progress")

    return

def DB_update(cat_id,updated_status,percentage,steps):
    connection = DB_connection()
    cursor = connection.cursor()
    if steps!='progress':
        catalog_status_query = "update Catalog_Automation_Status set Catalog_Status=" + updated_status + ",steps='"+steps+"' where Catalog_Id=" + str(cat_id)
        print("***************")
        print(catalog_status_query)
        print("***************")
        cursor.execute(catalog_status_query)
        connection.commit()
    else:
        catalog_status_query = "update Catalog_Automation_Status set Progress_Count='" + str(updated_status) + "',Completed_Percentage='"+str(percentage)+"' where Catalog_Id=" + str(cat_id)
        print("***************")
        print(catalog_status_query)
        print("***************")
        cursor.execute(catalog_status_query)
        connection.commit()

    return


def email_sender(TEXT,sub,MimeFlag=True):
    # host = '74.80.234.196'
    # port = '25'
    # user = 'meritgroup'
    # domain = 'meritgroup.co.uk'

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(receivers)
    msg['Subject'] = "OCR Alert -"+sub
    if MimeFlag:
        val=MIMEText(TEXT)
    else:
        val = MIMEText(TEXT,"html")
    msg.attach(val)

    smtpObj = smtplib.SMTP('smtp.gmail.com:587')
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(login, Email_passwd)
    smtpObj.sendmail(sender, receivers, msg.as_string())


def TLD_ContryCode_mapping(script_name,TLD,raw_file,cat_id):
    if TLD in ["com", "COM"]:
        start_cmd = script_name+ " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "US"
    elif TLD in ["couk", "COUK"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "UK"
    elif TLD in ["fr", "FR"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "FR"
    elif TLD in ["de", "DE"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "DE"
    elif TLD in ["ca", "CA"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "CA"
    elif TLD in ["in", "IN"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "IN"
    elif TLD in ["it", "IT"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "IT"
    elif TLD in ["es", "ES"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "ES"
    elif TLD in ["cn", "CN"]:
        start_cmd = script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "CN"
    elif TLD in ["jp", "JP"]:
        start_cmd =script_name + " " + CurrentPath() + "/Catalog_files/" + raw_file + " " + "JP"
    else:
        error_log = "Allowed regions [US,UK,FR,DE,CA,IN,IT,ES]"
        Log_writer("OCR_generic_error.log", cat_id, error_log, "-1", "Asin Info process is failed before start")
        # continue
        sys.exit(0)
    return (start_cmd)

def Email_Html(total_list,cat_id):
    Email_content=""
    for val in total_list:
        if val in ["Asin-Info-Process", "Asin-B", "Asin-M", "Asin-P", "Asin-Title", "Asin-IDM"] :
            Email_content = """<tr>
            <td> Asin Info : API Crawling </td>
            <td> Completed</td>
            </tr>"""
        elif val == "Title-Classification":
            val = "Title Classification : Product based classification"
            Email_content += """<tr>
            <td>""" + val + """</td>
            <td>Completed</td>
          </tr>"""
        elif val == "Keyword-Extraction":
            val = "Keyword Extraction : Splitting keywords from titles"
            Email_content += """<tr>
            <td>""" + val + """</td>
            <td>Completed</td>
          </tr>"""
        elif val == "Product-Grouping":
            val = "Product Grouping : Group similar products using title"
            Email_content += """<tr>
            <td>""" + val + """</td>
            <td>Completed</td>
          </tr>"""
        elif val == "Translation":
            val = "Translation:Translating product titles"
            Email_content += """<tr>
            <td>""" + val + """</td>
            <td>Completed</td>
          </tr>"""

        # print("Email" + str(Email_content))

    html = """<!DOCTYPE html>
    <html>
    <head>
    <style>
    table {
        border-collapse: collapse;
        width: 30%;
    }

    th, td {
        text-align: left;
        padding: 4px;
    }

    tr:nth-child(even){background-color: #f2f2f2}

    th {
        background-color: #4CAF50;
        color: white;
    }
    </style>
    </head>
    <body>
    Hello everyone,

    <h2>"""+str(cat_id)+""" Catalog Overall Summary</h2>
    <table>
      <tr>
        <th>Steps</th>
        <th>Status</th>
      </tr>
     """ + str(Email_content) + """

    <br>
    <br>
     Thanks,<br>
     OCR IT Team
    </table>
    </body>
    </html>
    """
    return html

if __name__ == "__main__":
    connection = DB_connection()
    cursor = connection.cursor()
    Automated_table_creation()
    startQuery="select * from Catalog_Automation_Status where Catalog_Status="+str(ToStart)
    cursor.execute(startQuery)
    result=cursor.fetchall()
    if len(result)==0:
        e="Please set Catalog_Status to 0 in Catalog_Automation_Status table"
        print(e)
        # Log_writer("OCR_generic_error.log", "", e, "-1", "Asin Info process is failed before start")
        sys.exit(1)

    start_cmd=""
    for row in result:
        filepath = CurrentPath() + "/Catalog_files/" + str(row[2])
        try:
            cat_id=row[1]
            print(filepath)
            df = pd.read_csv(filepath, encoding='latin1',error_bad_lines=False)
        except FileNotFoundError as e:
            print("FileNotFoundError :",e)
            Log_writer("OCR_generic_error.log",cat_id, e,str(AsinInfoFailed),"Failed ,Before start")
            continue
        except Exception as e:
            print(e)
            Log_writer("OCR_generic_error.log",cat_id, e,str(AsinInfoFailed),"Failed , before start")
            continue
        inputHeader = (list(df.columns.values))
        # input(len(inputHeader))
        # if len(inputHeader)==0:
        #     Log_writer("OCR_generic_error.log", cat_id, "File is empty", str(AsinInfoFailed), "Failed , Before start")
        #     continue
        try:
            countryIndex = inputHeader.index("TLD")
        except Exception as e:
            print(e)
            Log_writer("OCR_generic_error.log", cat_id, str(e), "-1", "File Issue ,Failed")
            continue
        f = open(filepath, 'rt', encoding="ISO-8859-1")
        reader =  list(csv.reader(f, delimiter=','))

        command = row[9]
        Isdbimport=eval(str(row[11]))
        IsFulltitleCase=eval(str(row[10]))
        #print(str(row[14]))
        IsFullTranslation=eval(str(row[14]))
        Force_Import=eval(str(row[16]))
        # IsFullTranslation=True
        if Isdbimport is None:
            print("Please mention DB import option")
            e = "Please mention DB import option for the catalog ID " + str(cat_id)
            Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Before Start Isdbimport ,Failed")
            sys.exit(0)
        if IsFulltitleCase is None:
            print("Please mention Full title Case option")
            e = "Please mention Full title Case option for the catalog ID " + str(cat_id)
            Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Before Start IsFulltitleCase, Failed")
            sys.exit(0)

        if IsFullTranslation is None:
            print("Please mention Full Translation Case option")
            e = "Please mention Full Translation Case option for the catalog ID " + str(cat_id)
            Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Before Start IsFullTranslation, Failed")
            sys.exit(0)

        print(command)
        if command!='N/A':
            command=command.strip()
            if command=="":
                commands=[]
            else:
                commands=command.split(",")

            if "Translation" in commands and str(reader[1][countryIndex]).lower() not in ["de","fr","es","jp","cn","it"]:
                print("Wrong Translation")
                Log_writer("OCR_generic_error.log", cat_id, "Translation validation:"+str(reader[1][countryIndex]).lower()+" wrong translation format", "40","Before Start,Failed")
                sys.exit(0)

            matched_result = list(set(commands) & set(["Asin-Info-Process", "Asin-B", "Asin-M", "Asin-P", "Asin-Title","Asin-IDM"]))
            print(matched_result)
            AsinExcludecommands=list(set(commands)-set(matched_result))

            #Asin Info run for different case like AsinInfo Brand,AsinInfo Manufacture,AsinInfo ProductGroup,etc
            if len(matched_result) > 0:
                print("Entered Into AsinIno ....")
                asin_info_cmd = TLD_ContryCode_mapping("AsinInfoExtractor.py ", reader[1][countryIndex], row[2], cat_id)
                asin_info_cmd = asin_info_cmd + " " + str(IsFulltitleCase) + "  '" + str(matched_result) + "'  start-single"
                print(asin_info_cmd)
                Log_writer("OCR_generic_error.log", cat_id, "AsinInfo Started", str(AsinInfoStarted), "AsinInfoStarted")
                Start_Process(asin_info_cmd)

            for command in commands:
                if command=="Keyword-Extraction" and len(matched_result)>0:
                    if os.path.exists(CurrentPath()+"/Catalog_files/"+str(row[2]).replace('.csv', '_p0.csv')):
                        if not os.path.exists(CurrentPath() + "/Catalog_files/" + str(cat_id) + "-Config.xlsx"):
                            create_config(True, CurrentPath() + "/Catalog_files/" + str(row[2]), cat_id)

                        title_classification_cmd = "ProductClassifier.py  " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv') + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Title Classification Started",str(TitleClassificationStarted), "Title Classification Started")
                        Start_Process(title_classification_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Title Classification Completed",str(TitleClassificationCompleted), "Title Classification Completed")

                        keyword_extraction_cmd="KeywordExtractor.py "+CurrentPath()+"/Catalog_files/"+str(row[2]).replace('.csv', '_p1.csv')+ " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Keyword Extraction Started", str(KeywordExtractionStarted),"Keyword Extraction Started")
                        Start_Process(keyword_extraction_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "KeywordExtraction Completed", str(KeywordExtractionCompleted),"KeywordExtraction Completed")
                    else:
                        e=str(str(row[2]).replace('.csv', '_p0.csv'))+" File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(KeywordExtractionFailed), "Keyword Extraction Failed")
                        sys.exit(0)

                elif command=="Keyword-Extraction" and len(matched_result)==0:
                    if os.path.exists(CurrentPath()+"/Catalog_files/"+str(row[2])):
                        if not os.path.exists(CurrentPath() + "/Catalog_files/" + str(cat_id) + "-Config.xlsx"):
                            create_config(True, CurrentPath() + "/Catalog_files/" + str(row[2]), cat_id)
                        title_classification_cmd = "ProductClassifier.py" \
                                                   " " + CurrentPath() + "/Catalog_files/" + str(row[2]) + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Title Classification Started",str(TitleClassificationStarted), "Title Classification Started")
                        Start_Process(title_classification_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Title Classification Completed",str(TitleClassificationCompleted), "Title Classification Completed")

                        # keyword_extraction_cmd="KeywordExtractor.py "+CurrentPath()+"/Catalog_files/"+str(row[2])+ " start-single"
                        keyword_extraction_cmd = "KeywordExtractor.py " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv') + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Keyword Extraction Started", str(KeywordExtractionStarted),"Keyword Extraction Started")
                        Start_Process(keyword_extraction_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "KeywordExtraction Completed", str(KeywordExtractionCompleted),"KeywordExtraction Completed")
                    else:
                        e = str(row[2]) + " File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(KeywordExtractionFailed), "Keyword Extraction Failed")
                        sys.exit(0)

                elif command == "Translation" and "Title-Grouping" in commands :
                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p2.csv')):
                        translation_cmd =TLD_ContryCode_mapping("GooogleAPITranslator.py ",reader[1][countryIndex], str(row[2]).replace('.csv', '_p2.csv'),cat_id)
                        translation_cmd = translation_cmd + " " + str(IsFullTranslation) + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Translation Started", str(TranslationStarted), "TranslationStarted")
                        Start_Process(translation_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Translation Completed", str(TranslationCompleted),"TranslationCompleted")
                    else:
                        e =  str(row[2]).replace('.csv', '_p2.csv') + " File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TranslationFailed),"TranslationFailed")
                        sys.exit(0)

                elif command == "Translation" and "Title-Classification" in commands:
                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv')):
                        translation_cmd = TLD_ContryCode_mapping("GooogleAPITranslator.py ",reader[1][countryIndex],str(row[2]).replace('.csv', '_p1.csv'), cat_id)
                        translation_cmd = translation_cmd+" "+str(IsFullTranslation)+" start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Translation Started", str(TranslationStarted), "TranslationStarted")
                        Start_Process(translation_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Translation Completed", str(TranslationCompleted),"TranslationCompleted")
                    else:
                        e = str(row[2]).replace('.csv', '_p1.csv') + " File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TranslationFailed),"TranslationFailed")
                        sys.exit(0)

                elif command == "Translation" and len(matched_result)>0:
                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv')):
                        translation_cmd = TLD_ContryCode_mapping("GooogleAPITranslator.py ",reader[1][countryIndex],str(row[2]).replace('.csv', '_p0.csv'), cat_id)
                        translation_cmd = translation_cmd + " " + str(IsFullTranslation) + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Translation Started", str(TranslationStarted), "TranslationStarted")
                        Start_Process(translation_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Translation Completed", str(TranslationCompleted),"TranslationCompleted")
                    else:
                        e = str(row[2]).replace('.csv', '_p0.csv') + " File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TranslationFailed),"TranslationFailed")
                        sys.exit(0)

                elif command == "Translation" and len(matched_result) == 0:
                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2])):
                        translation_cmd = TLD_ContryCode_mapping("GooogleAPITranslator.py ",reader[1][countryIndex],str(row[2]), cat_id)
                        translation_cmd = translation_cmd + " " + str(IsFullTranslation) + " start-single"
                        continue
                        Log_writer("OCR_generic_error.log", cat_id, "Translation Started", str(TranslationStarted), "TranslationStarted")
                        Start_Process(translation_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Translation Completed", str(TranslationCompleted),"TranslationCompleted")
                    else:
                        e = str(row[2])+ " File Not exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TranslationFailed),"TranslationFailed")
                        sys.exit(0)
                    # continue
                    # Start_Process(asin_info_cmd)
                elif command=="Title-Classification" and len(matched_result)>0:
                    if not os.path.exists(CurrentPath() + "/Catalog_files/" +str(cat_id)+"-Config.xlsx"):
                        #create_config(True, str(row[2]), cat_id)
                        create_config(True, CurrentPath() + "/Catalog_files/"+str(row[2]), cat_id)

                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv')):
                        title_classification_cmd = "ProductClassifier.py" \
                                                   " " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv','_p0.csv')+ " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Title Classification Started", str(TitleClassificationStarted),"Title Classification Started")
                        Start_Process(title_classification_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Title Classification Completed", str(TitleClassificationCompleted),"Title Classification Completed")
                    else:
                        e = str(row[2]).replace('.csv', '_p0.csv') + " Not Exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TitleClassificationFailed),"Title Classification Failed")
                        sys.exit(0)

                elif command=="Title-Classification" and len(matched_result)==0:
                    if not os.path.exists(CurrentPath() + "/Catalog_files/" +str(cat_id)+"-Config.xlsx"):
                        create_config(True, CurrentPath() + "/Catalog_files/"+str(row[2]), cat_id)

                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2])):
                        title_classification_cmd = "ProductClassifier.py" \
                                                   " " + CurrentPath() + "/Catalog_files/" + str(row[2])+ " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Title Classification Started", str(TitleClassificationStarted),"Title Classification Started")
                        Start_Process(title_classification_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Title Classification Completed", str(TitleClassificationCompleted),"Title Classification Completed")
                    else:
                        e = str(row[2])+" Not Exists"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TitleClassificationFailed), "Title Classification Failed")
                        sys.exit(0)

                elif command=="Title-Grouping":
                    if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv')):
                        title_grouping_cmd = "ProductGrouping.py " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv') + " start-single"
                        Log_writer("OCR_generic_error.log", cat_id, "Title Grouping Started", str(TitleGroupingStarted),"TitleGroupingStarted")
                        Start_Process(title_grouping_cmd)
                        # Log_writer("OCR_generic_error.log", cat_id, "Title Grouping Completed", str(TitleGroupingCompleted),"TitleGroupingCompleted")
                    else:
                        e =str(row[2]).replace('.csv', '_p1.csv')+" Not exits"
                        Log_writer("OCR_generic_error.log", cat_id, e, str(TitleGroupingFailed), "TitleGroupingFailed")
                        sys.exit(0)
                elif command in ["Asin-Info-Process", "Asin-B", "Asin-M", "Asin-P", "Asin-Title","Asin-IDM"]:
                    print(command)
                    continue
                else:
                    print(command)
                    print("Wrong Single Step Process Commmand")
                    Log_writer("OCR_generic_error.log", cat_id, "Wrong Keyword was passed", "22", "Wrong Keyword was passed")
                continue

            # classification_matched_result = set(commands) & set(["Title-Classification"])
            # group_matched_result = set(commands) & set(["Title-Grouping"])
            importFlag=True
            if Isdbimport and len(commands) == 0 and importFlag:
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2])):
                    #raw_file=re.sub(r'_p\d+|_pt', '', str(row[2]), re.I)
                    db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(row[2]) + "  " + CurrentPath() + "/Catalog_files/" + str(row[2]) +" "+str(Force_Import)
                    Log_writer("OCR_generic_error.log", cat_id, "DBImport Started", str(DBImportStarted), "DBImportStarted")
                    Start_Process(db_import_cmd)
                    # Log_writer("OCR_generic_error.log", cat_id, "DBImport Completed", str(DBImportCompleted), "DBImportCompleted")
                    importFlag=False
                else:
                    e = "Mentioned catalog file not exists to import for the catalog ID " + str(cat_id)
                    Log_writer("OCR_generic_error.log", cat_id, e, str(DBImportFailed), "DBImport Failed")
                    sys.exit(0)

            elif Isdbimport and "Translation" in commands and importFlag:
                outputfile = re.sub(r"_p\d+.csv", "_pt.csv", str(row[2]))
                if str(row[2]) == outputfile:
                    outputfile = re.sub(".csv", "_pt.csv", str(row[2]))
                else:
                    pass
                print(outputfile)
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(outputfile)):
                    if IsFullTranslation:
                        db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(outputfile) + "  " + CurrentPath() + "/Catalog_files/" + str(outputfile)
                    else:
                        db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(row[2]) + "  " + CurrentPath() + "/Catalog_files/" + str(outputfile)
                    Log_writer("OCR_generic_error.log", cat_id, "DBImport Started", str(DBImportStarted), "DBImportStarted")
                    Start_Process(db_import_cmd)
                    # Log_writer("OCR_generic_error.log", cat_id, "DBImport Completed", str(DBImportCompleted), "DBImportCompleted")
                    importFlag = False
                else:
                    e = str(str(row[2]).replace('.csv', '_pt.csv')) + " not exists"
                    Log_writer("OCR_generic_error.log", cat_id, e, str(DBImportFailed), "DBImportFailed")
                    sys.exit(0)

            elif Isdbimport and "Title-Grouping" in commands and importFlag:
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p2.csv')):
                    db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(row[2]) + "  " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv','_p2.csv')
                    Log_writer("OCR_generic_error.log", cat_id, "DBImport Started", str(DBImportStarted),"DBImportStarted")
                    Start_Process(db_import_cmd)
                    # Log_writer("OCR_generic_error.log", cat_id, "DBImport Completed", str(DBImportCompleted),"DBImportCompleted")
                    importFlag = False
                else:
                    e = str(str(row[2]).replace('.csv', '_p2.csv')) + " not exists"
                    Log_writer("OCR_generic_error.log", cat_id, e, str(DBImportFailed), "DBImportFailed")
                    sys.exit(0)

            elif Isdbimport and "Title-Classification" in commands and importFlag:
                print("Title Classification and DB import ")
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv')):
                    db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(row[2]) + "  " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv','_p1.csv')
                    Log_writer("OCR_generic_error.log", cat_id, "DBImport Started", str(DBImportStarted),"DBImportStarted")
                    Start_Process(db_import_cmd)
                    # Log_writer("OCR_generic_error.log", cat_id, "DBImport Completed", str(DBImportCompleted),"DBImportCompleted")
                    importFlag = False
                else:
                    e = str(str(row[2]).replace('.csv', '_p1_pt.csv')) + " not exists"
                    Log_writer("OCR_generic_error.log", cat_id, e, str(DBImportFailed), "DBImportFailed")
                    sys.exit(0)

            elif Isdbimport and set(["Asin-Info-Process", "Asin-B", "Asin-M", "Asin-P", "Asin-Title"]) >= set(matched_result) and len(matched_result) != 0 and importFlag:
                print("Asin Info and DB import")
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv')):
                    db_import_cmd = "CatalogImporter.py " + CurrentPath() + "/Catalog_files/" + str(row[2]) + "  " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv','_p0.csv')
                    Log_writer("OCR_generic_error.log", cat_id, "DBImport Started", str(DBImportStarted),"DBImportStarted")
                    Start_Process(db_import_cmd)
                    # Log_writer("OCR_generic_error.log", cat_id, "DBImport Completed", str(DBImportCompleted),"DBImportCompleted")
                    importFlag = False
                else:
                    e = str(str(row[2]).replace('.csv', '_p0.csv')) + " not exists"
                    Log_writer("OCR_generic_error.log", cat_id, e, str(DBImportFailed), "DBImportFailed")
                    sys.exit(0)
        else:
            script_name="AsinInfoExtractor.py "
            start_cmd=TLD_ContryCode_mapping(script_name,reader[1][countryIndex],row[2],cat_id)
            DB_update(cat_id, "1",'',"Asin Info process is started")
            Start_Process(str(start_cmd)+" "+str(IsFulltitleCase))

    autoSteps="select [AutomationSteps] from [Catalog_Automation_Status] where Catalog_Id=" + str(cat_id)
    cursor.execute(autoSteps)
    steps = cursor.fetchone()[0]
    connection.commit()

    sourceCountQuery = "select [Catalog_Status] from [Catalog_Automation_Status] where Catalog_Id="+str(cat_id)
    # print(sourceCountQuery)
    cursor.execute(sourceCountQuery)
    count_failed = cursor.fetchone()[0]
    print(count_failed)
    connection.commit()
    
    for key in r.scan_iter(str(cat_id) + "*"):
        r.delete(key)

    if int(count_failed) > 0 and not int(count_failed)==13 :
        # Deleting total keys from redis
        print(es.indices.exists(index="asininfo_cache"))
        if es.indices.exists(index="asininfo_cache"):
            es.indices.delete(index='asininfo_cache')
        completePercentag = "update [Catalog_Automation_Status] set Completed_percentage=100  where Catalog_Id=" + str(cat_id)
        cursor.execute(completePercentag)
        connection.commit()
        email_html = Email_Html(steps.split(","), cat_id)
        email_sender(email_html, "Overall Execution for Catalog " + str(cat_id), False)
    if int(count_failed)==13:
        NRCountQuery = "select count(*) from [" + str(cat_id) + "_Output]"
        cursor.execute(NRCountQuery)
        NRcount = cursor.fetchone()[0]
        connection.commit()

        sourceCountQuery = "select count(*) from [" + str(cat_id) + "_Input]"
        cursor.execute(sourceCountQuery)
        count = cursor.fetchone()[0]
        connection.commit()
        TEXT = "Hello everyone,\n"
        TEXT = TEXT + "\n"
        TEXT = TEXT + "Catalog " + str(cat_id) + ", loaded into Progen Tool successfully.\n Total NR count :" + str(
            NRcount) + ".\n Total Source count :" + str(count) + ".\n"
        TEXT = TEXT + "\n"
        TEXT = TEXT + "Thanks,\n"
        TEXT = TEXT + "OCR IT Team"
        
        email_sender(TEXT,"Database Stats for Catalog "+str(cat_id))
    # connection.close()
