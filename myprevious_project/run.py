import sys,pymssql,csv,os,datetime,re
import pandas as pd
import subprocess
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import imp

# DB_Create = imp.load_source('DB_Create', '/home/merit/OCR/AsinINfo/DB_Create.py')
# from DB_Create import DB_connection
# connection = pymssql.connect(host='CH1DEVBD01', user='User2', password='Merit456', database='OCR_Staging')
connection = pymssql.connect(host='CH1025BD03', user='User2', password='Merit456', database='OCR')
cursor= connection.cursor()

def CurrentPath():
    process = subprocess.Popen(['pwd'], stdout=subprocess.PIPE)
    out, err = process.communicate()
    current_path = out.decode("utf-8")
    current_path = re.sub("\s*", "", str(current_path))
    print(current_path)
    return (str(current_path))

def Automated_table_creation():
    try:
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

    catalog_status_query = "update Catalog_Automation_Status set Catalog_Status=" + catalog_status + ",steps='"+steps+"' where Catalog_Id=" + str(cat_id)
    print(catalog_status_query)
    cursor.execute(catalog_status_query)
    connection.commit()
    print("Data inserted inside Catalog_Automation_Status")

    current_time = str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    log=str(str(current_time) + " - " + str(cat_id) + " - " + str(error_log)) + "\n"
    log = re.sub("\'", "", str(log))
    log_query="insert into OCR_Log_Tracker values("+str(cat_id)+", '"+str(log)+"')"
    print("----------------")
    print(log_query)
    print("----------------")
    cursor.execute(log_query)
    connection.commit()

    print("Logs inserted inside OCR_Log_Tracker")
    with open(CurrentPath()+"/Logfiles/"+logfile_name, "a") as log_filehandler:
        log_filehandler.write(log)
    return

def Start_Process(script_with_args):
    start_cmd="/usr/bin/python3  " +CurrentPath()+"/"+script_with_args
    print("***********************")
    print(start_cmd)
    print("***********************")
    os.system(start_cmd)
    return

def create_config(flag,raw_filename,cat_id):
    if flag:
        #create config file
        config_create_cmd="config_creation.py "+raw_filename
        Start_Process(config_create_cmd)
        DB_update(cat_id, "3","Config file is created")
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

def DB_update(cat_id,updated_status,steps):
    catalog_status_query = "update Catalog_Automation_Status set Catalog_Status=" + updated_status + ",steps='"+steps+"' where Catalog_Id=" + str(cat_id)
    print("***********************")
    print(catalog_status_query)
    print("***********************")
    cursor.execute(catalog_status_query)
    connection.commit()
    return
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

def email_sender():
    login = 'tamilvanan.periyasamy@meritgroup.co.uk'
    password = 'mksimple'
    sender = 'Hai'
    receivers = ['tamperdroid@gmail.com']

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(receivers)
    msg['Subject'] = "Test Message"

    # Simple text message or HTML
    TEXT = "Hello everyone,\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Important message.\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Thanks,\n"
    TEXT = TEXT + "SMTP Robot"

    msg.attach(MIMEText(TEXT))

    filenames = [""]
    for file in filenames:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(file, 'rb').read())
        encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"'
                        % os.path.basename(file))
        msg.attach(part)

    smtpObj = smtplib.SMTP('smtp.gmail.com:587')
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(login, password)
    smtpObj.sendmail(sender, receivers, msg.as_string())


if __name__ == "__main__":
    Automated_table_creation()
    startQuery="select * from Catalog_Automation_Status where Catalog_Status=0"
    cursor.execute(startQuery)
    result=cursor.fetchall()
    if len(result)==0:
        e="Please set Catalog_Status to 0 in Catalog_Automation_Status table"
        print(e)
        # Log_writer("OCR_generic_error.log", "", e, "-1", "Asin Info process is failed before start")
        sys.exit(1)

    print(result)
    start_cmd=""
    # run_script="OCR_Asininfo_Org.py "
    print(len(result))
    for row in result:
        filepath = CurrentPath() + "/Catalog_files/" + row[2]
        try:
            cat_id=row[1]
            print(filepath)
            df = pd.read_csv(filepath, encoding='latin1')
        except FileNotFoundError as e:
            print("FileNotFoundError :",e)
            Log_writer("OCR_generic_error.log",cat_id, e,"-1","Asin Info process is failed before start")
            continue

        inputHeader = (list(df.columns.values))
        countryIndex = inputHeader.index("TLD")
        f = open(filepath, 'rt', encoding="ISO-8859-1")
        reader =  list(csv.reader(f, delimiter=','))

        print(reader[1][countryIndex])

        command = row[6]
        if command!='N/A':
            if command=="Keyword-Extraction":
                print("keyword-extraction")
                if os.path.exists(CurrentPath()+"/Catalog_files/"+str(row[2]).replace('.csv', '_p1.csv')):
                    keyword_extraction_cmd="keyword_extraction.py "+CurrentPath()+"/Catalog_files/"+str(row[2]).replace('.csv', '_p1.csv')+ " start-single"
                    Start_Process(keyword_extraction_cmd)
                else:
                    e="P1 file not exists for the catalog ID "+cat_id
                    Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Single Prcess Failure")
            elif command == "Translation":
                translation_cmd=""
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv')):
                    translation_cmd =TLD_ContryCode_mapping("translate.py ",reader[1][countryIndex], str(row[2]).replace('.csv', '_p0.csv'),cat_id)
                    translation_cmd = translation_cmd + " start-single"
                    Start_Process(translation_cmd)
                else:
                    e = "P0 file not exists for the catalog ID " + cat_id
                    Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Single Prcess Failure")
            elif command =="Asin-Info-Process":
                print("Asin-Info-Process")
                asin_info_cmd = TLD_ContryCode_mapping("OCR_Asininfo_Org.py ", reader[1][countryIndex], row[2], cat_id)
                Start_Process(asin_info_cmd)
            elif command=="Title-Classification":
                print("Title Classification")
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p0.csv')):
                    title_classification_cmd = "data_parser.py " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv','_p0.csv')+ " start-single"
                    Start_Process(title_classification_cmd)
                else:
                    e = "P0 file not exists for the catalog ID " + cat_id
                    Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Single Prcess Failure")
            elif command=="Title-Grouping":
                if os.path.exists(CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv')):
                    title_grouping_cmd = "search_clustering.py " + CurrentPath() + "/Catalog_files/" + str(row[2]).replace('.csv', '_p1.csv') + " start-single"
                    Start_Process(title_grouping_cmd)
                else:
                    e = "P1 file not exists for the catalog ID " + cat_id
                    Log_writer("OCR_generic_error.log", cat_id, e, "-1", "Single Prcess Failure")

            else:
                print("Wrong Single Step Process Commmand")
            continue
        script_name="OCR_Asininfo_Org.py "
        start_cmd=TLD_ContryCode_mapping(script_name,reader[1][countryIndex],row[2],cat_id)
        DB_update(cat_id, "1","Asin Info process is started")
        Start_Process(start_cmd)
        email_sender()
    connection.close()
