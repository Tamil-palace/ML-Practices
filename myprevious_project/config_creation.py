# -*- coding: utf-8 -*-
import re, sys, os
import csv
import xlwt

try:
   file=sys.argv[1]
except Exception as e:
   print("Please Enter the FileName")
   Log_writer("OCR_ErrorLog_" + cNumber + ".log", cNumber, e, "-3","Config File creation Error")
   sys.exit(0)

# for fList in fileList:

#     fileName = 'D:\\MuthuBabu\\OCR\\New Catalog\\2017\\oct\\26102017\\new catalog\\muthu source\\'+str(fList)
fileName = '/home/merit/OCR/AsinINfo/Catalog_files/' + str(file)
head, base = os.path.split(fileName)

cNumber=re.findall(r"^([\d]+)",str(base))[0]
print ("base",cNumber)
f = open(fileName, 'rt', encoding="ISO-8859-1")
reader = csv.reader(f, delimiter=',')

configName=str(head) + "/"+str(cNumber)+"-Config.xlsx"
print(configName)
workbook = xlwt.Workbook()
sheet1 = workbook.add_sheet("Exclude")
sheet1.write(0, 0, 'Keywords - Please enter all items to be excluded')
sheet2 = workbook.add_sheet("Include")
sheet2.write(0, 0, 'Include')
sheet3 = workbook.add_sheet("N-Nontrack")
sheet3.write(0, 0, 'KEYWORD')
sheet4 = workbook.add_sheet("TrackItemExclude")
sheet4.write(0, 0, 'KEYWORD')
sheet5 = workbook.add_sheet("Header")
sheet5.write(0, 0, 'Headers')
rowinc=1
for i,val in enumerate(next(reader)):
    if i>6:
        print (val)
        sheet5.write(rowinc,0,str(val))
        rowinc += 1

workbook.save(configName)
