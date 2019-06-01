#-*- coding: utf-8 -*-
import pandas as pd
import sys,re
import datetime
from datetime import datetime, timedelta, date
import time
import os
import imp
import requests
import csv
import html
import ftfy
import threading
import multiprocessing
from joblib import Parallel, delayed
from fuzzywuzzy import fuzz
from elasticsearch import Elasticsearch
import elasticsearch
import ast,redis
import configparser

now = datetime.now()

dir = os.path.dirname(os.path.abspath(__file__))
AutomationController = imp.load_source('AutomationController', dir+'/AutomationController.py')

from AutomationController import Log_writer
from AutomationController import email_sender

r = redis.StrictRedis()
config = configparser.ConfigParser()
config.read('Config.ini')

TranslationStarted=config.get("Status", "Translation-Started")
TranslationFailed=config.get("Status", "Translation-Failed")
TranslationCompleted=config.get("Status", "Translation-Completed")

host=config.get("Elastic-Search","host")
port=config.get("Elastic-Search","port")

translation_index="translation"
es = Elasticsearch([{'host': host, 'port': port}])

head, base = os.path.split(sys.argv[1])
cNumber=re.findall(r"^([\d]+)",str(base))[0]
try:
   date=re.findall(r"Catalog\-([\d]+)\_",str(base))[0]
except:
   date = re.findall(r"Catalog\-([\d]+)\.", str(base))[0]

r.set(str(cNumber)+"_"+str(date)+"_4_progress", 0)
r.set(str(cNumber)+"_"+str(date)+"_4_total", 0)

def special_to_normal(value):
    print("special_to_normal function called")
    value = re.sub(r"\\xe2\\x80\\x90", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x91", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x92", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x93", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x94", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x95", "-", str(value))
    value = re.sub(r"\\xe2\\x80\\x96", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x97", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x98", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x99", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9a", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9b", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9c", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9d", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9e", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\x9f", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa0", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa1", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa2", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa3", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa4", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa5", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa6", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xa7", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb0", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb1", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb2", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb3", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb4", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb5", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb6", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb7", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb8", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xb9", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xba", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xbb", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xbc", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xbd", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xbe", "'", str(value))
    value = re.sub(r"\\xe2\\x80\\xbf", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x80", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x81", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x82", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x83", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x84", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x85", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x86", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x87", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x88", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x89", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8a", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8b", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8c", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8d", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8e", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x8f", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x90", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x91", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x92", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x93", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x94", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x95", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x96", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x97", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x98", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x99", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9a", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9b", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9c", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9d", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9e", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\x9f", "", str(value))
    value = re.sub(r"\\xe2\\x81\\xa0⁠", "", str(value))
    value = re.sub(r"\\xe2\\x81\\xa5", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xa6", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xa7", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xa8", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xa9", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb0", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb1", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb2", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb3", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb4", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb5", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb6", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb7", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb8", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xb9", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xba", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbb", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbc", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbd", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbe", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbf", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbe", "'", str(value))
    value = re.sub(r"\\xe2\\x81\\xbf", "'", str(value))
    value = re.sub(r'\\xc2\\xa0', ' ', str(value))
    value = re.sub(r"\\xc2\\xa1", "¡", str(value))
    value = re.sub(r"\\xc2\\xa2", "¢", str(value))
    value = re.sub(r"\\xc2\\xa3", "£", str(value))
    value = re.sub(r"\\xc2\\xa4", "¤", str(value))
    value = re.sub(r"\\xc2\\xa5", "¥", str(value))
    value = re.sub(r"\\xc2\\xa6", "¦", str(value))
    value = re.sub(r"\\xc2\\xa7", "§", str(value))
    value = re.sub(r"\\xc2\\xa8", "¨", str(value))
    value = re.sub(r"\\xc2\\xa9", "©", str(value))
    value = re.sub(r"\\xc2\\xaa", "ª", str(value))
    value = re.sub(r"\\xc2\\xab", "«", str(value))
    value = re.sub(r"\\xc2\\xac", "¬", str(value))
    value = re.sub(r"\\xc2\\xad", "", str(value))
    value = re.sub(r"\\xc2\\xae", "®", str(value))
    value = re.sub(r"\\xc2\\xaf", "¯", str(value))
    value = re.sub(r"\\xc2\\xb0", "°", str(value))
    value = re.sub(r"\\xc2\\xb1", "±", str(value))
    value = re.sub(r"\\xc2\\xb2", "²", str(value))
    value = re.sub(r"\\xc2\\xb3", "³", str(value))
    value = re.sub(r"\\xc2\\xb4", "´", str(value))
    value = re.sub(r"\\xc2\\xb5", "µ", str(value))
    value = re.sub(r"\\xc2\\xb6", "¶", str(value))
    value = re.sub(r"\\xc2\\xb7", "·", str(value))
    value = re.sub(r"\\xc2\\xb8", "¸", str(value))
    value = re.sub(r"\\xc2\\xb9", "¹", str(value))
    value = re.sub(r"\\xc2\\xba", "º", str(value))
    value = re.sub(r"\\xc2\\xbb", "»", str(value))
    value = re.sub(r"\\xc2\\xbc", "¼", str(value))
    value = re.sub(r"\\xc2\\xbd", "½", str(value))
    value = re.sub(r"\\xc2\\xbe", "¾", str(value))
    value = re.sub(r"\\xc2\\xbf", "¿", str(value))
    value = re.sub(r'\\xc3\\x80','A',str(value))
    value = re.sub(r'\\xc3\\x81','A',str(value))
    value = re.sub(r'\\xc3\\x82','A',str(value))
    value = re.sub(r'\\xc3\\x83','A',str(value))
    value = re.sub(r'\\xc3\\x84','A',str(value))
    value = re.sub(r'\\xc3\\x85','A',str(value))
    value = re.sub(r'\\xc3\\x86','A',str(value))
    value = re.sub(r'\\xc3\\x87','C',str(value))
    value = re.sub(r'\\xc3\\x88','E',str(value))
    value = re.sub(r'\\xc3\\x89','E',str(value))
    value = re.sub(r'\\xc3\\x8a','E',str(value))
    value = re.sub(r'\\xc3\\x8b','E',str(value))
    value = re.sub(r'\\xc3\\x8c','I',str(value))
    value = re.sub(r'\\xc3\\x8d','I',str(value))
    value = re.sub(r'\\xc3\\x8e','I',str(value))
    value = re.sub(r'\\xc3\\x8f','I',str(value))
    value = re.sub(r'\\xc3\\x90','D',str(value))
    value = re.sub(r'\\xc3\\x91','N',str(value))
    value = re.sub(r'\\xc3\\x92','O',str(value))
    value = re.sub(r'\\xc3\\x93','O',str(value))
    value = re.sub(r'\\xc3\\x94','O',str(value))
    value = re.sub(r'\\xc3\\x95','O',str(value))
    value = re.sub(r'\\xc3\\x96','O',str(value))
    value = re.sub(r'\\xc3\\x98','O',str(value))
    value = re.sub(r'\\xc3\\x99','U',str(value))
    value = re.sub(r'\\xc3\\x9a','U',str(value))
    value = re.sub(r'\\xc3\\x9b','U',str(value))
    value = re.sub(r'\\xc3\\x9c','U',str(value))
    value = re.sub(r'\\xc3\\x9d','Y',str(value))
    value = re.sub(r'\\xc3\\x9e','DE',str(value))
    value = re.sub(r'\\xc3\\x9f','S',str(value))
    value = re.sub(r'\\xc3\\xa0','a',str(value))
    value = re.sub(r'\\xc3\\xa1','a',str(value))
    value = re.sub(r'\\xc3\\xa2','a',str(value))
    value = re.sub(r'\\xc3\\xa3','a',str(value))
    value = re.sub(r'\\xc3\\xa4','a',str(value))
    value = re.sub(r'\\xc3\\xa5','a',str(value))
    value = re.sub(r'\\xc3\\xa6','ae',str(value))
    value = re.sub(r'\\xc3\\xa7','c',str(value))
    value = re.sub(r'\\xc3\\xa7','c',str(value))
    value = re.sub(r'\\xc3\\xa8','e',str(value))
    value = re.sub(r'\\xc3\\xa9','e',str(value))
    value = re.sub(r'\\xc3\\xaa','e',str(value))
    value = re.sub(r'\\xc3\\xab','e',str(value))
    value = re.sub(r'\\xc3\\xac','i',str(value))
    value = re.sub(r'\\xc3\\xad','i',str(value))
    value = re.sub(r'\\xc3\\xae','i',str(value))
    value = re.sub(r'\\xc3\\xaf','i',str(value))
    value = re.sub(r'\\xc3\\xb0','o',str(value))
    value = re.sub(r'\\xc3\\xb1','n',str(value))
    value = re.sub(r'\\xc3\\xb2','o',str(value))
    value = re.sub(r'\\xc3\\xb3','o',str(value))
    value = re.sub(r'\\xc3\\xb4','o',str(value))
    value = re.sub(r'\\xc3\\xb5','o',str(value))
    value = re.sub(r'\\xc3\\xb6','o',str(value))
    value = re.sub(r'\\xc3\\xb8','o',str(value))
    value = re.sub(r'\\xc3\\xb9','u',str(value))
    value = re.sub(r'\\xc3\\xba','u',str(value))
    value = re.sub(r'\\xc3\\xbb','u',str(value))
    value = re.sub(r'\\xc3\\xbc','u',str(value))
    value = re.sub(r'\\xc3\\xbd','y',str(value))
    value = re.sub(r'\\xc3\\xbe','fe',str(value))
    value = re.sub(r'\\xc3\\xbf','y',str(value))
    value = re.sub(r'\\xc4\\x80','A',str(value))
    value = re.sub(r'\\xc4\\x81','a',str(value))
    value = re.sub(r'\\xc4\\x82','A',str(value))
    value = re.sub(r'\\xc4\\x83','a',str(value))
    value = re.sub(r'\\xc4\\x84','A',str(value))
    value = re.sub(r'\\xc4\\x85','a',str(value))
    value = re.sub(r'\\xc4\\x86','C',str(value))
    value = re.sub(r'\\xc4\\x87','c',str(value))
    value = re.sub(r'\\xc4\\x88','C',str(value))
    value = re.sub(r'\\xc4\\x89','c',str(value))
    value = re.sub(r'\\xc4\\x8a','C',str(value))
    value = re.sub(r'\\xc4\\x8b','c',str(value))
    value = re.sub(r'\\xc4\\x8c','C',str(value))
    value = re.sub(r'\\xc4\\x8d','c',str(value))
    value = re.sub(r'\\xc4\\x8e','D',str(value))
    value = re.sub(r'\\xc4\\x8f','d',str(value))
    value = re.sub(r'\\xc4\\x90','D',str(value))
    value = re.sub(r'\\xc4\\x91','d',str(value))
    value = re.sub(r'\\xc4\\x92','E',str(value))
    value = re.sub(r'\\xc4\\x93','e',str(value))
    value = re.sub(r'\\xc4\\x94','E',str(value))
    value = re.sub(r'\\xc4\\x95','e',str(value))
    value = re.sub(r'\\xc4\\x96','E',str(value))
    value = re.sub(r'\\xc4\\x97','e',str(value))
    value = re.sub(r'\\xc4\\x98','E',str(value))
    value = re.sub(r'\\xc4\\x99','e',str(value))
    value = re.sub(r'\\xc4\\x9a','E',str(value))
    value = re.sub(r'\\xc4\\x9b','e',str(value))
    value = re.sub(r'\\xc4\\x9c','G',str(value))
    value = re.sub(r'\\xc4\\x9d','g',str(value))
    value = re.sub(r'\\xc4\\x9e','G',str(value))
    value = re.sub(r'\\xc4\\x9f','g',str(value))
    value = re.sub(r'\\xc4\\xa0','G',str(value))
    value = re.sub(r'\\xc4\\xa1','g',str(value))
    value = re.sub(r'\\xc4\\xa2','G',str(value))
    value = re.sub(r'\\xc4\\xa3','g',str(value))
    value = re.sub(r'\\xc4\\xa4','H',str(value))
    value = re.sub(r'\\xc4\\xa5','h',str(value))
    value = re.sub(r'\\xc4\\xa6','H',str(value))
    value = re.sub(r'\\xc4\\xa7','h',str(value))
    value = re.sub(r'\\xc4\\xa8','I',str(value))
    value = re.sub(r'\\xc4\\xa9','i',str(value))
    value = re.sub(r'\\xc4\\xaa','I',str(value))
    value = re.sub(r'\\xc4\\xab','i',str(value))
    value = re.sub(r'\\xc4\\xac','I',str(value))
    value = re.sub(r'\\xc4\\xad','i',str(value))
    value = re.sub(r'\\xc4\\xae','I',str(value))
    value = re.sub(r'\\xc4\\xaf','i',str(value))
    value = re.sub(r'\\xc4\\xb0','I',str(value))
    value = re.sub(r'\\xc4\\xb1','i',str(value))
    value = re.sub(r'\\xc4\\xb2','IJ',str(value))
    value = re.sub(r'\\xc4\\xb3','ij',str(value))
    value = re.sub(r'\\xc4\\xb4','J',str(value))
    value = re.sub(r'\\xc4\\xb5','j',str(value))
    value = re.sub(r'\\xc4\\xb6','K',str(value))
    value = re.sub(r'\\xc4\\xb7','k',str(value))
    value = re.sub(r'\\xc4\\xb8','k',str(value))
    value = re.sub(r'\\xc4\\xb9','L',str(value))
    value = re.sub(r'\\xc4\\xba','l',str(value))
    value = re.sub(r'\\xc4\\xbb','L',str(value))
    value = re.sub(r'\\xc4\\xbc','l',str(value))
    value = re.sub(r'\\xc4\\xbd','L',str(value))
    value = re.sub(r'\\xc4\\xbe','l',str(value))
    value = re.sub(r'\\xc4\\xbf','L',str(value))
    value = re.sub(r'\\xc5\\x80','l',str(value))
    value = re.sub(r'\\xc5\\x81','L',str(value))
    value = re.sub(r'\\xc5\\x82','l',str(value))
    value = re.sub(r'\\xc5\\x83','N',str(value))
    value = re.sub(r'\\xc5\\x84','n',str(value))
    value = re.sub(r'\\xc5\\x85','N',str(value))
    value = re.sub(r'\\xc5\\x86','n',str(value))
    value = re.sub(r'\\xc5\\x87','N',str(value))
    value = re.sub(r'\\xc5\\x88','n',str(value))
    value = re.sub(r'\\xc5\\x89','n',str(value))
    value = re.sub(r'\\xc5\\x8a','n',str(value))
    value = re.sub(r'\\xc5\\x8b','n',str(value))
    value = re.sub(r'\\xc5\\x8c','O',str(value))
    value = re.sub(r'\\xc5\\x8d','o',str(value))
    value = re.sub(r'\\xc5\\x8e','O',str(value))
    value = re.sub(r'\\xc5\\x8f','o',str(value))
    value = re.sub(r'\\xc5\\x90','O',str(value))
    value = re.sub(r'\\xc5\\x91','o',str(value))
    value = re.sub(r'\\xc5\\x92','OE',str(value))
    value = re.sub(r'\\xc5\\x93','oe',str(value))
    value = re.sub(r'\\xc5\\x94','R',str(value))
    value = re.sub(r'\\xc5\\x95','r',str(value))
    value = re.sub(r'\\xc5\\x96','R',str(value))
    value = re.sub(r'\\xc5\\x97','r',str(value))
    value = re.sub(r'\\xc5\\x98','R',str(value))
    value = re.sub(r'\\xc5\\x99','r',str(value))
    value = re.sub(r'\\xc5\\x9a','S',str(value))
    value = re.sub(r'\\xc5\\x9b','s',str(value))
    value = re.sub(r'\\xc5\\x9c','S',str(value))
    value = re.sub(r'\\xc5\\x9d','s',str(value))
    value = re.sub(r'\\xc5\\x9e','S',str(value))
    value = re.sub(r'\\xc5\\x9f','s',str(value))
    value = re.sub(r'\\xc5\\xa0','S',str(value))
    value = re.sub(r'\\xc5\\xa1','s',str(value))
    value = re.sub(r'\\xc5\\xa2','T',str(value))
    value = re.sub(r'\\xc5\\xa3','t',str(value))
    value = re.sub(r'\\xc5\\xa4','T',str(value))
    value = re.sub(r'\\xc5\\xa5','t',str(value))
    value = re.sub(r'\\xc5\\xa6','T',str(value))
    value = re.sub(r'\\xc5\\xa7','t',str(value))
    value = re.sub(r'\\xc5\\xa8','U',str(value))
    value = re.sub(r'\\xc5\\xa9','u',str(value))
    value = re.sub(r'\\xc5\\xaa','U',str(value))
    value = re.sub(r'\\xc5\\xab','u',str(value))
    value = re.sub(r'\\xc5\\xac','U',str(value))
    value = re.sub(r'\\xc5\\xad','u',str(value))
    value = re.sub(r'\\xc5\\xae','U',str(value))
    value = re.sub(r'\\xc5\\xaf','u',str(value))
    value = re.sub(r'\\xc5\\xb0','U',str(value))
    value = re.sub(r'\\xc5\\xb1','u',str(value))
    value = re.sub(r'\\xc5\\xb2','U',str(value))
    value = re.sub(r'\\xc5\\xb3','u',str(value))
    value = re.sub(r'\\xc5\\xb4','W',str(value))
    value = re.sub(r'\\xc5\\xb5','w',str(value))
    value = re.sub(r'\\xc5\\xb6','Y',str(value))
    value = re.sub(r'\\xc5\\xb7','y',str(value))
    value = re.sub(r'\\xc5\\xb8','Y',str(value))
    value = re.sub(r'\\xc5\\xb9','Z',str(value))
    value = re.sub(r'\\xc5\\xba','z',str(value))
    value = re.sub(r'\\xc5\\xbb','Z',str(value))
    value = re.sub(r'\\xc5\\xbc','z',str(value))
    value = re.sub(r'\\xc5\\xbd','Z',str(value))
    value = re.sub(r'\\xc5\\xbe','z',str(value))
    value = re.sub(r'\\xc5\\xbf','S',str(value))
    value = re.sub(r'\\xc6\\x80','b',str(value))
    value = re.sub(r'\\xc6\\x81','B',str(value))
    value = re.sub(r'\\xc6\\x82','b',str(value))
    value = re.sub(r'\\xc6\\x83','b',str(value))
    value = re.sub(r'\\xc6\\x84','b',str(value))
    value = re.sub(r'\\xc6\\x85','b',str(value))
    value = re.sub(r'\\xc6\\x86','O',str(value))
    value = re.sub(r'\\xc6\\x87','C',str(value))
    value = re.sub(r'\\xc6\\x88','c',str(value))
    value = re.sub(r'\\xc6\\x89','D',str(value))
    value = re.sub(r'\\xc6\\x8a','D',str(value))
    value = re.sub(r'\\xc6\\x8b','D',str(value))
    value = re.sub(r'\\xc6\\x8c','d',str(value))
    value = re.sub(r'\\xc6\\x8d','ƍ',str(value))
    value = re.sub(r'\\xc6\\x8e','Ǝ',str(value))
    value = re.sub(r'\\xc6\\x8f','Ə',str(value))
    value = re.sub(r'\\xc6\\x90','Ɛ',str(value))
    value = re.sub(r'\\xc6\\x91','Ƒ',str(value))
    value = re.sub(r'\\xc6\\x92','ƒ',str(value))
    value = re.sub(r'\\xc6\\x93','Ɠ',str(value))
    value = re.sub(r'\\xc6\\x94','Ɣ',str(value))
    value = re.sub(r'\\xc6\\x95','ƕ',str(value))
    value = re.sub(r'\\xc6\\x96','Ɩ',str(value))
    value = re.sub(r'\\xc6\\x97','Ɨ',str(value))
    value = re.sub(r'\\xc6\\x98','Ƙ',str(value))
    value = re.sub(r'\\xc6\\x99','ƙ',str(value))
    value = re.sub(r'\\xc6\\x9a','ƚ',str(value))
    value = re.sub(r'\\xc6\\x9b','ƛ',str(value))
    value = re.sub(r'\\xc6\\x9c','Ɯ',str(value))
    value = re.sub(r'\\xc6\\x9d','Ɲ',str(value))
    value = re.sub(r'\\xc6\\x9e','ƞ',str(value))
    value = re.sub(r'\\xc6\\x9f','Ɵ',str(value))
    value = re.sub(r'\\xc6\\xa0','Ơ',str(value))
    value = re.sub(r'\\xc6\\xa1','ơ',str(value))
    value = re.sub(r'\\xc6\\xa2','Ƣ',str(value))
    value = re.sub(r'\\xc6\\xa3','ƣ',str(value))
    value = re.sub(r'\\xc6\\xa4','Ƥ',str(value))
    value = re.sub(r'\\xc6\\xa5','ƥ',str(value))
    value = re.sub(r'\\xc6\\xa6','Ʀ',str(value))
    value = re.sub(r'\\xc6\\xa7','Ƨ',str(value))
    value = re.sub(r'\\xc6\\xa8','ƨ',str(value))
    value = re.sub(r'\\xc6\\xa9','Ʃ',str(value))
    value = re.sub(r'\\xc6\\xaa','ƪ',str(value))
    value = re.sub(r'\\xc6\\xab','ƫ',str(value))
    value = re.sub(r'\\xc6\\xac','Ƭ',str(value))
    value = re.sub(r'\\xc6\\xad','ƭ',str(value))
    value = re.sub(r'\\xc6\\xae','Ʈ',str(value))
    value = re.sub(r'\\xc6\\xaf','Ư',str(value))
    value = re.sub(r'\\xc6\\xb0','ư',str(value))
    value = re.sub(r'\\xc6\\xb1','Ʊ',str(value))
    value = re.sub(r'\\xc6\\xb2','Ʋ',str(value))
    value = re.sub(r'\\xc6\\xb3','Ƴ',str(value))
    value = re.sub(r'\\xc6\\xb4','ƴ',str(value))
    value = re.sub(r'\\xc6\\xb5','Ƶ',str(value))
    value = re.sub(r'\\xc6\\xb6','ƶ',str(value))
    value = re.sub(r'\\xc6\\xb7','Ʒ',str(value))
    value = re.sub(r'\\xc6\\xb8','Ƹ',str(value))
    value = re.sub(r'\\xc6\\xb9','ƹ',str(value))
    value = re.sub(r'\\xc6\\xba','ƺ',str(value))
    value = re.sub(r'\\xc6\\xbb','ƻ',str(value))
    value = re.sub(r'\\xc6\\xbc','Ƽ',str(value))
    value = re.sub(r'\\xc6\\xbd','ƽ',str(value))
    value = re.sub(r'\\xc6\\xbe','ƾ',str(value))
    value = re.sub(r'\\xc6\\xbf','ƿ',str(value))
    value = re.sub(r'\\xc7\\x80','ǀ',str(value))
    value = re.sub(r'\\xc7\\x81','ǁ',str(value))
    value = re.sub(r'\\xc7\\x82','ǂ',str(value))
    value = re.sub(r'\\xc7\\x83','ǃ',str(value))
    value = re.sub(r'\\xc7\\x84','DZ',str(value))
    value = re.sub(r'\\xc7\\x85','Dz',str(value))
    value = re.sub(r'\\xc7\\x86','dz',str(value))
    value = re.sub(r'\\xc7\\x87','LJ',str(value))
    value = re.sub(r'\\xc7\\x88','Lj',str(value))
    value = re.sub(r'\\xc7\\x89','lj',str(value))
    value = re.sub(r'\\xc7\\x8a','NJ',str(value))
    value = re.sub(r'\\xc7\\x8b','Nj',str(value))
    value = re.sub(r'\\xc7\\x8c','nj',str(value))
    value = re.sub(r'\\xc7\\x8d','A',str(value))
    value = re.sub(r'\\xc7\\x8e','a',str(value))
    value = re.sub(r'\\xc7\\x8f','I',str(value))
    value = re.sub(r'\\xc7\\x90','i',str(value))
    value = re.sub(r'\\xc7\\x91','O',str(value))
    value = re.sub(r'\\xc7\\x92','o',str(value))
    value = re.sub(r'\\xc7\\x93','U',str(value))
    value = re.sub(r'\\xc7\\x94','u',str(value))
    value = re.sub(r'\\xc7\\x95','U',str(value))
    value = re.sub(r'\\xc7\\x96','u',str(value))
    value = re.sub(r'\\xc7\\x97','U',str(value))
    value = re.sub(r'\\xc7\\x98','u',str(value))
    value = re.sub(r'\\xc7\\x99','U',str(value))
    value = re.sub(r'\\xc7\\x9a','u',str(value))
    value = re.sub(r'\\xc7\\x9b','U',str(value))
    value = re.sub(r'\\xc7\\x9c','u',str(value))
    value = re.sub(r'\\xc7\\x9d','e',str(value))
    value = re.sub(r'\\xc7\\x9e','A',str(value))
    value = re.sub(r'\\xc7\\x9f','a',str(value))
    value = re.sub(r'\\xc7\\xa0','A',str(value))
    value = re.sub(r'\\xc7\\xa1','a',str(value))
    value = re.sub(r'\\xc7\\xa2','AE',str(value))
    value = re.sub(r'\\xc7\\xa3','ae',str(value))
    value = re.sub(r'\\xc7\\xa4','G',str(value))
    value = re.sub(r'\\xc7\\xa5','g',str(value))
    value = re.sub(r'\\xc7\\xa6','G',str(value))
    value = re.sub(r'\\xc7\\xa7','g',str(value))
    value = re.sub(r'\\xc7\\xa8','K',str(value))
    value = re.sub(r'\\xc7\\xa9','k',str(value))
    value = re.sub(r'\\xc7\\xaa','Ǫ',str(value))
    value = re.sub(r'\\xc7\\xab','ǫ',str(value))
    value = re.sub(r'\\xc7\\xac','Ǭ',str(value))
    value = re.sub(r'\\xc7\\xad','ǭ',str(value))
    value = re.sub(r'\\xc7\\xae','EZH',str(value))
    value = re.sub(r'\\xc7\\xaf','ezh',str(value))
    value = re.sub(r'\\xc7\\xb0','j',str(value))
    value = re.sub(r'\\xc7\\xb1','DZ',str(value))
    value = re.sub(r'\\xc7\\xb2','Dz',str(value))
    value = re.sub(r'\\xc7\\xb3','dz',str(value))
    value = re.sub(r'\\xc7\\xb4','G',str(value))
    value = re.sub(r'\\xc7\\xb5','g',str(value))
    value = re.sub(r'\\xc7\\xb6','HWAIR',str(value))
    value = re.sub(r'\\xc7\\xb7','WYNN',str(value))
    value = re.sub(r'\\xc7\\xb8','N',str(value))
    value = re.sub(r'\\xc7\\xb9','n',str(value))
    value = re.sub(r'\\xc7\\xba','A',str(value))
    value = re.sub(r'\\xc7\\xbb','a',str(value))
    value = re.sub(r'\\xc7\\xbc','AE',str(value))
    value = re.sub(r'\\xc7\\xbd','ae',str(value))
    value = re.sub(r'\\xc7\\xbe','O',str(value))
    value = re.sub(r'\\xc7\\xbf','o',str(value))
    return value

def Trans_Ping(tempTitle,region,dfexcel,index,api_key):
    # global translateResponse
    connectionErrorFlag=False
    try:
        tempTitle = ftfy.fix_text(tempTitle.lower())
        Rawtitle = ftfy.fix_text(tempTitle)
        print(index)
        dfexcel.loc[index, 'Raw_Title_P1'] = tempTitle
        htmlencodedTitle = html.unescape(str(tempTitle.lower()))
        try:
            #tempTitle = htmlencodedTitle.encode("utf-8")
            #tempTitle = tempTitle.decode('unicode-escape')
            #tempTitle = bytes(tempTitle, 'iso-8859-1').decode('utf-8')
            tempTitle = ftfy.fix_text(tempTitle)
            tempTitle=tempTitle.replace("&","AND")
            time.sleep(2)
            print('https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(tempTitle) + '&source=' + str(region) + '&target=en')
            translateResponse = requests.get('https://www.googleapis.com/language/translate/v2?key='+str(api_key)+'&q=' + str(tempTitle.replace('#',"")) + '&source=' + str(region) + '&target=en', timeout=None)
            print(translateResponse.status_code)
            if translateResponse.status_code==403:
                TEXT = "Hello everyone,\n"
                TEXT = TEXT + "\n"
                TEXT = TEXT + "Google Translation Api Key:"+api_key+" Daily Limit Exceeded.Please Reset Transaltion." + "\n"
                TEXT+'https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(tempTitle) + '&source=' + str(region) + '&target=en'
                TEXT = TEXT + "\n"
                TEXT = TEXT + "Thanks,\n"
                TEXT = TEXT + "OCR IT Team"
                email_sender(TEXT,"Translation - Api key Issue")
                time.sleep(86400)

            if translateResponse.status_code!=200:
                print(translateResponse.status_code)
                time.sleep(30)
                tempTitle = tempTitle.replace("&", "AND")
                translateResponse = requests.get('https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(tempTitle.replace('#',"")) + '&source=' + str(region) + '&target=en', timeout=None)
                print("Retried Status : "+str(translateResponse.status_code))
        except UnicodeDecodeError as ue:
            print(ue, sys.exc_info()[-1].tb_lineno)
            encoded = htmlencodedTitle.encode('utf-8')
            spc = special_to_normal(encoded)
            spc = re.sub(r'^b\'|^b\"', '', spc)
            spc = re.sub(r'\'$|\"$', '', spc)
            print('https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(spc) + '&source=' + str(region) + '&target=en')
            translateResponse = requests.get('https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(spc) + '&source=' + str(region) + '&target=en', timeout=None)
            print(translateResponse.status_code)
            if translateResponse.status_code!=200:
                time.sleep(30)
                translateResponse = requests.get('https://www.googleapis.com/language/translate/v2?key=' + str(api_key) + '&q=' + str(spc) + '&source=' + str(region) + '&target=en', timeout=None)
                print("Retried Status : "+str(translateResponse.status_code))
            # dfexcel.loc[index, 'Title'] = valTemp
            dfexcel.loc[index, 'decodeIssue'] = str(ue)
            with open("UnicodeDecodeError"+cNumber+".txt", "a", encoding='utf-8') as f:
                f.write('ID : ' + str(index) + ' Title :' + str(tempTitle) + ' decodeIssue:' + str(ue) + " - lineNo:" + str(sys.exc_info()[-1].tb_lineno) + "\n")
            Log_writer("OCR_"+ cNumber +"_error.log",cNumber,ue+"  : "+str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed),"Translation Failed")
            logFile.write(str(index) + "\t" + str(ue) + "\t" + str(sys.exc_info()[-1].tb_lineno) + "\n")

        except requests.HTTPError as e:
            print("Checking internet connection failed ::" + str(e))
            Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e) + "  : " + str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed), "Translation Failed")
            TEXT = "Hello everyone,\n"
            TEXT = TEXT + "\n"
            TEXT = TEXT + "Checking HTTPError" + "\n"
            TEXT = TEXT + "\n"
            TEXT = TEXT + "Thanks,\n"
            TEXT = TEXT + "OCR IT Team"
            email_sender(TEXT,"Translation Connection Issue for Catalog "+cNumber)
            #sys.exit(0)

        except requests.ConnectionError as e:
            print("No internet connection ::" + str(e))
            Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e)+"  : "+str(sys.exc_info()[-1].tb_lineno), str(TranslationFailed), "Translation Failed")
            TEXT = "Hello everyone,\n"
            TEXT = TEXT + "\n"
            TEXT = TEXT + "No internet connection to Ping tranlsation:Issue - "+str(e) + "\n"
            TEXT = TEXT + "\n"
            TEXT = TEXT + "Thanks,\n"
            TEXT = TEXT + "OCR IT Team"
            email_sender(TEXT,"Translation Connection Issue for Catalog "+cNumber)
            connectionErrorFlag=True
            time.sleep(60)
            #sys.exit(0)
            # translateResponse['text']=''


    except Exception as e:
        print(str(e))
        Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e) + "  : " + str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed), "Translation Failed")
        sys.exit(0)

    try:
        if not (connectionErrorFlag):
            val = re.findall(r'\"translatedText\"\:\s*\"([\w\W]*?)\"\s*\}', str(translateResponse.text))
        else:
            val=[]
        if len(val)==0:
            replaceValue=''
        else:
            replaceValue = val[0]
        replaceValue = replaceValue.replace("AND", "&")
        valTemp = ftfy.fix_text(html.unescape(str(replaceValue.lower())))
        Rawtitle = ftfy.fix_text(Rawtitle.lower())
        print('-----------------')
        doc = {"TranslatedText": valTemp, "ActualTitle": Rawtitle}
        doc_type = cNumber + '_' + region
        data=es.index(index=translation_index, doc_type=doc_type, id=index, body=doc)
        print(data)
        if not data['created']:
            es.update(index=translation_index, doc_type=doc_type, id=index, body={"doc": doc})

        print(valTemp)
        with open("Translated_Titles_"+cNumber+".txt", "a") as f:
            f.write(str(index) + '\t' + str(valTemp) + "\n")

        dfexcel.loc[index, 'Title'] = valTemp
        r.incrby(str(cNumber) + "_" + str(date) + "_4_progress", 1)
    except Exception as e:
        spc = special_to_normal(valTemp.encode('utf-8'))
        spc = re.sub(r'^b\'|^b\"', '', spc)
        spc = re.sub(r'\'$|\"$', '', spc)
        replaced_spc = re.sub(u"(\u2013|\u2014|\u2015|\u2017|\u2018|\u2019|\u201a|\u201b|\u201c|\u201d|\u201e|\u2020|\u2021|\u2022|\u2026|\u2030|\u2032|\u2033|\u2039|\u203a|\u203c|\u203e|\u2044|\u204a|\u00ff|\u2122|\u202f|\u0153|\u3010|\u3011|\u2103|\u0192|\u20ac|\ufeff|\uff08|\uff09|\u0152|\ufffd|\uff0c|\u0192\u2013|\u2019|\u2122|\u2026|\u202f|\u0153|\u2032|\u3010|\u2103|\u0192|\u20ac|\ufeff|\u2018|\uff08|\u0152|\ufffd|\u201a|\u201d|\uff0c|\u0161|\u200e|\u25cf|\u0301|\u2714|\u0300|\u201c|\u201e|\u25e6|\u2022|\uff09|\u2665|\u2605|\u267b|\u272e|\u2014|\u017d|\u2219|\u0430|\u2002|\u2740|\u2030|\u200b|\u2028|\u2666|\u2606|\u02c6|\u04d5|\u266a|\u2021)","'", spc)
        dfexcel.loc[index, 'Title'] = replaced_spc
        # print(e, sys.exc_info()[-1].tb_lineno)
        dfexcel.loc[index, 'decodeIssue'] = str(e)
        with open("backup_encoding"+cNumber+".txt", "a", encoding='utf-8') as f:
            f.write('ID : ' + str(index) + ' Title :' + str(valTemp) + ' decodeIssue:' + str(e) + " - lineNo:" + str(sys.exc_info()[-1].tb_lineno) + "\n")


def input_values(sourcecontent,titleIndex,trackItemIndex,idIndex,IsFulltitleCase):
    max_row = len(sourcecontent)
    # print(max_row)
    dataValue = []
    for row_number in range(1, int(max_row)):
        tempdata = []
        if (IsFulltitleCase):
            # print("Full title run case")
            tempdata.append(sourcecontent[row_number][idIndex])
            tempdata.append(sourcecontent[row_number][titleIndex])
            dataValue.append(tempdata)
        else:
            #print("Needs to review count")
            if 'needs review' in str(sourcecontent[row_number][trackItemIndex]).lower():
                tempdata.append(sourcecontent[row_number][idIndex])
                tempdata.append(sourcecontent[row_number][titleIndex])
                dataValue.append(tempdata)
            # elif 'needs review' not in str(sourcecontent[row_number][1]).lower():
            #     id.append(str(sourcecontent[row_number][idIndex]).strip())
            #     Ytitle.append(str(sourcecontent[row_number][titleIndex]).strip())

    return dataValue

def IsParallelOrNot(inputData,region,dfexcel):
    print(len(inputData))
    if len(inputData) >= 1000:
        print("Parallel run case")
        num_cores = multiprocessing.cpu_count()
        print(num_cores)
        # print(round(len(inputData) / 4))
        print(round(len(inputData) / 2))
        # print(round(len(inputData) / 4) * 3)

        Parallel(n_jobs=num_cores, backend="threading") \
            (delayed(Trans_Ping)(inputData[i][1], region, dfexcel, inputData[i][0],"AIzaSyAvbOuYKa_rBueQEugZ0pdgQcKbVkXayhk") for i in range(0, round(len(inputData))))
        #Parallel(n_jobs=num_cores, backend="threading") \
        #    (delayed(Trans_Ping)(inputData[i][1], region, dfexcel, inputData[i][0],"AIzaSyD6apVZFtrRQmhmzvi_Uk2fJZXvnlDMa5U") for i in range(round(len(inputData)/2), round(len(inputData))))
        #Parallel(n_jobs=num_cores, backend="threading") \
        #   (delayed(Trans_Ping)(inputData[i][1], region, dfexcel, inputData[i][0],"AIzaSyBDiMEb3-7VJJno9qlz17OxTtvFUFTK1Ck") for i in range(round(len(inputData)/2), round(len(inputData)/4)*3))
        #Parallel(n_jobs=num_cores, backend="threading") \
        #   (delayed(Trans_Ping)(inputData[i][1], region, dfexcel, inputData[i][0],"AIzaSyBDC7RvjLFFTtK098tN_hCF5QY0-FrrZvU") for i in range(round(len(inputData)/4)*3, len(inputData)))

    else:
        print("Serial run case")
        for index, row in enumerate(inputData):
            Trans_Ping(inputData[index][1],region,dfexcel,inputData[index][0],"AIzaSyDhj3UeW-Z9Q1DysEGphr-IOzQv4-yhc0A")


def Elastic_Search_Cache(region,inputData,dfexcel):
    list_NotInGoogleCache=[]
    doc_type = cNumber + '_' + region
    print(len(inputData))
    # print("50")
    # print("-----")
    for index_in in range(0,len(inputData)):
        temp_data=[]
        RawTitle=inputData[index_in][1]
        try:
            result = es.search(
                index=translation_index,
                doc_type=doc_type,
                body={
                    "query": {
                        "more_like_this": {
                            "fields": ["ActualTitle"],
                            "like_text": RawTitle,
                            "min_term_freq": 1,
                            "min_doc_freq": 1
                        }
                    }
                }
            )

            countFlag = True
            for data2 in result['hits']['hits']:
                fuzzyScore = fuzz.token_set_ratio(str(re.sub(r"[^!-~\s]", "", re.sub(r"\W+", " ", str(RawTitle).lower()))).strip(),str(re.sub(r"[^!-~\s]", "",re.sub(r"\W+", " ", str(data2['_source']['ActualTitle']).lower()))).strip())
                if float(fuzzyScore) == 100 and countFlag:
                    countFlag = False
                    dfexcel.loc[inputData[index_in][0], 'Title'] = data2['_source']['TranslatedText']
                    dfexcel.loc[inputData[index_in][0], 'Raw_Title_P1'] = RawTitle

            if countFlag:
                #print("Need Google Tranlslation")
                temp_data.append(inputData[index_in][0])
                temp_data.append(inputData[index_in][1])
                list_NotInGoogleCache.append(temp_data)
                print(len(list_NotInGoogleCache))
                # print(list_NotInGoogleCache)
                # input()
        except elasticsearch.ConnectionError as e:
            print("Exception::", e)
            Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e) + "  : " + str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed), "Translation Failed")

        except elasticsearch.RequestError as e:
            print("Exception::", e)
            Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e) + "  : " + str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed), "Translation Failed")

        except elasticsearch.ElasticsearchException as e:
            Log_writer("OCR_" + cNumber + "_error.log", cNumber, str(e) + "  : " + str(sys.exc_info()[-1].tb_lineno),str(TranslationFailed), "Translation Failed")
            pass
    r.set(str(cNumber) + "_" + str(date) + "_4_total", len(list_NotInGoogleCache))
    IsParallelOrNot(list_NotInGoogleCache,region,dfexcel)



if __name__ == "__main__":
    try:
        f1=sys.argv[1]
    except:
        print("Please pass Catalog filename as first argument")
        sys.exit(0)
    startTime = datetime.now()
    fileName =str(f1)
    print(fileName)
    try:
        region=sys.argv[2]
    except:
        print("Please pass region as second argument")
        sys.exit(0)
    try:
        IsFulltitleCase=sys.argv[3]
    except:
        print("Please pass NR or Full Translation Case as Third Args")
        sys.exit(0)

    # outputfile = fileName.replace('.csv', '_pt.csv')
    outputfile = re.sub(r"_p\d+.csv", "_pt.csv", str(fileName))
    if fileName == outputfile:
        outputfile = re.sub(".csv", "_pt.csv", str(fileName))
    else:
        pass
    print(outputfile)
    dfexcel = pd.read_csv(fileName, encoding='latin1',)
    inputHeader = (list(dfexcel.columns.values))
    excelValueHeader = (list(dfexcel.columns.values))
    titleIndex = inputHeader.index("Title")
    trackItemIndex = inputHeader.index("Track Item")
    try:
        idIndex=inputHeader.index("Retailer Item ID")
        dfexcel = dfexcel.set_index("Retailer Item ID")
    except:
        idIndex=inputHeader.index("Retailer Item ID1")
        dfexcel = dfexcel.set_index("Retailer Item ID1")
    dfexcel.head()

    dic = {}
    dic["mappings"] = {}
    dic["mappings"][str(cNumber)] = {}
    dic["mappings"][str(cNumber)]["properties"] = {}
    for val in inputHeader:
        dic["mappings"][str(cNumber)]["properties"][val] = {}
        dic["mappings"][str(cNumber)]["properties"][val]["type"] = "string"

    setting = requests.put("http://" + str(host) + ":" + str(port) + "/" + str(translation_index) + "/", data=dic,headers={"Content-Type": "application/json"})
    # refresh index for every catalogs
    es.indices.refresh(translation_index)

    dfexcel['Raw_Title_P1'] = ''
    dfexcel['decodeIssue'] = ''
    logFile = open(str(region)+".txt","w")
    BackupFile = open(str(fileName.replace(".csv", "")) + ".txt", "a")

    f = open(fileName, 'rt', encoding="ISO-8859-1")
    sourcecontent = []
    reader = csv.reader(f, delimiter=',')
    for data in reader:
        sourcecontent.append(data)

    IsFulltitleCase = ast.literal_eval(IsFulltitleCase)
    inputData =input_values(sourcecontent, titleIndex, trackItemIndex, idIndex,IsFulltitleCase)
    Elastic_Search_Cache(region, inputData, dfexcel)
    try:
        dfexcel.to_csv(outputfile, sep=',', encoding ='latin1')
    except UnicodeEncodeError as e:
        print("utf-8")
        dfexcel.to_csv(outputfile, sep=',', encoding ='utf-8')
        with open("Error_translation_error"+cNumber+".txt", "a", encoding='utf-8') as f:
            f.write('latin1 decodeIssue:' + str(e) + " - lineNo:" + str(sys.exc_info()[-1].tb_lineno) + "\n")
    else:
        exc_type, exc_obj, exc_tb = sys.exc_info()
    TEXT = "Hello everyone,\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Translation Completed for Catalog : "+cNumber + "\n"
    TEXT = TEXT + "\n"
    TEXT = TEXT + "Thanks,\n"
    TEXT = TEXT + "OCR IT Team"
    email_sender(TEXT,"Translation - Catalog"+cNumber)
    Log_writer("OCR_generic_error.log", cNumber, "Translation Completed", str(TranslationCompleted),"TranslationCompleted")
    print ("Start Time::",startTime ,"\nEnd Time",datetime.now())


