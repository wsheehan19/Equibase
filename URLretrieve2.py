#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  5 09:33:21 2021

@author: williamsheehan
"""

import urllib.request
import json
import os
from urllib.request import urlopen
import posixpath
from urllib.parse import urlparse 
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import requests
import random
from selenium import webdriver
from bs4 import BeautifulSoup as bs
import numpy as np
import time


PATH = "/Users/williamsheehan/Downloads/chromedriver 3"


# def check(url):
#     req = requests.head(url)
#     req.content
#     size = req.style["width"]
#     if size != '0':
#         return True
#         print(size)
#     else:
#         return False

# def check(url):
#         content = str(r.content)
#         print(content)
#         if 'Request unsuccessful' in content:
#             return False
#         else:
#             return True
#             print('good')
#         if counter == len(UAS):
#             print('bad')
    
def download(url):
    UAS = ("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
            "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            )
    ua = UAS[random.randrange(len(UAS))]
    headers = {'user-agent': ua}
    r=requests.get(url, headers=headers, stream = True)
    text = r.text
    if 'Helvetica' in text:
#        filename = url.split("/")[4].split(".cfm")[0]
        filename = url.split('?')[1].split('=D&STYLE')[0]
        file = open(filename, 'wb+')
        file.write(r.content)
        file.close()
    if "ROBOTS" in text:
        print('text')
        return text
    else:
        print('invalid')
    return None

# with open('random300.json', 'r') as f:
#      urls = json.load(f)
error_arr = []

# for url in urls[:25]:
#     a = download(url)
#     if a is not None:
#         error_arr += [a]
        
    
#print(np.random.choice(np.array(error_arr), 5))
#print(urls)


good_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB/", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
             "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
             "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB"]
bad_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2005&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2022&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=GP&CTRY=USA&DT=10/06/2025&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/26/2021&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"]

good = "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB/"
bad = "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"

for url in good_urls:
    a = download(url)
    time.sleep(90)
    if a is not None:
        error_arr += [a]

#if os.path.exists(output_path):
#        shutil.rmtree(output_path)
#    os.mkdir(output_path)
