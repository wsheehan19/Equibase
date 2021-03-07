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


PATH = "/Users/williamsheehan/Downloads/chromedriver 3"
driver = webdriver.Chrome(PATH)

url = """https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB/"""

# def check(url):
#     req = requests.head(url)
#     req.content
#     size = req.style["width"]
#     if size != '0':
#         return True
#         print(size)
#     else:
#         return False

def check(url):
    UAS = ("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
            "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            )
    ua = UAS[random.randrange(len(UAS))]
    headers = {'user-agent': ua}    
    r = requests.get(url, headers = headers)
    content = str(r.content)
    if 'Request unsuccessful' in content:
        return False
    else:
        return True
    
    
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
    
    filename = url.split("/")[4].split(".cfm")[0]
    
    file = open(filename, 'wb')
    file.write(r.content)
    file.close()


with open('random300.json', 'r') as f:
     urls = json.load(f)

for url in urls:
    if check(url):
        download(url)
        print('file downloading')
    else:
        pass
        print('blank page')



# soup = bs(r.text, 'html.parser')
# print(soup)


