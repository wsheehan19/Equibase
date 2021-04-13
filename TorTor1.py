#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 11:48:57 2021

@author: williamsheehan
"""

import requests
from stem import Signal
from stem.control import Controller
import json
import os
import random
import time
from google.cloud import storage
import re
from google.cloud import vision
from json import JSONDecodeError
import datetime
from datetime import timedelta

def daterange(start_date, end_date):
    """
    creates range between start date and end date

    Parameters
    ----------
    start_date : start date
    end_date : end date

    Yields
    ------
    range

    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)  


def generate(tracks_dict, query_string, start_date, end_date):
    """
    generates all possible chart PDF URLs

    Parameters
    ----------
    tracks_dict : dictionary of all track ids with countries as tag
    query_string : query string template
    start_date : start date
    end_date : end date

    Returns
    -------
    items : list of all possible URLs

    """
    items = []
    for track_countries in tracks_dict.keys():
        items += [query_string.format(race=race,
                                      track=track,
                                      date=date.strftime('%m-%d-%Y'))
                  for date in daterange(start_date, end_date)
                  for race in range(0,15)
                  for track in tracks_dict[track_countries]
                  ]
    return items   

    
def download(url):
    """
    downloads a pdf to local folder

    Parameters
    ----------
    url : PDF url 

    Returns
    -------
    None if valid or invalid, error message if blocked by robots

    """
    
    
    # UAS = ("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
    #         "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
    #         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
    #         "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    #         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
    #         "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
    #         )
    UAS = ('Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/89.0.4389.112 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/89.0.4389.112 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/89.0.4389.99 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/89.0.4389.99 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/89.0.4389.93 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/89.0.4389.93 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/89.0.4389.84 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/89.0.4389.84 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/88.0.4324.208 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/88.0.4324.208 Safari/537.36',
       'Mediapartners-Google',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/88.0.4324.202 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/88.0.4324.202 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/88.0.4324.143 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/88.0.4324.143 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/88.0.4324.163 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/88.0.4324.163 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/88.0.4324.114 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/88.0.4324.114 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/87.0.4280.90 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/87.0.4280.90 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.140 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/85.0.4183.140 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/86.0.4240.96 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.136 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/85.0.4183.136 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.122 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.122 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.122 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/85.0.4183.113 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/85.0.4183.105 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.105 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/85.0.4183.93 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/85.0.4183.93 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.140 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.140 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.133 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.133 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.126 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.126 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.108 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.108 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.118 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.118 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/84.0.4147.98 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/84.0.4147.98 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.122 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.122 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.119 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.119 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.118 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.118 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.108 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.108 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.100 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.100 Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/83.0.4103.93 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/83.0.4103.93 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/81.0.4044.108 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/81.0.4044.108 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/80.0.3987.92 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/80.0.3987.92 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/79.0.3945.120 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/79.0.3945.120 Mobile Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/78.0.3904.74 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/78.0.3904.74 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/[WEBKIT_VERSION] (KHTML, like Gecko; Mediapartners-Google) Chrome/[CHROME_VERSION] Mobile Safari/[WEBKIT_VERSION]',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/41.0.2272.118 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/41.0.2272.118 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/77.0.3865.99 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/77.0.3865.99 Safari/537.36',
       'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.36 (KHTML, like Gecko; Mediapartners-Google) Chrome/77.0.3865.97 Mobile Safari/537.36',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/537.36 (KHTML, like Gecko, Mediapartners-Google) Chrome/77.0.3865.97 Safari/537.36',
       )
    ua = UAS[random.randrange(len(UAS))]
    headers = {'user-agent': ua}
    r=requests.get(url, headers=headers, stream = True)
    r=requests.get(url, headers=headers, stream = True)
    text = r.text
    if 'Helvetica' in text:
        filename = url.split('?')[1].split('/')[0]
        file = open(filename, 'wb+')
        file.write(r.content)
        file.close()
        return filename
    if "ROBOTS" in text:
        return 'ROBOTS'
    else:
        filename = url.split('?')[1].split('/')[0]
        return 'Invalid'
    
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    uploads local pdf file to Google Cloud Storage bucket
    
    Parameters
    ----------
    bucket_name : bucket name
    
    source_file_name : local path to file
    
    destination_blob_name : storage object name

    Returns
    -------
    None.

    """


    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
    
    return None

def get_current_ip():
    session = requests.session()

    # TO Request URL with SOCKS over TOR
    session.proxies = {}
    session.proxies['http']='socks5h://localhost:9050'
    session.proxies['https']='socks5h://localhost:9050'

    try:
        r = session.get('http://httpbin.org/ip')
    except Exception as e:
        print(str(e))
    else:
        return r.text

def renew_tor_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password="password93")
        controller.signal(Signal.NEWNYM)
        
        
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Documents/Equibase Docs/Equibase-4f0a3e50cac6.json"
# good_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB/", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
#              "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
#              "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB"]
bad_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2005&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2022&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=GP&CTRY=USA&DT=10/06/2025&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/26/2021&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"]

with open("trackids1.json", 'r') as file:
    tracks = json.load(file)
    

start_date = datetime.date(1998,1,1)
end_date = datetime.datetime.now().date()

urls = generate(tracks,
          "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE={race}&BorP=P&TID={track}&CTRY=USA&DT={date}&DAY=D&STYLE=EQB",
          start_date,
          end_date)

dictionary = {'untested': [], 'success': [], 'blank': [], 'error': []}

with open("testedurls.json", 'r') as file:
    urlss = json.load(file)


for url in urls:
    renew_tor_ip()
    time.sleep(random.randint(60,90))
    a = download(url)
    print(a)
    if 'Invalid' in a:
        dictionary['blank'] += [a]
        pass
    if 'ROBOTS' in a:
        dictionary['error'] += [a]
        pass    
    if 'RACE' in a:
        path = "/Users/williamsheehan/Documents/Equibase.pys/" + a
        upload_blob('equibasestorage', path, 'Equibase Charts/{}'.format(a) )
        
        dictionary['success'] += [a]
    
    if len(dictionary['error']) > 30:
        print(url)
        print('ROBOTS win')
        break;


with open('testedurls.json', 'w+') as f:
    json.dump(dictionary, f)

    