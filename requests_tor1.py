#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 14:17:52 2021

@author: williamsheehan
"""

from requests_tor import RequestsTor
import os
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Documents/Equibase Docs/Equibase-4f0a3e50cac6.json"

rt = RequestsTor(tor_ports=[9050, 9000, 9001, 9002, 9003, 9004], tor_cport=9051,
                 password='password93', autochange_id=1)



good_urls = ['https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=11&BorP=P&TID=SAR&CTRY=USA&DT=08/26/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=7&BorP=P&TID=BEL&CTRY=USA&DT=10/29/2005&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=4&BorP=P&TID=LS&CTRY=USA&DT=10/30/2004&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=BEL&CTRY=USA&DT=09/07/2003&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=SA&CTRY=USA&DT=03/01/2008&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=CD&CTRY=USA&DT=05/05/2007&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=SA&CTRY=USA&DT=03/06/2021&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=SA&CTRY=USA&DT=02/07/2021&DAY=D&STYLE=EQB',
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB"]

bad_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2005&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2022&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=GP&CTRY=USA&DT=10/06/2025&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/26/2021&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"]

all_urls = ['https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=11&BorP=P&TID=SAR&CTRY=USA&DT=08/26/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=7&BorP=P&TID=BEL&CTRY=USA&DT=10/29/2005&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=4&BorP=P&TID=LS&CTRY=USA&DT=10/30/2004&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=BEL&CTRY=USA&DT=09/07/2003&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=SA&CTRY=USA&DT=03/01/2008&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=CD&CTRY=USA&DT=05/05/2007&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=SA&CTRY=USA&DT=03/06/2021&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=SA&CTRY=USA&DT=02/07/2021&DAY=D&STYLE=EQB',
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2005&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2022&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=GP&CTRY=USA&DT=10/06/2025&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/26/2021&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"]

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

    #print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
    
    return None



results = rt.get_urls(all_urls)

for result in results:
    url = result.url
    name = url.split('?')[1].split('/')[0]
    filename = '/Users/williamsheehan/Documents/Equibase Charts/{}.pdf'.format(name)
    if int(result.headers['Content-length']) > 1000:
        with open(filename, 'wb') as f:
            for chunk in result.iter_content():
                f.write(chunk)
        upload_blob('equibasestorage', filename, 'Equibase Charts/{}'.format(name))
    else:
        print('invalid')
                

 
# print(len(real_urls))
# print(len(fake_urls))
         

            
            