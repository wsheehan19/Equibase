#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 13:08:28 2021

@author: williamsheehan
"""
import time
import json
import datetime
from datetime import timedelta
from requests_tor import RequestsTor
import os
from google.cloud import storage
from retrying import retry
import concurrent.futures

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Documents/Equibase Docs/Equibase-4f0a3e50cac6.json"

rt = RequestsTor(tor_ports=[9050, 9000, 9001, 9002, 9003, 9004, 9005, 9006, 9007, 
                            9008, 9009, 9010, 9011, 9012, 9013, 9014, 9015, 9016, 9017, 9018], 
                 tor_cport=9051,
                 password='password93', autochange_id=1)

good_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
              "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
              "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB"]
bad_urls = ["https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2005&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/06/2022&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=GP&CTRY=USA&DT=10/06/2025&DAY=D&STYLE=EQB", "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BTP&CTRY=USA&DT=10/26/2021&DAY=D&STYLE=EQB",
            "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=BEL&CTRY=USA&DT=10/06/2023&DAY=D&STYLE=EQB"]
def check(page):
    text = page.text
    if 'Helvetica' in text:
        return True
    
    else:
        return False
    
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
    country : country

    Returns
    -------
    items : list of all possible URLs

    """
    items = []
    for track_country in tracks_dict.keys():
        items += [query_string.format(race=race,
                                      track=track,
                                      date=date.strftime('%m/%d/%Y'),
                                      country=track_country)
                  for date in daterange(start_date, end_date)
                  for race in range(1,15)
                  for track in tracks_dict[track_country]
                  ]
    return items

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

@retry(stop_max_attempt_number=7)
def download(urls):
    gen_counter = 0
    valid_pdfs = 0
    downloaded_pdfs = 0
    uploaded_pdfs = 0
    results = rt.get_urls(urls)
    for result in results:
        gen_counter += 1
        url = result.url
        name = url.split('?')[1].split('/')[0]
        filename = '/Users/williamsheehan/Documents/Equibase Charts/{}.pdf'.format(name)
        if check(result) == True:
            valid_pdfs += 1
            with open(filename, 'wb') as f:
                for chunk in result.iter_content():
                    f.write(chunk)
            downloaded_pdfs += 1
            time.sleep(2)
            upload_blob('equibasestorage', filename, 'Equibase Charts/{}'.format(name))
            uploaded_pdfs += 1
            time.sleep(2)
            os.remove(filename)
        else:
            pass;
    print('{} total pdfs in this chunk'.format(gen_counter))
    print('{} valid pdfs in this chunk'.format(valid_pdfs))
    print('{} pdfs downloaded to local folder'.format(downloaded_pdfs))
    print('{} pdfs uploaded to GCS'.format(uploaded_pdfs))
            
    return None

    

# tracks = {"USA": ["ALB", "AQU", "ARP", "AZD", "AP ", "ATO", "BEL", "BTP", "BKF", "CBY", "CAS", "CPW", "CD", "DG", "CNL", "CLS", "PRV", "DMR", "DEL", "DED", "UN", "ELK", "ELP", "EMD", "ED", "EVD", "FG", "FMT", "FP", "FH", "FER", "FL", "FON", "FNO", "GIL", "GG", "GRP", "GF", "GRM", "GP", "GPW", "BRN", "HAW", "CT", "HPO", "IND", "KEE", "KD", "LRL", "LNN", "LS", "LA", "LRC", "LAD", "MVR", "MED", "MC", "MTH", "MNR", "FAR", "OTP", "OP", "OTC", "ONE", "PRX", "PEN", "PIM", "PMT", "PLN", "POD", "PM ", "PRM", "PID", "RP", "RET", "RIL", "RUI", "SAC", "HOU", "SA", "SON", "SR", "SAR", "SUF", "SUD", "SUN", "SRP", "SWF", "TAM", "TDN", "TIL", "TIM", "TRY", "TUP", "TP ", "ELY", "WRD", "WYO", "ZIA"],
#           "CAN": ["ASD", "CTD", "CTM", "FE", "GPR", "HST", "LBG", "MD", "MIL", "NP ", "WO"], 
#           "PR": ["CMR", "PRV", "GPR", "PRX", "PRM"]}

tracks = {"USA": ["ALB", "WW", "MAN", "PHA", "PM", "RD", "BEU", "ATL", "YD", "SFE", "RKM", "GLD", "LBT" "RDM", "WIL", "WDS", "EUR", "GPR", "ANF", "HOO", "WMF", "MAF", "HCF", "JRN", "SAF", "DUN", "CWF", "FPL", "FAX", "FAI", "GBF", "BCF", "BMF", "RUP", "OTH", "BOI", "HIA", "CRC", "SOL", "STK" "OSA", "BHP", "BSR", "FPX", "AQU", "ARP", "AZD", "AP", "ATO", "BEL", "BM" "BTP", "BKF", "CBY", "CAS", "CPW", "CD", "DG", "CNL", "CLS", "PRV", "DMR", "DEL", "DED", "UN", "ELK", "ELP", "EMD", "ED", "EVD", "FG", "FMT", "FP", "FH", "FER", "FL", "FON", "FNO", "GIL", "GG", "GRP", "GF", "GRM", "GP", "GPW", "BRN", "HAW", "CT", "HPO", "IND", "KEE", "KD", "LRL", "LNN", "LS", "LA", "LRC", "LAD", "MVR", "MED", "MC", "MTH", "MNR", "FAR", "OTP", "OP", "OTC", "ONE", "PRX", "PEN", "PIM", "PMT", "PLN", "POD", "PM ", "PRM", "PID", "RP", "RET", "RIL", "RUI", "SAC", "HOU", "SA", "SON", "SR", "SAR", "SUF", "SUD", "SUN", "SRP", "SWF", "TAM", "TDN", "TIL", "TIM", "TRY", "TUP", "TP", "ELY", "WRD", "WYO", "YAV", "ZIA"],
          "CAN": ["ASD", "CTD", "CTM", "FE", "GPR", "HST", "LBG", "MD", "MIL", "NP ", "WO"], 
          "PR": ["CMR", "PRV", "GPR", "PRX", "PRM"]}    

start_date = datetime.date(1998,1,1)
end_date = datetime.datetime.now().date()

urls = generate(tracks,
          "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE={race}&BorP=P&TID={track}&CTRY=USA&DT={date}&DAY=D&STYLE=EQB",
          start_date,
          end_date)

chunks = [urls[x:x+300] for x in range(0, len(urls), 300)]
# print(len(chunks))

chunk_count = 0
for chunk in chunks[:11]:
    chunk_count += 1
    print('thru {} chunks'.format(chunk_count))
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(download, urls=chunk)

    time.sleep(90)
    
print('Through 10 Chunks')

    

    

