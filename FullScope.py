#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 11:24:15 2021

@author: williamsheehan
"""
import json
import os
import requests
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
        filename = url.split('?')[1].split('/')[0]
        file = open(filename, 'wb+')
        file.write(r.content)
        file.close()
        print('download')
        return None
    if "ROBOTS" in text:
        print('text')
        return text
    else:
        print('invalid')
        return None
    
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


def async_detect_document(gcs_source_uri, gcs_destination_uri):
    """
    Annotates PDF document with text detection and saves .txt file to local folder

    Parameters
    ----------
    gcs_source_uri : gcs path to PDF image
    
    gcs_destination_uri : gcs json file which will be written to .txt file

    Returns
    -------
    None.

    """
    client = vision.ImageAnnotatorClient()

    batch_size = 10
    mime_type = 'application/pdf'
    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)
    
    gcs_source = vision.GcsSource(uri = gcs_source_uri)
    input_config = vision.InputConfig(gcs_source=gcs_source, mime_type=mime_type)
    
    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(gcs_destination=gcs_destination, batch_size = batch_size)
    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config, output_config=output_config)
    
    operation = client.async_batch_annotate_files(requests=[async_request])
    operation.result(timeout=180)
    
    storage_client = storage.Client()
    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)
    bucket = storage_client.get_bucket(bucket_name)
    
    blob_list = list(bucket.list_blobs(prefix=prefix))
    
    for n in range(1, len(blob_list)):
        output = blob_list[n]
        json_string = output.download_as_string()
        try:
            response = json.loads(json_string)
    
            first_page_response = response['responses'][0]
            annotation = first_page_response['fullTextAnnotation']
    
            #print('Full text:\n')
            file = open('chart{}.txt'.format(n), 'w+')
            #print(annotation['text'])
            file.write(annotation['text'])
        except JSONDecodeError:
            print('jsondecode')
            pass
    return None



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Downloads/Equibase-4f0a3e50cac6.json"



with open("trackids1.json", 'r') as file:
    tracks = json.load(file)
    

start_date = datetime.date(1998,1,1)

end_date = datetime.datetime.now().date()

urls = generate(tracks,
         "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE={race}&BorP=P&TID={track}&CTRY=USA&DT={date}&DAY=D&STYLE=EQB",
         start_date,
         end_date)


error_arr = []        
    

for url in urls:
    a = download(url)
    time.sleep(90)
    if a is not None:
        error_arr += [a]
        
    else:
        file = url.split('?')[1].split('/')[0]
        path = "/Users/williamsheehan/Documents/Equibase.pys/all_images/" + file
        upload_blob('equibasestorage', path,'Equibase Charts/' )
        
        gcs_path = 'gs://equibasestorage/Equibase Charts/' + file
        gcs_destination_uri = 'gs://equibasestorage/Equibase Charts/output URI'
        
        async_detect_document(gcs_path, gcs_destination_uri)


with open('errorURLS.json', 'w+') as fp:
    fp.write(error_arr)
            











    
    