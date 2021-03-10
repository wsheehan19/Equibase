#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 15:28:16 2021

@author: williamsheehan
"""
import json
import re
from google.cloud import vision
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Downloads/Equibase-4f0a3e50cac6.json"


gcs_destination_uri = 'gs://equibasestorage/Equibase Charts/output URI'

def read(destination):
    storage_client = storage.Client()
    match = re.match(r'gs://([^/]+)/(.+)', destination)
    bucket_name = match.group(1)
    prefix = match.group(2)
    bucket = storage_client.get_bucket(bucket_name)
    print(bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))
    print(blob_list)
    print('Output files:')
    for blob in blob_list:
        print(blob.name)
    for n in range(len(blob_list)):
         output = blob_list[n]
         json_string = output.download_as_string()
    response = json.loads(json_string.decode())
    file = open("batch{}.txt".format(str(n)), "w+")
    for m in range(len(response['responses'])):
        first_page_response = response['responses'][m]
        annotation = first_page_response['fullTextAnnotation']
        #print('Full text:\n')
        #print(annotation['text'])
        file.write(annotation['text'])     
    
    

read(gcs_destination_uri)