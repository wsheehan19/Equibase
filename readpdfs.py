#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 12:20:07 2021

@author: williamsheehan
"""

import json
import re
from google.cloud import vision
from google.cloud import storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Downloads/Equibase-4f0a3e50cac6.json"

def implicit():
    #from google.cloud import storage

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)



def async_detect_document(gcs_source_uri, gcs_destination_uri):
    mime_type = 'application/pdf'
    batch_size = 100
    client = vision.ImageAnnotatorClient()
    feature = vision.Feature(
        type_ = vision.Feature.Type.DOCUMENT_TEXT_DETECTION)
    gcs_source = vision.GcsSource(uri = gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source= gcs_source, mime_type = mime_type)
    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)
    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)
    operation = client.async_batch_annotate_files(
        requests=[async_request])
    print('Waiting for operation to finish')
    operation.result(timeout=400)

def write_to_text(gcs_destination_uri):
    storage_client = storage.Client()
    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)
    bucket = storage_client.get_bucket(bucket_name)
    blob_list = list(bucket.list_blobs(prefix=prefix))
    print('Output files:')
    for blob in blob_list:
        print(blob.name)
    for n in range(len(blob_list)):
        output = blob_list[n]
        json_string = output.download_as_string()
        response = json.loads(json_string)
    file = open("batch{}.txt".format(str(n)), "w")
    for m in range(len(response['responses'])):
        first_page_response = response['response'][m]
        annotation = first_page_response['fullTextAnnotation']
        print('Full text:\n')
        print(annotation['text'])
        file.write(annotation['text'])

 

async_detect_document('gs://equibasestorage/Equibase Charts/eqbPDFChartPlus (1).pdf', 'gs://equibasestorage/Equibase Charts/output URI')
    
write_to_text('gs://equibasestorage/Equibase Charts/eqbPDFChartPlus (1).pdf')    
    
    
    
    
    
    
    
    
    
    
    
    