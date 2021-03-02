#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 17:17:57 2021

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

PATH = "/Users/williamsheehan/Documents/ChromeDriver/chromedriver"


def check(lnk):
    try:
        u = urlopen(lnk)
        u.close()
        return True
    except:
        print('no such url')
        return False

# download_folder = "/Users/williamsheehan/Documents/Equibase Charts"

def download_pdf(lnk):

    from selenium import webdriver

    options = webdriver.ChromeOptions()

    download_folder = "/Users/williamsheehan/Documents/Equibase.pys/all_images"    

    profile = {"plugins.plugins_list": [{"enabled": False,
                                         "name": "Chrome PDF Viewer"}],
               "download.default_directory": download_folder,
               "download.extensions_to_open": ""}

    options.add_experimental_option("prefs", profile)

    print("Downloading file from link: {}".format(lnk))

    driver = webdriver.Chrome(PATH, chrome_options = options)
    driver.get(lnk)

    filename = lnk.split("/")[4].split(".cfm")[0]
    print("File: {}".format(filename))

    print("Status: Download Complete.")
    print("Folder: {}".format(download_folder))

    driver.close()
    
with open('first100urls.json', 'r') as f:
    urls = json.load(f)


for url in urls:
    if check(url):
        download_pdf(url)
    else:
        print('url not found')
        pass


# https://stackoverflow.com/questions/43470535/python-download-pdf-embedded-in-a-page
# someone is coincidentally trying to do the exact same thing, but with only one pdf
