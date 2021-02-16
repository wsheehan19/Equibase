#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 13:30:46 2021

@author: williamsheehan
"""
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from time import sleep
import pandas as pd
import json
#import tabula
import time
import os
#import module_name
import urllib
from urllib import request
import pickle
import datetime

PATH = "/Users/williamsheehan/Documents/ChromeDriver/chromedriver"
driver = webdriver.Chrome(PATH)
driver.implicitly_wait(15)

action = ActionChains(driver)
options = webdriver.ChromeOptions()

# go to equibase
driver.get("https://www.equibase.com/stats/View.cfm?tf=year&tb=horse")
driver.maximize_window()

def foalingyear():
    '''
    Navigates to  'Horses - By Foaling Year' Page
    '''
    
    horses_by_foaling_year = WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Foaling Year")))
    horses_by_foaling_year.click()
    return None

def move_foaling_years(year):
    '''
    Opens menu of years and selects one
    
    Parameters:
        year
    
    Returns:
        None, opens first page of results from that year
    '''
    year_menu = Select(driver.find_element_by_id("foalYearList"))
    year_menu.select_by_value(year)
    return None


def next_page(css_selector):
    '''
    Navigates to the next page of search results
    
    Parameters:
        css selector of 'next page' button
    
    Returns:
        None, opens next page of results
    '''
    driver.find_element_by_css_selector(css_selector).click()
    time.sleep(5)
    return None


def extract(foaling_year, array):
    '''
    locates all horse links, extracts hrefs, appends hrefs to an array, 
    appends list to a dictionary with corresponding foaling year
    
    Parameters:
        foaling year, array
    
    
    Returns:
        None
    '''
    horses = driver.find_elements_by_css_selector(".horse > a")
    for horse in horses:
        try:
            horse_href = horse.get_attribute('href')
            if horse_href is not None:
                array.append(str(horse_href))
                foaling_years[foaling_year].append(array)
        except StaleElementReferenceException:
            driver.refresh()
            foalingyear()
            move_foaling_years(foaling_year)
            time.sleep(15)
            continue
    return None  

def save(dictionary):
    '''
    Saves updated dictionary to a json file

    Parameters:
        dictionary
        
    Returns:
        None
        
    '''
    with open("equibasehrefs.json", 'w+') as f:
        json.dump(dictionary, f)
    return None

 
def download(foaling_year):
    '''
    Navigates to horse page, clicks on 'Results' tab, downloads each pdf chart and saves into a folder
    
    Arguments:
        foaling year
        
    Returns:
        None
    
    '''
    with open("equibasehrefs.json", 'r') as file:
        data = json.load(file)
        for href in data[foaling_year]:
            url = "/premium/chartEmb.cfm?track={0}=USA&rn=8"
            url.format(href)
            driver.get(href)
            results_page = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "Hresults")))
            results_page.click()
            pdfs = driver.find_elements_by_class_name("chart")
            for pdf in pdfs:
                lnk = pdf.get_attribute("href")
                download_folder = "/Users/williamsheehan/Documents/Equibase Charts"
                profile = {"plugins.plugins_list": [{"enabled": False,
                                         "name": "Chrome PDF Viewer"}],
               "download.default_directory": download_folder,
               "download.extensions_to_open": ""}
                options.add_experimental_option("prefs", profile)
                driver.get(lnk)
    return None

    
# main program structure
hrefs_1998 = []
start_year = 1998
current_year = datetime.datetime.today().year
foaling_years = {x:[] for x in range(start_year, current_year+1)}
foalingyear()
move_foaling_years("1998")
pages_remaining = True
counter = 0
while pages_remaining:   
    try:
        extract(1998, hrefs_1998)
        print(len(hrefs_1998))
        next_page("#Pagination > ul > a:nth-child(9) > img")
        counter += 1
    except NoSuchElementException:
        pages_remaining = False
    except StaleElementReferenceException:
        driver.refresh()
        foalingyear()
        move_foaling_years("1998")
        time.sleep(15)
        continue
    if counter > 5:
        pages_remaining = False
        break;
save(foaling_years)
print(len(hrefs_1998))
print(hrefs_1998)
download(1998)

