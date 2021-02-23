#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 19:35:24 2021

@author: williamsheehan
"""
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import json
import time
import datetime
import urllib.request
import os

PATH = "/Users/williamsheehan/Documents/ChromeDriver/chromedriver"
driver = webdriver.Chrome(PATH)
driver.implicitly_wait(15)

action = ActionChains(driver)
options = webdriver.ChromeOptions()

driver.maximize_window()

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
        url = "{}"
        link = url.format(href)
        driver.get(str(link))
        try:
            results_page = WebDriverWait(driver, 120).until(
                EC.presence_of_element_located((By.ID, "Hresults")))
            results_page.click()
            pdfs = driver.find_elements_by_css_selector(".chart > a")
            for pdf in pdfs:
                pdf.click()
                view_full_pdf = driver.find_element(By.CSS_SELECTOR, '#interior-content > div.content > div:nth-child(4) > div > span:nth-child(6) > a:nth-child(2)')
                lnk = view_full_pdf.get_attribute('href')
                print(lnk)
                img_name = os.path.basename(lnk)
                if "chart" in str(lnk):
                    urllib.request.urlretrieve(str(lnk), img_name)
                    driver.close()
        except TimeoutException:
            pass;
    return None


# main program structure
start_year = 1998
current_year = datetime.datetime.today().year
foaling_years = {x:[] for x in range(start_year, current_year+1)}
download("1998")