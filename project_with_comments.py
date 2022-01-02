#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  2 10:03:19 2022

@author: williamsheehan
"""
import time
import json
import datetime
from requests_tor import RequestsTor
import concurrent.futures
from bs4 import BeautifulSoup
from retrying import retry
import os
from google.cloud import storage

# Equibase.com uses security measures to block web scrapers downloading large amounts of race charts
# I tried several different methods to evade theur security, and found this package
# it is effectively the 'requests' package but disguised by the Tor browser
# it was loosely documented and was slightly temperamental when I was using it
# it seems like it may have now been fully deleted, so this may have to be totally reworked
# the password and open ports are specific to my computer, a new object will have to be created
rt = RequestsTor(tor_ports=[9050, 9000, 9001, 9002, 9003, 9004, 9005, 9006, 9007, 
                            9008, 9009, 9010, 9011, 9012, 9013, 9014, 9015, 9016, 9017, 9018], 
                  tor_cport=9051,
                  password='password93', autochange_id=1)    


query_string = 'https://www.equibase.com/premium/eqpVchartBuy.cfm?mo={month}&da={day}&yr={year}&trackco=ALL;ALL&cl=Y'    
    
def generate(query_string):
    """
    generates url for historical chart page for all dates 
    starting on Jan 1, 1991
    
    Some of these are race days, and some aren't
    
    Parameters
    ----------
    query_string = generic url of historical chart page (above)

    Returns
    -------
    list of all possible urls of historical chart pages
    """
    items = []
    items += [query_string.format(month = month,
                                  day = day,
                                  year = year)
              for month in range(1,13)
              for day in range(1,32)
              for year in range(1991, datetime.datetime.today().year + 1)                 
              ]
    return items

string = "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE={race}&BorP=P&TID={track}&CTRY={ctry}&DT={date}&DAY=D&STYLE=EQB"
robot_retries = []
reals = []
def generate_reals(historical_charts, pdf_query_string):
    """
    checks all the possible historical chart pages and creates the real urls of race
    charts for that day, e.g. if there races at 3 on a particular day, only urls to the 
    pdf charts for those tracks will be added to the 'reals' list. If that day was not a
    race day, nothing will be added. 

    Parameters
    ----------
    historical_charts : urls of historical charts pages
    pdf_query_string : generic url of a race pdf ('string' above)

    Returns
    -------
    None.

    """
    
    results = rt.get_urls(historical_charts)
    urls = []
    for page in results:
        if page.status_code != 200: 
            # even with the Tor disguise, the robots still sometimes catch the request
            # the page may have several valid races, so I just save them to try later
            robot_retries.append(page.url)
            
        data = page.text
        soup = BeautifulSoup(data, features="lxml")
        
        hrefs = []
        for link in soup.find_all('a', attrs={'class':'dkbluesm'}): #tracks racing on that day
            hrefs.append(link.get('href'))
          
        for href in hrefs: 
            tid = href.split('tid=')[-1].split('&dt')[0] # track code
            dt = href.split('dt=')[-1].split('&ctry')[0]  # date of race          
            ctry = href.split('&ctry')[-1] # country of race
            urls += [pdf_query_string.format(race = race,
                                              track = tid,
                                              date = dt,
                                              ctry = ctry)
                      for race in range(1,15)] 
            # 15 is most possible number of races at one track on one day, 
            # so I just account for that even if there were fewer in reality
            
    
    print(len(urls))
    for url in urls:
        reals.append(url)
        

    return None



urls = generate(query_string)
print(len(urls)) # = amount of days since Jan 1, 1991 (first year recorded in equibase)

chunks = [urls[x:x+300] for x in range(0, len(urls), 300)] 
# since there are a lot of days to check, I broke it up in chunks of 300 to try at time


for c in chunks:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor: 
        # this employs multiple threads of the computer on the task simultaneously to make it go quicker
        executor.submit(generate_reals,# function to call
                        historical_charts=c, pdf_query_string=string) # parameters of function
    time.sleep(5) # sleep a few seconds between chunks to help mitigate the robots catching on


with open('realurls.json', 'w+') as file: # dump the reals to a json file 
    json.dump(reals, file)

with open('robot_retries.json', 'w+') as f: # dump the untested urls which were blocked by robots to try later
    json.dump(robot_retries, f)


def check(page):
    """
    checks if pdf is real or blank
    equibase stores blank pdfs for races that didn't happen, so checking for
    Helvetica makes sure theres actually content on the pdf

    Parameters
    ----------
    page : pdf

    Returns
    -------
    bool
        True = valid pdf with content
        False = blank pdf

    """
    text = page.text
    if 'Helvetica' in text:
        return True
    
    else:
        return False
    
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
    
    # much of this is specific to my Google Cloud account which has been deactivated, but the code works
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/williamsheehan/Documents/Equibase Docs/Equibase-4f0a3e50cac6.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    #print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
    
    return None
    
@retry(stop_max_attempt_number=7) # since the package is finicky, I added this retry decorator to automatically try again if it fails
def download(urls):
    """
    downloads the bytes from the race pdfs, writes them to a local file,
    uploads that file to GCS, then removes the file locally

    Parameters
    ----------
    urls : urls of race PDFs

    Returns
    -------
    None.

    """
    valid_pdfs = 0
    results = rt.get_urls(urls)
    for result in results:
            url = result.url
            name = url.split('?')[1].split('/')[0]
            filename = '/Users/williamsheehan/Documents/Equibase Charts/{}.pdf'.format(name)
            if check(result) == True: 
                with open(filename, 'wb') as f:
                    for chunk in result.iter_content():
                        f.write(chunk) # writes the bytes to a file with the name of the race
                time.sleep(2)
                upload_blob('equibasestorage', filename, 'Equibase Charts/{}'.format(name))
                valid_pdfs += 1
                time.sleep(2)
                os.remove(filename) # this removes the file locally so you don't have thousands of pdfs taking up storage
            else:
                pass;
    print('{} pdfs downloaded in this chunk'.format(valid_pdfs))
            
    return None

# set urls = the valid urls from the json file of real urls
chunks = [urls[x:x+300] for x in range(0, len(urls), 300)]
# same method as before, but now we are moving through actual race PDFs instead of historical charts pages
for chunk in chunks:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(download, urls=chunk)

    time.sleep(10)
    
## code below is to parse the pdf data and upload it to a SQL database    
import camelot
import pandas as pd
import numpy as np
import PyPDF2
import numbers
import re
from dateutil import parser
import time
import sqlalchemy
from sqlalchemy import create_engine, select, exists
from sqlalchemy import Column, Integer, String, ForeignKey, Date, TIME, DECIMAL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# connects to SQL database and creates a session
engine = create_engine('mysql+mysqlconnector://root:Virginia2023!*@localhost/equibase', echo = True)
Session = sessionmaker(bind=engine)
session = Session()


Base = declarative_base()


class horsetable(Base): # table of  horses
    __tablename__ = 'horsetable'
    
    id_horse = Column(Integer, primary_key=True, autoincrement="auto")
    name = Column(String(300), unique = True, nullable = False)
    
    

class racetable(Base): # table of races
    __tablename__ = 'racetable'
    
    id_race = Column(Integer, primary_key=True, autoincrement="auto")
    RaceID = Column(String(50), unique = True)
    RaceDate = Column(Date)
    Track = Column(String(300))
    RaceNum = Column(Integer)
    RaceType = Column(String(800))
    Distance = Column(String(800))
    Purse = Column(String(300))
    ClaimingPrice = Column(String(300))
    Weather = Column(String(300))
    TrackSpeed = Column(String(300))
    StartType = Column(String(300))
    OffAt = Column(TIME)
    FinalTime = Column(TIME)
    Fractional_Time_One = Column(TIME)
    Fractional_Time_Two = Column(TIME)
    Fractional_Time_Three = Column(TIME)
    Fractional_Time_Four = Column(TIME)
    Fractional_Time_Five = Column(TIME)
    Fractional_Time_Six = Column(TIME)

    
    
class PPFtable(Base): # table of individual horse's performances in a race
    __tablename__ = 'PPFtable'
    
    id_ppf = Column(Integer, primary_key=True, autoincrement="auto")
    id_horse = Column(Integer, ForeignKey('horsetable.id_horse'))
    id_race = Column(Integer, ForeignKey('racetable.id_race'))
    horsename = Column(String(300))
    RaceID = Column(String(50))
    pgm = Column(String(100))
    startposition = Column(String(100))
    three_sixteenths = Column(String(100))
    qmile = Column(String(100))
    three_eighths = Column(String(100))
    halfmile = Column(String(100))
    threeqmile = Column(String(100))
    mile = Column(String(100))
    mileandq = Column(String(100))
    mileandhalf = Column(String(100))
    mileandthreeq = Column(String(100))
    twomile = Column(String(100))
    twoandhalf = Column(String(100))
    threemile = Column(String(100))
    threeandhalf = Column(String(100))
    stretchone = Column(String(100))
    stretch = Column(String(100))
    finish = Column(String(100))
    odds = Column(DECIMAL(4,2))
    trainer = Column(String(300))
    
    
    
Base.metadata.create_all(engine)    
    
    
def get_loc(section, text): # locates the location of a word on the page
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    a = result[1]
    b = result[0]
    
    return section[a][b]

def get_index(section, text): # locates the indexed location of a word on the page
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    
    return result

def coerce_to_numeric(value): # turns a number which python is not reading as a number into a true numeric
    if isinstance(value, numbers.Number):
        return value
    else:
        return np.NaN



def fractional_times(path, raceID):
    """
    extracts the fractional times of a race
    since races can be many different lengths, there can be many different 
    amounts of fractional times, hence the bulky code to account for this

    Parameters
    ----------
    path : path to pdf
    raceID : unique ID of this race with track, date, and race number

    Returns
    -------
    frac_time_queries : the appropriate SQL queries to update this race with fractional times

    """
    pdfFileObject = open(path, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObject)
    count = pdfReader.numPages
    for i in range(count):
        page = pdfReader.getPage(i)
        a = page.extractText()
        try:
            frac_times = a.split('FractionalTimes:')[1].split('FinalTime:')[0]
        except IndexError:
            pass;
    
    
    frac_time_queries = []
    update_query = "UPDATE {table} SET {column} = '{value}' WHERE {condition_col} = '{condition_val}'"
    
    a = frac_times.count(':') # counts the colins in this section which gives the amount of fractional times
    b = frac_times.count('.') # annpyingly some fractional times are accurate to the decimal, and others aren't
                                # this affects the way we split it up, so we have to count the decimals also
    
    
    if b == 1:
        if a == 1:
            time_one = '00:' + str(frac_times[:8].strip('\n'))
        if a == 0:
            time_one = frac_times[:6].strip('\n')
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
    if b == 2:
        if a == 2:
            time_one = '00:' + str(frac_times[:8].strip('\n'))
            time_two = '00:' + str(frac_times[8:15].rstrip())
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = '00:' + str(frac_times[6:13].rstrip())
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
    if b == 3:
        if a == 3:
            time_one = '00:' + str(frac_times[:8].strip('\n'))
            time_two = '00:' + str(frac_times[8:15].rstrip())
            time_three = '00:' +str(frac_times[15:22].rstrip())
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = '00:' +str(frac_times[6:13].rstrip())
            time_three = '00:' +str(frac_times[13:20].rstrip())
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = '00:' +str(frac_times[11:18].rstrip())
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
    if b == 4:
        if a == 4:
            time_one = '00:' +str(frac_times[:8].strip('\n'))
            time_two = '00:' +str(frac_times[8:15].rstrip())
            time_three = '00:' +str(frac_times[15:22].rstrip())
            time_four = '00:' +str(frac_times[22:29].rstrip())
    
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = '00:' +str(frac_times[6:13].rstrip())
            time_three = '00:' +str(frac_times[13:20].rstrip())
            time_four = '00:' +str(frac_times[20:27].rstrip())
        
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = '00:' +str(frac_times[11:18].rstrip())
            time_four = '00:' +str(frac_times[18:25].rstrip())
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = '00:' +str(frac_times[16:23].rstrip())
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        
    if b == 5:
        if a == 5:
            time_one = '00:' +str(frac_times[:8].strip('\n'))
            time_two = '00:' +str(frac_times[8:15].rstrip())
            time_three = '00:' +str(frac_times[15:22].rstrip())
            time_four = '00:' +str(frac_times[22:29].rstrip())
            time_five = '00:' +str(frac_times[29:36].rstrip())
        if a == 4:
            time_one = frac_times[:6].strip('\n')
            time_two = '00:' +str(frac_times[6:13].rstrip())
            time_three = '00:' +str(frac_times[13:20].rstrip())
            time_four = '00:' +str(frac_times[20:27].rstrip())
            time_five = '00:' +str(frac_times[27:34].rstrip())
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = '00:' +str(frac_times[11:18].rstrip())
            time_four = '00:' +str(frac_times[18:25].rstrip())
            time_five = '00:' +str(frac_times[25:32].rstrip())
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = '00:' +str(frac_times[16:23].rstrip())
            time_five = '00:' +str(frac_times[23:30].rstrip())
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = '00:' +str(frac_times[21:28].rstrip())
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        
    if b == 6:
        if a == 6:
            time_one = '00:' +str(frac_times[:8].strip('\n'))
            time_two = '00:' +str(frac_times[8:15].rstrip())
            time_three = '00:' +str(frac_times[15:22].rstrip())
            time_four = '00:' +str(frac_times[22:29].rstrip())
            time_five = '00:' +str(frac_times[29:36].rstrip())
            time_six = '00:' +str(frac_times[36:43].rstrip())
        if a == 5:
            time_one = frac_times[:6].strip('\n')
            time_two = '00:' +str(frac_times[6:13].rstrip())
            time_three = '00:' +str(frac_times[13:20].rstrip())
            time_four = '00:' +str(frac_times[20:27].rstrip())
            time_five = '00:' +str(frac_times[27:34].rstrip())
            time_six = '00:' +str(frac_times[34:41].rstrip())
        if a == 4:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = '00:' +str(frac_times[11:18].rstrip())
            time_four = '00:' +str(frac_times[18:25].rstrip())
            time_five = '00:' +str(frac_times[25:32].rstrip())
            time_six = '00:' +str(frac_times[32:39].rstrip())
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = '00:' +str(frac_times[16:23].rstrip())
            time_five = '00:' +str(frac_times[23:30].rstrip())
            time_six = '00:' +str(frac_times[30:37].rstrip())
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = '00:' +str(frac_times[21:28].rstrip())
            time_six = '00:' +str(frac_times[28:35].rstrip())
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = '00:' +str(frac_times[26:33].rstrip())
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = frac_times[26:31].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Six', value=time_six, condition_col='RaceID', condition_val=raceID))
    
        
    return frac_time_queries
        
def get_trainers(path): # camelot, the package I used to extract all the other info,
#                        somehow can't reach the part of the page where trainers are recorded
#                       so I used PyPDF2 to get the trainers
    pdfFileObject = open(path, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObject)
    
    count = pdfReader.numPages
    for i in range(count):
        page = pdfReader.getPage(i)
        a = page.extractText()
        try:
            trainers = a.split('Trainers:')[1].split('Owners')[0]
        except IndexError:
            pass;
    
    trainer_names = []
    for a in trainers.split(';'):
        name = a.split('-')[-1]
        name = name.rstrip()
        trainer_names.append(name)    
 
    return trainer_names

def check_letters(string): 
    if re.search('[a-zA-Z]+', string):
        return False
    else:
        return True
 


def info_and_upload(path):
    """
    Extracts info from PDF and uploads it to several SQL tables

    Parameters
    ----------
    path : path to PDF

    Returns
    -------
    None.

    """
    tables = camelot.read_pdf(path, pages='all', flavor='stream',
                              multiple_tables = True, flag_size = True
                              )    
    # camelot is good for reading charts, so it organizes the data from these race charts
    # consistently enough for us to use. There is some variation in the way it reads the charts
    intro_and_chart = tables[0].df
    if len(intro_and_chart.columns) < 4:
        return None # camelot occasionally just can't read a page
 
    
    # RACE INFO
    TID = path.split('TID=')[-1].split('&CTRY')[0]
    racename_loc = get_loc(intro_and_chart, '-Race')
    racename = racename_loc
    a = racename.split('<')[0]
    b = racename.split('>')[-1]
    race = a+b
    
    date_str = race.split('-')[1]
    date = parser.parse(date_str).date()
    
    racenum = race[-2:]
    if check_letters(racenum) == True:
        racenum = racenum
    else:
        racenum = race[-1]
        
    track = race.split('-')[0]
    
    raceID = TID + '-' + str(date) + '-' + str(racenum)
      
    try:
        racetype_loc = get_loc(intro_and_chart, 'FOR')
        racetype = racetype_loc.split('.')[0]
    except IndexError:
        racetype = 'No Stipulations'

    
    try:
        distance_loc = get_loc(intro_and_chart, 'Mile')
        distance = distance_loc.split('Current')[0]
    except IndexError:
        try:
            distance_loc = get_loc(intro_and_chart, 'Furlongs')
            distance = distance_loc.split('Current')[0]
        except IndexError:
            distance_loc = get_loc(intro_and_chart, 'Yards')
            distance = distance_loc.split('Current')[0]
    
    try:
        cprice_loc = get_loc(intro_and_chart, 'Claiming Price')
        claimingprice = cprice_loc.split(':')[-1]
    except IndexError:
        claimingprice = 'N/A'
        
    purse_loc =  get_loc(intro_and_chart, 'Purse:')
    purse = purse_loc.split(':')[-1]
    
    weather_loc = get_loc(intro_and_chart, 'Weather:')
    trackspeed_loc = get_loc(intro_and_chart, 'Track:')
    if 'Track' in str(weather_loc):
        weather = weather_loc.split('Track')[0].split(' ')[-1]
    
    else:
        weather = weather_loc.split(': ')[-1]
    
    trackspeed = trackspeed_loc.split(': ')[-1]
    
    offat_loc = get_loc(intro_and_chart, 'Off at:')
   
    if 'Start' in offat_loc:
        offat = offat_loc.split('Start')[0].split('at: ')[-1]
    else:
        offat = offat_loc.split(':')[-1]
        
    if '::' in offat:
        offat = ''
    
    start_loc = get_loc(intro_and_chart, 'Off at:')
    if 'Timer' in start_loc:    
        start = start_loc.split(': ')[-2]
    else:
        start = start_loc.split(': ')[-1]
    
    
    try:
        finaltime_loc = get_loc(intro_and_chart, 'Final Time:')
        if "(New Track Record)" in finaltime_loc:
            finaltime = finaltime_loc.split('Time: ')[-1].split(' (')[0] 
        else:
            finaltime = finaltime_loc.split('Time: ')[-1]
        if ':' in finaltime:
            finaltime = '00:' + finaltime
    except IndexError:
        finaltime = ''
    
    if ':.' in finaltime:
        finaltime = finaltime.replace(':.', ':')
    if '00:Final' in finaltime:
        finaltime= ''
            
    # add a race into racetable      
    race = racetable(RaceID=raceID,RaceDate=date,
                         Track=track,RaceNum=racenum,
                         RaceType=racetype,Distance=distance,
                         Purse=purse,ClaimingPrice=claimingprice,
                         Weather=weather,TrackSpeed=trackspeed,
                         StartType=start,OffAt=offat,FinalTime=finaltime)
    
    ret = session.query(exists().where(racetable.RaceID==raceID)).scalar() #check if race exists in the table
    if ret == False:
        session.add(race)
        session.commit()
    else:
        print('duplicate')
    
    # HORSE PPF INFO                   
    try:
        past_pf = tables[2].df
    except IndexError:
        try:
            past_pf = tables[1].df
        except IndexError:
            return None
    
    topleft = get_index(past_pf, 'Pgm')
 
    
    arrays = past_pf[topleft[0]:].values
    data=arrays[1:]
    columns=arrays[0]
    df = pd.DataFrame(np.vstack(arrays))
    df = pd.DataFrame(data=data,
                      columns=columns,
                      )
    
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)
    
    if df.columns[0] == 'Pgm Horse Name':
        df.rename(columns={'Pgm Horse Name': 'pgm', '': 'horsename'}, inplace=True)
    
    if df.columns[1] == 'Pgm Horse Name':
        df.rename(columns={'': 'Pgm', 'Pgm Horse Name': 'horsename'}, inplace=True)
    try:
        odds_ind = get_index(intro_and_chart, 'Odds')
        odds_comments = intro_and_chart.loc[odds_ind[0]:, odds_ind[1]: ]
        odds_comments.dropna()
    
        filter = odds_comments != ' '
        odds = odds_comments[filter]
        
        odds_list = []
        for row in odds.iterrows():
            string = str(row)
            if '.' in string:
                a = string.split('.')[0][-2:]
                b = string.split('.')[1][:2]
                c = a.strip()
                value = c + '.' + b
                odds_list.append(value)
            else:
                pass
        odds = []
        for x in odds_list:
            if check_letters(x) == True:
                if len(x.split('.')[0]) == 1:
                    odd = '0' + x
                    odds.append(odd)
                else:    
                    odds.append(x)
            else:
                pass
        try:
           df['odds'] = odds
        except ValueError:
            return None
    except IndexError:
        odds = []   
    
    try:
        trainers = []    
        for trainer in get_trainers(path):
            a = trainer.replace("'", "")
            trainers.append(a.strip('\n'))
            
            
        df['trainer'] = trainers
    except:
        print('Cant get trainers')
        pass

    
    # clean up column names
    df.rename(columns={'Start': 'startposition', 'Str': 'stretch', 'Fin': 'finish'}, inplace=True)
    if '3/16' in df.columns:
        df.rename(columns={'3/16': 'three_sixteenths'}, inplace=True)
    if '1/4' in df.columns:
        df.rename(columns={'1/4': 'qmile'}, inplace=True)
    if '3/8' in df.columns:
        df.rename(columns={'3/8': 'three_eighths'}, inplace=True)
    if '1/2' in df.columns:
        df.rename(columns={'1/2': 'halfmile'}, inplace=True)
    if '3/4' in df.columns:
        df.rename(columns={'3/4': 'threeqmile'}, inplace=True)
    if '1m' in df.columns:
        df.rename(columns={'1m': 'mile'}, inplace=True)
    if '11/4' in df.columns:
        df.rename(columns={'11/4': 'mileandq'}, inplace=True)
    if '11/2' in df.columns:
        df.rename(columns={'11/2': 'mileandhalf'}, inplace=True)
    if '13/4' in df.columns:
        df.rename(columns={'13/4': 'mileandthreeq'}, inplace=True)
    if '2m' in df.columns:
        df.rename(columns={'2m': 'twomile'}, inplace=True)
    if '21/2' in df.columns:
        df.rename(columns={'21/2': 'twoandhalf'}, inplace=True)
    if '3m' in df.columns:
        df.rename(columns={'3m': 'threemile'}, inplace=True)
    if '31/2' in df.columns:
        df.rename(columns={'31/2': 'threeandhalf'}, inplace=True)
    if 'Str 1' in df.columns:
        df.rename(columns={'Str 1': 'stretchone'}, inplace=True)
    if 'Str1' in df.columns:
        df.rename(columns={'Str1': 'stretchone'}, inplace=True)
       
    
    
    horse_dict = df.to_dict('records')
    
    
    
    def create_ppf_row(horsename, RaceID): # creates a row in PPF Table for a horse's performance in a race
        ins_str = "INSERT INTO PPFtable (horsename, RaceID, id_horse, id_race) VALUES ('{horsename}', '{raceID}', (SELECT id_horse FROM horsetable WHERE name='{horsename}'), (SELECT id_race FROM racetable WHERE RaceID='{raceID}'))".format(raceID=raceID, horsename=horsename)
        engine.execute(ins_str)
    
    def update_ppf_row(col, val, horsename, RaceID): # updates a horse's performance row
        upd_str = "UPDATE PPFtable SET {col} = '{val}' WHERE RaceID = '{raceID}' AND horsename = '{horsename}'".format(col=key, val=value, raceID=raceID, horsename=horsename)
        engine.execute(upd_str)
    
    for item in horse_dict:
        horsename = item['horsename'].replace("'", "") # clean up horse name
        horse = horsetable(name=horsename)
        ret = session.query(exists().where(horsetable.name==horsename)).scalar() # check if horse exists
        if ret == False:
            session.add(horse) # add horse into horse table
            session.commit() 
        else:
            pass             
        create_ppf_row(horsename, raceID) # create the horse's PPF row
        for key in item:
            value = item[key]
            if 'horsename' not in key:    
                update_ppf_row(key,value,horsename,raceID) # fill out horse's PPF row with data
            else:
                pass;
                
    try: # get fractional times and update the race accordingly
        a = fractional_times(path, raceID)
        print(a)
        for x in a:
            engine.execute(x)
    except UnboundLocalError:
        pass
    
            
    return None
