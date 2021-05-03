#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 25 15:04:36 2021

@author: williamsheehan
"""

import mysql.connector
from mysql.connector import errorcode
import camelot
import pandas as pd
import numpy as np
import PyPDF2
import os
import numbers
import re
from dateutil import parser
import time


cnx = mysql.connector.connect(user='root', password='Virginia2023!*',
                              host='localhost', database = 'equibase')
cursor = cnx.cursor()
#cursor.execute('CREATE TABLE racetable (RaceID VARCHAR(30) NOT NULL, PRIMARY KEY(RaceID), RaceDate DATE NOT NULL, Track VARCHAR(50) NOT NULL,RaceNum tinyint NOT NULL, RaceType VARCHAR(300) NOT NULL, Distance VARCHAR(150) NOT NULL, Purse int UNSIGNED, ClaimingPrice int UNSIGNED, Weather VARCHAR(50) NOT NULL, TrackSpeed VARCHAR(15) NOT NULL, StartType VARCHAR(50), OffAt TIME NOT NULL, FinalTime int NOT NULL, Fractional_Time_One int NOT NULL, Fractional_Time_Two int, Fractional_Time_Three int, Fractional_Time_Four int, Fractional_Time_Five int, Fractional_Time_Six int)')
#cursor.execute('CREATE TABLE horsetable(name VARCHAR(50) NOT NULL, HorseID int PRIMARY KEY AUTO_INCREMENT NOT NULL)')
#cursor.execute('CREATE TABLE RacePPFtable(RaceID VARCHAR(15), FOREIGN KEY (RaceID) REFERENCES racetable(RaceID), horsename VARCHAR(50) NOT NULL, pgm smallint NOT NULL, startposition VARCHAR(50) NOT NULL, qmile VARCHAR(50), 1halfmile VARCHAR(50),3qmile VARCHAR(50), 1mile VARCHAR(50),1andqmile VARCHAR(50), stretch VARCHAR(50) NOT NULL, Finish VARCHAR(50) NOT NULL, Odds int NOT NULL, Trainer VARCHAR(50) NOT NULL)')
#q1 = 'CREATE TABLE HorsePPFtable (horsename VARCHAR(50) NOT NULL, HorseID int, FOREIGN KEY(HorseID) REFERENCES horsetable(HorseID),RaceID VARCHAR(15), FOREIGN KEY(RaceID) REFERENCES racetable(RaceID), pgm smallint NOT NULL, startposition VARCHAR(50) NOT NULL, qmile VARCHAR(50), 1halfmile VARCHAR(50),3qmile VARCHAR(50), 1mile VARCHAR(50),1andqmile VARCHAR(50), stretch VARCHAR(50) NOT NULL, Finish VARCHAR(50) NOT NULL, Odds int NOT NULL, Trainer VARCHAR(50) NOT NULL, RaceDate DATE NOT NULL, Track VARCHAR(50) NOT NULL,RaceNum tinyint NOT NULL, RaceType VARCHAR(300) NOT NULL, Distance VARCHAR(150) NOT NULL, Purse int UNSIGNED, ClaimingPrice int UNSIGNED, Weather VARCHAR(50) NOT NULL, TrackSpeed VARCHAR(15) NOT NULL, StartType VARCHAR(50), OffAt TIME NOT NULL, FinalTime int NOT NULL, Fractional_Time_One int NOT NULL, Fractional_Time_Two int, Fractional_Time_Three int, Fractional_Time_Four int, Fractional_Time_Five int, Fractional_Time_Six int)'
#cursor.execute("DROP TABLE HorsePPFtable")
#cursor.execute("DROP TABLE RacePPFtable")
#cursor.execute("DROP TABLE racetable")



def get_loc(section, text):
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    a = result[1]
    b = result[0]
    
    return section[a][b]

def get_index(section, text):
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    
    return result

def coerce_to_numeric(value):
    if isinstance(value, numbers.Number):
        return value
    else:
        return np.NaN

def get_fractional_times(path):
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
    
    a = frac_times.count(':')
    b = frac_times.count('.')
    
    if b <= 4:
        if a == 4:
            time_one = frac_times[:8].rstrip()
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
    
        if a == 3:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
        
        if a == 2:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
        if a == 1:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
        if a == 0:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
        return [time_one, time_two, time_three, time_four]
    if b == 5:
        if a == 5:
            time_one = frac_times[:8].rstrip()
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
            time_five = frac_times[29:36].rstrip()
        if a == 4:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
            time_five = frac_times[27:34].rstrip()
        if a == 3:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
            time_five = frac_times[25:32].rstrip()
        if a == 2:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
            time_five = frac_times[23:30].rstrip()
        if a == 1:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:28].rstrip()
        if a == 0:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
        return [time_one, time_two, time_three, time_four, time_five]           
    if b == 6:
        if a == 6:
            time_one = frac_times[:8].rstrip()
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
            time_five = frac_times[29:36].rstrip()
            time_six = frac_times[36:43].rstrip()
        if a == 5:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
            time_five = frac_times[27:34].rstrip()
            time_six = frac_times[34:41].rstrip()
        if a == 4:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
            time_five = frac_times[25:32].rstrip()
            time_six = frac_times[32:39].rstrip()
        if a == 3:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
            time_five = frac_times[23:30].rstrip()
            time_six = frac_times[30:37].rstrip()
        if a == 2:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:28].rstrip()
            time_six = frac_times[28:35].rstrip()
        if a == 1:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = frac_times[26:33].rstrip()
        if a == 0:
            time_one = frac_times[:6].rstrip()
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = frac_times[26:31].rstrip()
        return [time_one, time_two, time_three, time_four, time_five, time_six] 

out_dict = {}

horse_dictionary = {}

def get_trainers(path):
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
    queries = []
    
    insert_query = "INSERT INTO {table} ({column}) VALUES ('{value}')"
    
    tables = camelot.read_pdf(path, pages='all', flavor='stream',
                              multiple_tables = True, flag_size = True
                              )
    
    intro_and_chart = tables[0].df
    if len(intro_and_chart.columns) < 4:
        
        return None
 
    
    TID = path.split('TID=')[-1].split('&CTRY')[0]
    racename_loc = get_loc(intro_and_chart, '-Race')
    racename = racename_loc
    a = racename.split('<')[0]
    b = racename.split('>')[-1]
    race = a+b
    
    date_str = race.split('-')[1]
    date = parser.parse(date_str).date()
    queries.append(insert_query.format(table='racetable', column='RaceDate', value=date))
    
    racenum = race[-2:]
    if check_letters(racenum) == True:
        racenum = racenum
    else:
        racenum = race[-1]
    queries.append(insert_query.format(table='racetable', column='RaceNum', value=racenum))
        
    track = race.split('-')[0]
    queries.append(insert_query.format(table='racetable', column='Track', value=track))
    
    raceID = TID + '-' + str(date) + '-' + str(racenum)
    print(raceID)
    queries.append(insert_query.format(table='racetable', column='RaceID', value=raceID))
    
    
    
    try:
        racetype_loc = get_loc(intro_and_chart, 'FOR')
        racetype = racetype_loc.split('.')[0]
    except IndexError:
        racetype = 'No Stipulations'
    queries.append(insert_query.format(table='racetable', column='RaceType', value=racetype))

    
    try:
        distance_loc = get_loc(intro_and_chart, 'Miles')
        distance = distance_loc.split('Current')[0]
    except IndexError:
        try:
            distance_loc = get_loc(intro_and_chart, 'Furlongs')
            distance = distance_loc.split('Current')[0]
        except IndexError:
            distance_loc = get_loc(intro_and_chart, 'Yards')
            distance = distance_loc.split('Current')[0]
    queries.append(insert_query.format(table='racetable', column='Distance', value=distance))
    
    try:
        cprice_loc = get_loc(intro_and_chart, 'Claiming Price')
        claimingprice = cprice_loc.split(':')[-1]
    except IndexError:
        claimingprice = 'N/A'
    queries.append(insert_query.format(table='racetable', column='ClaimingPrice', value=claimingprice))  
        
    purse_loc =  get_loc(intro_and_chart, 'Purse:')
    purse = purse_loc.split(':')[-1]
    queries.append(insert_query.format(table='racetable', column='Purse', value=purse))
    
    weather_loc = get_loc(intro_and_chart, 'Weather:')
    trackspeed_loc = get_loc(intro_and_chart, 'Track:')
    if 'Track' in str(weather_loc):
        weather = weather_loc.split('Track')[0].split(' ')[-1]
    
    else:
        weather = weather_loc.split(': ')[-1]
    queries.append(insert_query.format(table='racetable', column='Weather', value=weather))
    
    trackspeed = trackspeed_loc.split(': ')[-1]
    queries.append(insert_query.format(table='racetable', column='TrackSpeed', value=trackspeed))
    
    offat_loc = get_loc(intro_and_chart, 'Off at:')
   
    if 'Start' in offat_loc:
        offat = offat_loc.split('Start')[0].split('at: ')[-1]
    else:
        offat = offat_loc.split(':')[-1]
    queries.append(insert_query.format(table='racetable', column='OffAt', value=offat))
    
    start_loc = get_loc(intro_and_chart, 'Off at:')
    if 'Timer' in start_loc:    
        start = start_loc.split(': ')[-2]
    else:
        start = start_loc.split(': ')[-1]
    queries.append(insert_query.format(table='racetable', column='StartType', value=start))
    
    
    try:
        finaltime_loc = get_loc(intro_and_chart, 'Final Time:')
        if "(New Track Record)" in finaltime_loc:
            finaltime = finaltime_loc.split('Time: ')[-1].split(' (')[0] 
        else:
            finaltime = finaltime_loc.split('Time: ')[-1]
    except IndexError:
        finaltime = 'Not Listed'
    queries.append(insert_query.format(table='racetable', column='FinalTime', value=finaltime))
    
    
    try:
        fractional_times = get_fractional_times(path)
    except:
        fractional_times = ''
    
    num_ftimes = len(fractional_times)     
    if num_ftimes == 6:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
        ftime2 = fractional_times[1]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Two', value=ftime2))
        ftime3 = fractional_times[2]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Three', value=ftime3))
        ftime4 = fractional_times[3]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Four', value=ftime4))
        ftime5 = fractional_times[4]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Five', value=ftime5))
        ftime6 = fractional_times[5]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Six', value=ftime6))
    if num_ftimes == 5:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
        ftime2 = fractional_times[1]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Two', value=ftime2))
        ftime3 = fractional_times[2]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Three', value=ftime3))
        ftime4 = fractional_times[3]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Four', value=ftime4))
        ftime5 = fractional_times[4]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Five', value=ftime5))
    if num_ftimes == 4:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
        ftime2 = fractional_times[1]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Two', value=ftime2))
        ftime3 = fractional_times[2]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Three', value=ftime3))
        ftime4 = fractional_times[3]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Four', value=ftime4))
    if num_ftimes == 3:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
        ftime2 = fractional_times[1]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Two', value=ftime2))
        ftime3 = fractional_times[2]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Three', value=ftime3))
    if num_ftimes == 2:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
        ftime2 = fractional_times[1]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_Two', value=ftime2))
    if num_ftimes == 1:
        ftime1 = fractional_times[0]
        queries.append(insert_query.format(table='racetable', column='Fractional_Time_One', value=ftime1))
    else:
        print('No Fractional Times')
    # HORSE PF INFO                   
    try:
        past_pf = tables[2].df
    except IndexError:
        past_pf = tables[1].df
    
    topleft = get_index(past_pf, 'Pgm')
 
    
    arrays = past_pf[topleft[0]:].values
    
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
            odds.append(x)
        else:
            pass
            
    data=arrays[1:]
    columns=arrays[0]
    df = pd.DataFrame(np.vstack(arrays))
    df = pd.DataFrame(data=data,
                      columns=columns,
                      )
    
    if columns[0] == 'Pgm Horse Name':
        df.rename(columns={'Pgm Horse Name': 'pgm', '': 'horsename'}, inplace=True)
    
    if columns[1] == 'Pgm Horse Name':
        df.rename(columns={'': 'Pgm', 'Pgm Horse Name': 'horsename'}, inplace=True)
    
    
    df['Odds'] = odds    
    df['Trainer'] = get_trainers(path)
    
    df.rename(columns={'Start': 'startposition', '1/4': 'qmile', 'Str': 'stretch', 'Fin': 'Finish'}, inplace=True)
    if '1/2' in df.columns:
        df.rename(columns={'1/2': '1halfmile'}, inplace=True)
    if '3/4' in df.columns:
        df.rename(columns={'3/4': '3qmile'}, inplace=True)
    if '1m' in df.columns:
        df.rename(columns={'1m': '1mile'}, inplace=True)
    if '11/4' in df.columns:
        df.rename(columns={'11/4': '1andqmile'}, inplace=True)
       
    
    horse_dict = df.to_dict('records')
    
    horsenames = []
    for item in horse_dict:
        horsename = item['horsename']
        horsenames.append(horsename)
        queries.append(insert_query.format(table='horsetable', column='name', value=horsename))
        for key in item:
            value = item[key]
            # queries.append(insert_query.format(table='RacePPFtable', column=key, 'RaceID', value=[value, raceID]))
            # queries.append(insert_query.format(table='HorsePPFtable', column=key, 'RaceID', value=[value, raceID]))
            queries.append("INSERT INTO HorsePPFtable ({key}) VALUES ('{value}')".format(key=key, value=value))
            queries.append("INSERT INTO RacePPFtable ({key}) VALUES ('{value}')".format(key=key, value=value))
        queries.append("INSERT INTO HorsePPFtable (RaceID) VALUES ('{RaceID}')".format(RaceID=raceID))
        queries.append("INSERT INTO RacePPFtable (RaceID) VALUES ('{RaceID}')".format(RaceID=raceID))

    
                
    for query in queries:
        print(query)
        cursor.execute(query)
        time.sleep(2)
    
    return None
    
path = '/Users/williamsheehan/Documents/Equibase Charts/'
folder = os.fsencode(path)
filenames = []
for file in os.listdir(folder):
    filename = os.fsdecode(file)
    if filename.endswith('.pdf'): 
        filenames.append(filename)
filenames.sort() 
pathnames = []
for file in filenames:
    pathname = path + file
    pathnames.append(pathname)


    
#cursor.execute("INSERT INTO racetable (RaceID) VALUES ('FG%20-1998-02-06-1')")