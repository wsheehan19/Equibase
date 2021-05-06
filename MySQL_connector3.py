#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  5 16:25:14 2021

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
import datetime


cnx = mysql.connector.connect(user='root', password='Virginia2023!*',
                              host='localhost', database = 'equibase')
cursor = cnx.cursor()
# cursor.execute('CREATE TABLE racetable (ID int PRIMARY KEY AUTO_INCREMENT, RaceID VARCHAR(30) NOT NULL UNIQUE, RaceDate DATE NOT NULL, Track VARCHAR(50) NOT NULL,RaceNum tinyint NOT NULL, RaceType VARCHAR(300) NOT NULL, Distance VARCHAR(150) NOT NULL, Purse VARCHAR(30), ClaimingPrice VARCHAR(30), Weather VARCHAR(50) NOT NULL, TrackSpeed VARCHAR(15), StartType VARCHAR(50), OffAt TIME, FinalTime TIME(2), Fractional_Time_One TIME(2), Fractional_Time_Two TIME(2), Fractional_Time_Three TIME(2), Fractional_Time_Four TIME(2), Fractional_Time_Five TIME(2), Fractional_Time_Six TIME(2))')
# cursor.execute('CREATE TABLE horsetable(name VARCHAR(50) NOT NULL, HorseID int PRIMARY KEY AUTO_INCREMENT NOT NULL)')

# cursor.execute('CREATE TABLE HorsePPFtable (horsename VARCHAR(50) NOT NULL UNIQUE, HorseID int NOT NULL, FOREIGN KEY(HorseID) REFERENCES horsetable(HorseID),RaceID VARCHAR(30) NOT NULL, FOREIGN KEY(RaceID) REFERENCES racetable(RaceID), ID int, FOREIGN KEY(ID) REFERENCES racetable(ID), pgm smallint, startposition VARCHAR(50),three_sixteenths VARCHAR(50), qmile VARCHAR(50), three_eighths VARCHAR(50), 1halfmile VARCHAR(50),3qmile VARCHAR(50), 1mile VARCHAR(50),1andqmile VARCHAR(50),stretchone VARCHAR(50), stretch VARCHAR(50), Finish VARCHAR(50), Odds int, Trainer VARCHAR(50), RaceDate DATE, Track VARCHAR(50),RaceNum tinyint, RaceType VARCHAR(300), Distance VARCHAR(150), Purse VARCHAR(30), ClaimingPrice VARCHAR(30), Weather VARCHAR(50), TrackSpeed VARCHAR(15), StartType VARCHAR(50), OffAt TIME, FinalTime TIME(2), Fractional_Time_One TIME(2), Fractional_Time_Two TIME(2), Fractional_Time_Three TIME(2), Fractional_Time_Four TIME(2), Fractional_Time_Five TIME(2), Fractional_Time_Six TIME(2))')
# cursor.execute('CREATE TABLE RacePPFtable (horsename VARCHAR(50) NOT NULL UNIQUE, HorseID int NOT NULL, FOREIGN KEY(HorseID) REFERENCES horsetable(HorseID),RaceID VARCHAR(30) NOT NULL, FOREIGN KEY(RaceID) REFERENCES racetable(RaceID), ID int, FOREIGN KEY(ID) REFERENCES racetable(ID), pgm smallint, startposition VARCHAR(50),three_sixteenths VARCHAR(50), qmile VARCHAR(50), three_eighths VARCHAR(50), 1halfmile VARCHAR(50),3qmile VARCHAR(50), 1mile VARCHAR(50),1andqmile VARCHAR(50),stretchone VARCHAR(50), stretch VARCHAR(50), Finish VARCHAR(50), Odds int, Trainer VARCHAR(50))')


# drop_queries = ["DROP TABLE HorsePPFtable", "DROP TABLE RacePPFtable", "DROP TABLE racetable", "DROP TABLE horsetable"]
# for x in drop_queries:
#     cursor.execute(x)
#     cnx.commit()

#cursor.execute(q1)


# cursor.execute("DESCRIBE RacePPFtable")
# for x in cursor:
#     print(x)



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

def fractional_times(path, raceID):
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
    
    a = frac_times.count(':')
    b = frac_times.count('.')
    
    
    if b == 1:
        if a == 1:
            time_one = frac_times[:8].strip('\n')
        if a == 0:
            time_one = frac_times[:6].strip('\n')
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
    if b == 2:
        if a == 2:
            time_one = frac_times[:8].rstrip()
            time_two = frac_times[8:15].rstrip()
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:13].rstrip()
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
    if b == 3:
        if a == 3:
            time_one = frac_times[:8].strip('\n')
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
    if b == 4:
        if a == 4:
            time_one = frac_times[:8].strip('\n')
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
    
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
        
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        
    if b == 5:
        if a == 5:
            time_one = frac_times[:8].strip('\n')
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
            time_five = frac_times[29:36].rstrip()
        if a == 4:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
            time_five = frac_times[27:34].rstrip()
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
            time_five = frac_times[25:32].rstrip()
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
            time_five = frac_times[23:30].rstrip()
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:28].rstrip()
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        
    if b == 6:
        if a == 6:
            time_one = frac_times[:8].strip('\n')
            time_two = frac_times[8:15].rstrip()
            time_three = frac_times[15:22].rstrip()
            time_four = frac_times[22:29].rstrip()
            time_five = frac_times[29:36].rstrip()
            time_six = frac_times[36:43].rstrip()
        if a == 5:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:13].rstrip()
            time_three = frac_times[13:20].rstrip()
            time_four = frac_times[20:27].rstrip()
            time_five = frac_times[27:34].rstrip()
            time_six = frac_times[34:41].rstrip()
        if a == 4:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:18].rstrip()
            time_four = frac_times[18:25].rstrip()
            time_five = frac_times[25:32].rstrip()
            time_six = frac_times[32:39].rstrip()
        if a == 3:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:23].rstrip()
            time_five = frac_times[23:30].rstrip()
            time_six = frac_times[30:37].rstrip()
        if a == 2:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:28].rstrip()
            time_six = frac_times[28:35].rstrip()
        if a == 1:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = frac_times[26:33].rstrip()
        if a == 0:
            time_one = frac_times[:6].strip('\n')
            time_two = frac_times[6:11].rstrip()
            time_three = frac_times[11:16].rstrip()
            time_four = frac_times[16:21].rstrip()
            time_five = frac_times[21:26].rstrip()
            time_six = frac_times[26:31].rstrip()
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_One', value=time_one, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Two', value=time_two, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Three', value=time_three, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Four', value=time_four, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Five', value=time_five, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='racetable', column='Fractional_Time_Six', value=time_six, condition_col='RaceID', condition_val=raceID))
        frac_time_queries.append(update_query.format(table='HorsePPFtable', column='Fractional_Time_Six', value=time_six, condition_col='RaceID', condition_val=raceID))
        
    return frac_time_queries
        
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
    except IndexError:
        finaltime = 'Not Listed'

    
    racetable_string = "INSERT INTO racetable (RaceID, RaceNum, RaceDate, Track, RaceType, Distance, ClaimingPrice, Purse, Weather, TrackSpeed, OffAt, StartType, FinalTime) VALUES ('{raceID}', '{racenum}', '{date}', '{track}', '{racetype}', '{distance}', '{claimingprice}', '{purse}', '{weather}', '{trackspeed}', '{offat}', '{start}', '{finaltime}')"
    queries.append(racetable_string.format(raceID=raceID, racenum=racenum, date=date, track=track, racetype=racetype, distance=distance, claimingprice=claimingprice, purse=purse, weather=weather, trackspeed=trackspeed, offat=offat, start=start, finaltime=finaltime))
    
        
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
                odds.append(x)
            else:
                pass
        try:
           df['Odds'] = odds
        except ValueError:
            return None
    except IndexError:
        odds = []   
    
    try:
        trainers = []    
        for trainer in get_trainers(path):
            a = trainer.replace("'", "")
            trainers.append(a.strip('\n'))
            
            
        df['Trainer'] = trainers
    except:
        print('Cant get trainers')
        pass

    
    df.rename(columns={'Start': 'startposition', 'Str': 'stretch', 'Fin': 'Finish'}, inplace=True)
    if '3/16' in df.columns:
        df.rename(columns={'3/16': 'three_sixteenths'}, inplace=True)
    if '1/4' in df.columns:
        df.rename(columns={'1/4': 'qmile'}, inplace=True)
    if '3/8' in df.columns:
        df.rename(columns={'3/8': 'three_eighths'}, inplace=True)
    if '1/2' in df.columns:
        df.rename(columns={'1/2': '1halfmile'}, inplace=True)
    if '3/4' in df.columns:
        df.rename(columns={'3/4': '3qmile'}, inplace=True)
    if '1m' in df.columns:
        df.rename(columns={'1m': '1mile'}, inplace=True)
    if '11/4' in df.columns:
        df.rename(columns={'11/4': '1andqmile'}, inplace=True)
    if 'Str 1' in df.columns:
        df.rename(columns={'Str 1': 'stretchone'}, inplace=True)
    if 'Str1' in df.columns:
        df.rename(columns={'Str1': 'stretchone'}, inplace=True)
       
    
    horse_dict = df.to_dict('records')
    
    raceinfo_string = "INSERT INTO {table} (horsename, RaceNum, RaceDate, Track, RaceType, Distance, ClaimingPrice, Purse, Weather, TrackSpeed, OffAt, StartType, FinalTime, RaceID, HorseID, ID) VALUES ('{horsename}', '{racenum}', '{date}', '{track}', '{racetype}', '{distance}', '{claimingprice}', '{purse}', '{weather}', '{trackspeed}', '{offat}', '{start}', '{finaltime}', '{raceID}', (SELECT HorseID FROM horsetable WHERE name='{horsename}'), (SELECT ID FROM racetable WHERE RaceID='{raceID}'))"
    
    
    for item in horse_dict:
        horsename = item['horsename'].replace("'", "")
        queries.append("INSERT INTO horsetable (name) VALUES ('{horsename}')".format(horsename=horsename))
        queries.append(raceinfo_string.format(table='HorsePPFtable', racenum=racenum, date=date, track=track, racetype=racetype, distance=distance, claimingprice=claimingprice, purse=purse, weather=weather, trackspeed=trackspeed, offat=offat, start=start, finaltime=finaltime, horsename=horsename, raceID=raceID))
        queries.append("INSERT INTO RacePPFtable (horsename, RaceID, HorseID, ID) VALUES ('{horsename}', '{raceID}', (SELECT HorseID FROM horsetable WHERE name='{horsename}'), (SELECT ID FROM racetable WHERE RaceID='{raceID}'))".format(raceID=raceID, horsename=horsename))
        for key in item:
            value = item[key]
            #queries.append(update_query.format(table='HorsePPFtable', column=key, value=value, condition='horsename', value=horsename))
            if 'horsename' not in key:
                queries.append("UPDATE HorsePPFtable SET {col} = '{val}' WHERE RaceID = '{raceID}' AND horsename = '{horsename}'".format(col=key, val=value, raceID=raceID, horsename=horsename))
                queries.append("UPDATE RacePPFtable SET {col} = '{val}' WHERE RaceID = '{raceID}' AND horsename = '{horsename}'".format(col=key, val=value, raceID=raceID, horsename=horsename))
            else:
                pass;
                
    try:
        a = fractional_times(path, raceID)
        for x in a:
            queries.append(x)
    except UnboundLocalError:
        pass
            
    return queries
    
path = '/Users/williamsheehan/Documents/Equibase Charts/'
folder = os.fsencode(path)
filenames = []
for file in os.listdir(folder):
    filename = os.fsdecode(file)
    if filename.endswith('.pdf'): 
        filenames.append(filename)
# filenames.sort() 
pathnames = []
for file in filenames:
    pathname = path + file
    pathnames.append(pathname)

test_pdfs = ['/Users/williamsheehan/Documents/TestUploads/RACE=2&BorP=P&TID=WRD&CTRY=USA&DT=04.pdf',
             '/Users/williamsheehan/Documents/TestUploads/RACE=4&BorP=P&TID=CRC&CTRY=USA&DT=01.pdf',
             '/Users/williamsheehan/Documents/TestUploads/RACE=5&BorP=P&TID=CRC&CTRY=USA&DT=01.pdf',
             '/Users/williamsheehan/Documents/TestUploads/RACE=5&BorP=P&TID=MNR&CTRY=USA&DT=01.pdf',
             '/Users/williamsheehan/Documents/TestUploads/RACE=9&BorP=P&TID=FON&CTRY=USA&DT=04.pdf',
             ]
for pdf in test_pdfs:
    print(pdf)
    print(datetime.datetime.now())
    a = info_and_upload(pdf)
    if a is not None:
        for query in a:
            cursor.execute(query)
            cnx.commit()
            time.sleep(3)
    else:
        print('Camelot cannot read PDF:' + pdf)

#info_and_upload('/Users/williamsheehan/Documents/TestUploads/RACE=5&BorP=P&TID=CRC&CTRY=USA&DT=01.pdf')
#info_and_upload('/Users/williamsheehan/Documents/Equibase Charts/RACE=1&BorP=P&TID=CD&CTRY=USA&DT=04.pdf')
#info_and_upload('/Users/williamsheehan/Documents/Equibase Charts/RACE=7&BorP=P&TID=EVD&CTRY=USA&DT=04.pdf')
        

        
        