#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 12:48:01 2021

@author: williamsheehan
"""
import json
from itertools import product


# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=KEE&CTRY=USA&DT=11/07/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=CMR&CTRY=PR&DT=12/19/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=10&BorP=P&TID=KEE&CTRY=USA&DT=11/06/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=4&BorP=P&TID=GP&CTRY=USA&DT=01/02/2021&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=MTH&CTRY=USA&DT=09/15/2004&DAY=D&STYLE=EQB

url = "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?"

query_string = "RACE={}&BorP=P&TID={}&CTRY=USA&DT={}&DAY=D&STYLE=EQB"

# list of all possible dates
with open("dates.json", "r") as f:
    dates_list = json.load(f)  


with open("trackids1.json", 'r') as file:
    tracks_dict = json.load(file)

race_num_list = []

for i in range(1,15):
    race_num_str = str(i)
    race_num_list.append(race_num_str)

urls_with_race = []
for race in race_num_list:
    urls_with_race.append("RACE=" + race)
        
usa_urls_with_track = []
for track in tracks_dict['USA']:
    usa_urls_with_track.append("&BorP=P&TID=" + track + "&CTRY=USA&DT=")

can_urls_with_track = []    
for track in tracks_dict['CAN']:
    can_urls_with_track.append("&BorP=P&TID=" + track + "&CTRY=CAN&DT=")

pr_urls_with_track = []
for track in tracks_dict['PR']:
    pr_urls_with_track.append("&BorP=P&TID=" + track + "&CTRY=PR&DT=")
    
urls_with_track = usa_urls_with_track + can_urls_with_track + pr_urls_with_track

   
urls_with_date = []
for date in dates_list:
    urls_with_date.append(date + "&DAY=D&STYLE=EQB")


query_strings = [''.join(p) for p in product(urls_with_race, urls_with_track, urls_with_date)]

full_urls = []
for string in query_strings:
    full_urls.append("https://www.equibase.com/premium/eqbPDFChartPlus.cfm?" + string)
    





        
