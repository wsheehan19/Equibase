#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 12:48:01 2021

@author: williamsheehan
"""
import json
import datetime
import pandas as pd


# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=KEE&CTRY=USA&DT=11/07/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=1&BorP=P&TID=CMR&CTRY=PR&DT=12/19/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=10&BorP=P&TID=KEE&CTRY=USA&DT=11/06/2020&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=4&BorP=P&TID=GP&CTRY=USA&DT=01/02/2021&DAY=D&STYLE=EQB

# https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=MTH&CTRY=USA&DT=09/15/2004&DAY=D&STYLE=EQB

url = "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?"

query_string = "RACE={}&BorP=P&TID={}&CTRY=USA&DT={}&DAY=D&STYLE=EQB"

race_num_list = []

date1 = '01/01/2000'
date2 = '03/01/2021'
date_list = pd.date_range(date1, date2).tolist()
new_dates = []
newer_dates = []
newest_dates = []
for date in date_list:
    new_dates.append(str(date).split(',')[0])
    
        
print(new_dates)
# json_obj = json.dumps(new_dates)    
# with open('dates.json', 'w+') as fp:
#     fp.write(json_obj)
    
    


     


with open("trackids1.json", 'r') as file:
    data = json.load(file)



for i in range(1,15):
    race_num_str = "RACE=" + str(i)
    race_num_list.append(race_num_str)


#print(data)
#print(race_num_list)
#print(date_list)
