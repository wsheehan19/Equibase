#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 13:15:50 2021

@author: williamsheehan
"""
import camelot
import pandas as pd
import numpy as np

# pdf = "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus.pdf"
# pdf1 =  "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (4).pdf"

pdfs = ["/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus.pdf",
        "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (1).pdf",
        "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (2).pdf",
        "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (3).pdf",
        "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (4).pdf"]

def get_loc(section, text):
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    a = result[1]
    b = result[0]
    
    return section[a][b]



out_dict = {}

def read(path):

    tables = camelot.read_pdf(path, pages='all', flavor='stream',
                              multiple_tables = True, flag_size = True
                              )
    
    # sections of pdf
    intro_and_chart = tables[0].df
    # odds = tables[1].df
    # past_pf = tables[2].df
    
    racename_loc = get_loc(intro_and_chart, '-Race')
    racename = racename_loc
    print('Race Name: ' + racename)
    try:
        racetype_loc = get_loc(intro_and_chart, 'FOR')
        racetype = racetype_loc.split('.')[0]
        print('Race Type: ' + racetype)
    except IndexError:
        racetype = 'No Stipulations'
    
    try:
        distance_loc = get_loc(intro_and_chart, 'Miles')
        distance = distance_loc.split('Current')[0]
        print('Distance: ' + distance)
    except IndexError:
        distance_loc = get_loc(intro_and_chart, 'Furlongs')
        distance = distance_loc.split('Current')[0]
        print('Distance: ' + distance)
    
    try:
        cprice_loc = get_loc(intro_and_chart, 'Claiming Price')
        claimingprice = cprice_loc.split(':')[-1]
        print('Claiming Price: ' + claimingprice)
    except IndexError:
        print('Not a Claiming Race')
        claimingprice = 'N/A'
        
        
    purse_loc =  get_loc(intro_and_chart, 'Purse:')
    purse = purse_loc.split(':')[-1]
    print('Purse: ' + purse)
    
    
    weather_loc = get_loc(intro_and_chart, 'Weather:')
    trackspeed_loc = get_loc(intro_and_chart, 'Track:')
    if 'Track' in str(weather_loc):
        weather = weather_loc.split('Track')[0].split(' ')[-1]
    
    else:
        weather = weather_loc.split(': ')[-1]
    
    print('Weather: ' + weather)
    
    trackspeed = trackspeed_loc.split(': ')[-1]
    print('Trackspeed: ' + trackspeed)
    
    offat_loc = get_loc(intro_and_chart, 'Off at:')
    start_loc = get_loc(intro_and_chart, 'Off at:')
    if 'Start' in offat_loc:
        offat = offat_loc.split('Start')[0].split('at: ')[-1]
    else:
        offat = offat_loc.split(':')[-1]
        
    start = start_loc.split(': ')[-1]
    
    print('Off at: ' + offat)
    print('Start: ' + start)
    
    finaltime_loc = get_loc(intro_and_chart, 'Final Time:')
    if "(New Track Record)" in finaltime_loc:
        finaltime = finaltime_loc.split('Time: ')[-1].split(' (')[0] 
    else:
        finaltime = finaltime_loc.split('Time: ')[-1]
    print('Final Time: ' + finaltime)
                      
    temp_race_dict = {'race_type': racetype, 'distance': distance,
                      'purse': purse, 'claiming_price': claimingprice,
                      'weather': weather, 'track_speed': trackspeed,
                      'off_at': offat, 'start': start, 'final_time': finaltime}

    out_dict.update({racename: temp_race_dict})
    
    return temp_race_dict

for pdf in pdfs:    
    read(pdf)


print(out_dict)


# RACE INFO CHECKLIST

# racename: check NEEDS FORMATTING
# racetype: check
# distance: check
# purse: check
# claimingprice: check
# weather: check
# trackspeed: check
# offat: check
# start: check
# fractional times: doesnt read this ??
# finaltime: check











