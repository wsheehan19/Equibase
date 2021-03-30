#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 14:58:18 2021

@author: williamsheehan
"""
import camelot
import pandas as pd
import numpy as np

pdf = "/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus.pdf"

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

def get_index(section, text):
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    
    return result

horse_dict = {}


def horse_info(path):
    tables = camelot.read_pdf(path, pages='all', flavor='stream',
                              multiple_tables = True, flag_size = True
                              )
    
    # sections of pdf
    intro_and_chart = tables[0].df
    try:
        past_pf = tables[2].df
    except IndexError:
        past_pf = tables[1].df
    

    intro_and_chart.to_csv('test.csv')
    racename_loc = get_loc(intro_and_chart, '-Race')
    racename = racename_loc
    
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
        
    data=arrays[1:]
    columns=arrays[0]
    df = pd.DataFrame(np.vstack(arrays))
    df = pd.DataFrame(data=data,
                      columns=columns,
                      )
    df.rename(columns={'Pgm Horse Name': 'Pgm', '': 'Horse Name'}, inplace=True)
    df['Odds'] = odds_list
    
    
    horse_dict = df.to_dict('records')
    
    
    temp_dict = {}
    for horse in horse_dict:
        key = horse['Horse Name']
        value = horse
        temp_dict[key] = value
    
            
    horse_dict[racename] = temp_dict
    
    return temp_dict

     


#horse_info("/Users/williamsheehan/Documents/Equibase Charts/eqbPDFChartPlus (4).pdf")
for pdf in pdfs:
    print(pdf)
    horse_info(pdf)
    
    
print(horse_dict)