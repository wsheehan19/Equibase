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

def get_index(section, text):
    mask = np.column_stack([section[col].str.contains(text, na=False) for col in section])
    find_result = np.where(mask==True)
    result = [find_result[0][0], find_result[1][0]]
    
    return result

out_dict = {}


def horse_info(path):
    tables = camelot.read_pdf(path, pages='all', flavor='stream',
                              multiple_tables = True, flag_size = True
                              )
    
    # sections of pdf
    intro_and_chart = tables[0].df
    odds = tables[1].df
    past_pf = tables[2].df
    racename_loc = get_loc(intro_and_chart, '-Race')
    racename = racename_loc
    
    topleft = get_index(past_pf, 'Pgm')
    topright = get_index(past_pf, 'Fin')
    
    
    temp_dict = {}
    
    
    arrays = past_pf[topleft[0]:].values
    horses = []    
    keys = arrays[0]
    
    temp_horse_dict = dict.fromkeys(keys)
    
    print(keys)
    
    for arr in arrays[1:]:
        name = arr[1]
        for n in range(2, len(arr)):
            pgm = arr[0]
            value = arr[n]
            key = keys[n]
            temp_horse_dict[keys[0]] = pgm
            temp_horse_dict[key] = value
            temp_dict[name] = temp_horse_dict
    
     
    print(temp_dict)           
    
    # for array in arrays:
    #     horsename = array[1]
    #     horses.append(horsename)
        
    #     for n in range(len(array)):
    #         value = array[n]
    #         key = arrays[0][n]
    #         if len(key) > 2:
    #             temp_dict[key] = value
    #             print(temp_dict)
    #             temp_horse_dict.update({horsename: temp_dict})
    #         else:
    #             key = 'Inplace'
    #             temp_dict[key] = value
    #             print(temp_dict)
    #             temp_horse_dict.update({horsename: temp_dict})
            
        
    #print(temp_horse_dict)

horse_info(pdf)