#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 14:17:52 2021

@author: williamsheehan
"""

from requests_tor import RequestsTor
import time

rt = RequestsTor(tor_ports=[9050, 9000, 9001, 9002, 9003, 9004], tor_cport=9051, password='password93', autochange_id=1)



urls = ['https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=11&BorP=P&TID=SAR&CTRY=USA&DT=08/26/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=3&BorP=P&TID=AQU&CTRY=USA&DT=04/29/2006&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=7&BorP=P&TID=BEL&CTRY=USA&DT=10/29/2005&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=4&BorP=P&TID=LS&CTRY=USA&DT=10/30/2004&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=BEL&CTRY=USA&DT=09/07/2003&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=SA&CTRY=USA&DT=03/01/2008&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=CD&CTRY=USA&DT=05/05/2007&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=SA&CTRY=USA&DT=03/06/2021&DAY=D&STYLE=EQB',
        'https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=SA&CTRY=USA&DT=02/07/2021&DAY=D&STYLE=EQB',
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=12&BorP=P&TID=GP&CTRY=USA&DT=01/23/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=05/30/2005&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=5&BorP=P&TID=DED&CTRY=USA&DT=01/19/2021&DAY=D&STYLE=EQB", 
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=6&BorP=P&TID=AQU&CTRY=USA&DT=02/21/2021&DAY=D&STYLE=EQB",
        "https://www.equibase.com/premium/eqbPDFChartPlus.cfm?RACE=9&BorP=P&TID=BEL&CTRY=USA&DT=10/10/2020&DAY=D&STYLE=EQB"]



# rt.get_urls(urls)
# results = rt.get_urls(urls)
# for result in results:
#     print(result.status_code)
#     print(result.url)


for url in urls:
    rt.get(url)
    r = rt.get(url)
    print(r.status_code)
    time.sleep(5)