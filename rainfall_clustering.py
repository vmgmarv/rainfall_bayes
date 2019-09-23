# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 10:48:35 2019

@author: Dynaslope
"""

import rain_prototype4 as rp
import pandas as pd

'''
Needs to look for the optimal rain gauge for each site
'''

site_codes = ['bar','bol','hin','ime','lip',
         'lpa','lte','lun','par','tal']

sites = ['bartb','bolra','hinsa','imera','lipra',
              'lpasa','lteg','lung','parta','talsa']
start = '2017-01-01'
end = '2018-12-30'

############################################################################### rain parameters
gap = pd.Timedelta(hours=50)
min_rain = 1.5
days_landslide = pd.Timedelta(days=15)

final_df = pd.DataFrame()
###############################################################################
for i in range(len(sites)):
    print(sites[i])
    
    try:
        df_rain = rp.query_rain(sites[i], start,end)
        df_rain.loc[(df_rain['rain']>=50),'rain'] = 0
        df_rain.sort_values(by=['ts_rain'],inplace=True)
        df_rain = df_rain.drop_duplicates(subset='ts_rain', keep='first')
    except:
        print('empty df')
        continue
    
    try:
        df_alerts = rp.query_alert(site_codes[i])
    except:
        print('no alerts')
        continue            
    
    ts_alerts = list(df_alerts.ts_landslide)
    ts_alerts.insert(0,df_rain.ts_rain[0])
    ts_disc, rain_disc = rp.discretize(ts_alerts,df_rain,gap,min_rain)
    final = rp.alerts(df_rain,ts_disc, rain_disc, ts_alerts,days_landslide,min_rain, to_plot=0)
    
    final_df = pd.concat([final_df,final])


bayes = rp.bayesian(final_df,to_plot=1)
    
    