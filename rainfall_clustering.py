# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 10:48:35 2019

@author: Dynaslope
"""

import rain_prototype4 as rp
import pandas as pd
import mysql.connector as sql
import numpy as np
import matplotlib.pyplot as plt
db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
                            user='root', password='senslope')

'''
Needs to look for the optimal rain gauge for each site
'''


def available_rg(site_code):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.rainfall_priorities"
    query += " INNER join senslopedb.rainfall_gauges as rg USING (rain_id)"
    query += " INNER join senslopedb.sites USING (site_id)"
    query += " WHERE site_code in ('%s')" %(site_code)
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['distance', 'gauge_name', 'site_code']]
    d.sort_values(by='distance', inplace=True)
    return d

def query_rain_noah(site,start,end):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.rain_noah_%s"%(site)
    query += " WHERE ts BETWEEN '%s' AND '%s'" %(start, end)
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['ts', 'rain']]
    d.loc[:, 'ts'] = d.loc[:, 'ts'].dt.round('30min')
    
    d.columns = ['ts_rain', 'rain']
    d['id'] = 1
    
    return d
    

site_codes = ['bar','bol','hin','ime','lip',
         'lpa','lte','lun','par','tal']
    
#site_codes = ['lpa','par']

start = '2017-01-01'
end = '2018-12-30'



############################################################################### rain parameters
gap = pd.Timedelta(hours=3)
min_rain = 1
days_landslide = pd.Timedelta(days=10)

final_df = pd.DataFrame()
################################################################################
for i in range(len(site_codes)):
    print('SITE = ',site_codes[i])
    gauges = available_rg(site_codes[i])
    df_rain = pd.DataFrame()
    for n in range(len(gauges)):
        print(gauges.gauge_name[n])
        
        try:
            rain = rp.query_rain(gauges.gauge_name[n], start, end)
            rain['site'] = site_codes[i]
            rain.loc[(rain['rain']>=50),'rain'] = 0
            rain.loc[(rain['rain']<=0),'rain'] = 0
        except:
            rain = query_rain_noah(gauges.gauge_name[n], start, end)
            rain.loc[(rain['rain']>=50),'rain'] = 0
            rain.loc[(rain['rain']<=0),'rain'] = 0
            
        df_rain = df_rain.append(rain).drop_duplicates('ts_rain',keep='first')
    
    df_rain.sort_values(by='ts_rain',inplace=True)
    df_rain.reset_index(inplace=True)

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
    

#for i in range(len(df_alerts)):
#    jj = pd.Timestamp(df_alerts.ts.values[i])
#    start_date = jj - days_landslide
#
#    j = 0
#    
#    while j < (len(gauges)):######################## check which rain gauge is best for analysis
#        try:
#            df_rain = rp.query_rain(gauges.gauge_name.values[j], start,end) #### query rain gauge
#            df_rain = df_rain.rename(columns={'ts_rain':'ts','rain':'rain','id':'id'})
#            df_rain.sort_values(by=['ts'],inplace=True)
#        except:
#            df_rain = query_rain_noah(gauges.gauge_name.values[j],start, end)
#            
#        temp_df = df_rain.loc[(df_rain.ts >= start_date)\
#                              &(df_rain.ts <= jj)]
#        
#        temp_df.sort_values(by=['ts'],inplace=True)
#        
#        if sum(temp_df.rain.values) < 10:
#            j+=1
#        else:
#            print(i,j)
#            break
    

#for i in range(len(sites)):
#    print(sites[i])
#    
#    try:
#        df_rain = rp.query_rain(sites[i], start,end)
#        df_rain.loc[(df_rain['rain']>=50),'rain'] = 0
#        df_rain.sort_values(by=['ts_rain'],inplace=True)
#        df_rain = df_rain.drop_duplicates(subset='ts_rain', keep='first')
#    except:
#        print('empty df')
#        continue
#    
#    try:
#        df_alerts = rp.query_alert(site_codes[i])
#    except:
#        print('no alerts')
#        continue            
#    
#    ts_alerts = list(df_alerts.ts_landslide)
#    ts_alerts.insert(0,df_rain.ts_rain[0])
#    ts_disc, rain_disc = rp.discretize(ts_alerts,df_rain,gap,min_rain)
#    final = rp.alerts(df_rain,ts_disc, rain_disc, ts_alerts,days_landslide,min_rain, to_plot=0)
#    final_df = pd.concat([final_df,final])
#    print('Length of final_df:',len(final_df))
#
#
#bayes = rp.bayesian(final_df,to_plot=1)
    
    