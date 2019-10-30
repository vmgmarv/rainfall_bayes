# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 10:48:35 2019
"""

import rain_prototype4 as rp
import pandas as pd
import mysql.connector as sql
import numpy as np
import matplotlib.pyplot as plt
import time
import seaborn as sns
start_time = time.time()

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
    

############################################################################### rain parameters
#gap = pd.Timedelta(hours=48)
#min_rain = 1.5
#days_landslide = pd.Timedelta(days=2)
###############################################################################
gap_itr = np.arange(pd.Timedelta(hours=24), pd.Timedelta(hours=50), pd.Timedelta(hours=5))
days_landslide_itr = np.arange(pd.Timedelta(days=2), pd.Timedelta(days=15), pd.Timedelta(days=1))
min_rain = 1.5

for w in range(len(gap_itr)):
    gap = gap_itr[w]
    
    for z in range(len(days_landslide_itr)):
        days_landslide = days_landslide_itr[z]
    
        site_codes = ['bar','bol','hin','ime','lip',
                 'lpa','lte','lun','par','tal']
            
        start = '2016-01-01'
        end = '2018-12-30'
        ###############################################################################
        final_df = pd.DataFrame()
        for i in range(len(site_codes)):
            gauges = available_rg(site_codes[i])
            df_rain = pd.DataFrame()
            for n in range(len(gauges)):
                
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
                df_alerts = rp.query_alert(site_codes[i]) ###### check if there are alerts
            except:
                continue            
            
            ts_alerts = list(df_alerts.ts_landslide)
            ts_alerts.insert(0,df_rain.ts_rain[0])
            
            ts_disc, rain_disc = rp.discretize(ts_alerts,df_rain,gap,min_rain)
            
            final = rp.alerts(df_rain,ts_disc, rain_disc, ts_alerts,days_landslide,min_rain, to_plot=0)
            final_df = pd.concat([final_df,final])
        
        bayes = rp.bayesian(final_df,to_plot=1)    
        
        bayes['d2'] = bayes.n_duration / np.timedelta64(1,'h')
        
#        bayes.to_csv(r'D:\rainfall_threshold\result_csv\gap_{}_tim_{}.csv'.format(w,z))
        
        print(w,z)

#palette = sns.color_palette("RdPu",10)
#sns.lmplot(data=bayes, x='d2',y='n_rain',hue='p_tot',fit_reg=False,aspect=1.5, palette=palette)
#print('{}'.format(bayes[bayes.p_tot > 0.00001]))    
#    
print("###### %s seconds ######" % (time.time() - start_time))
