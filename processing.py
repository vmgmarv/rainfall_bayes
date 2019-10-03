# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 08:56:10 2019
"""
import mysql.connector as sql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
os.path.abspath('D:\\rainfall_threshold\\result_csv') 
sys.path.insert(1,os.path.abspath('D:\\rainfall_threshold\\rainfall_bayes'))
import rain_prototype4 as rp

db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
                            user='root', password='senslope')

#db_connection = sql.connect(host='192.168.150.75', database='senslopedb', 
#                            user='pysys_local', password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg')

#db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
#                            user='root', password='alienware091394')

def query_alert(site_code):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.public_alert_trigger as PA"
    query += " INNER join senslopedb.public_alert_event as PV using (event_id)"
    query += " INNER join senslopedb.sites USING (site_id)"
    query += " WHERE site_code = '%s'" %(site_code)
    query += " AND trigger_type != 'R'"
    query += " AND trigger_type != 'D'"
    query += " AND trigger_type != 'E'"
    query += " AND timestamp >= '2017-02-01'"
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['trigger_type', 'timestamp']]
    d.columns = ['trigger_type', 'ts_landslide']
    d.loc[:, 'ts_landslide'] = d.loc[:, 'ts_landslide'].dt.round('30min')
    d.loc[:, 'id'] = 1
    return d

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


gap_itr = np.arange(pd.Timedelta(hours=24), pd.Timedelta(hours=50), pd.Timedelta(hours=5))
days_landslide_itr = np.arange(pd.Timedelta(days=2), pd.Timedelta(days=15), pd.Timedelta(days=1))
min_rain = 1.5

site_codes = 'par'
df_alerts = query_alert(site_codes)
df = pd.read_csv('gap_3_tim_11.csv')

a3 = pd.Timestamp('2017-12-20 16:30:00')

gg = pd.DataFrame()
mm = pd.DataFrame()

gaps_ = []
leads_ = []
p_ave = []
c_rain = []
for w in range(len(gap_itr)):
    gap = gap_itr[w]
    
    for z in range(len(days_landslide_itr)):
        days_landslide = days_landslide_itr[z]

        site_codes = ['par']
            
        start = '2017-11-01'
        end = '2018-02-20'
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
                df_alerts = pd.Timestamp(a3) ###### check if there are alerts
            except:
                continue            
            
            ts_alerts = [df_rain.ts_rain[0], df_alerts, pd.Timestamp('2017-12-20 10:30:00')]
            
            ts_disc, rain_disc = rp.discretize(ts_alerts,df_rain,gap,min_rain)
            
            final = rp.alerts(df_rain,ts_disc, rain_disc, ts_alerts,days_landslide,min_rain, to_plot=0)
            final_df = pd.concat([final_df,final])
            
            gg= pd.concat([gg,final_df])
            
            
#            
#        print(gap,days_landslide)
#        
        bayes = rp.bayesian(final_df,to_plot=0)
        mm = pd.concat([mm, bayes])
        temp_df = bayes[bayes.alerts == 1]
        
        if temp_df.empty:
            p_ave.append(0.0)
            c_rain.append(0)
        else:
            p_ave.append(temp_df.p_tot.mean())
            c_rain.append(temp_df.n_rain.values[0])
        gaps_.append(gap_itr[w])
        leads_.append(days_landslide_itr[z])
#        
#        
#        
#
df = pd.DataFrame({'gap':gaps_, 'lead_time':leads_,'c_rain':c_rain, 'p_ave':p_ave})
#
#fplot = final[final.alerts ==1]
#plt.plot(df_rain.ts_rain, df_rain.rain)
#plt.axvline(a3, color = 'red')
#plt.axvspan(a3 - fplot.duration.values[0], a3, facecolor = 'red', alpha = 0.5)
#plt.text(a3 - fplot.duration.values[0], y=max(df_rain.rain),s = pd.Timestamp(fplot.duration.values[0]))
#d_u = np.arange(pd.Timedelta(minutes=30), pd.Timedelta(days=7), pd.Timedelta(hours=24))
#rain_u = np.arange(1,df.n_rain.max(),40)
#
#lines = d_u / np.timedelta64(1,'h')
#
#palette = sns.color_palette("RdPu",10)
#sns.lmplot(data=df, x='d2',y='n_rain',hue='p_tot',fit_reg=False,aspect=1.5, palette=palette)
##plt.vlines(x=lines,ymin=0,ymax=2000)
#plt.hlines(y=rain_u,xmin=0,xmax=max(df.d2)+1, alpha=0.5)