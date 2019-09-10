# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:45:22 2019

@author: Dynaslope
"""

import mysql.connector as sql
import pandas as pd
from pandas import Series
import matplotlib.pyplot as plt
import matplotlib.dates as md
import numpy as np
from mpl_toolkits.mplot3d import Axes3D

db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
                            user='root', password='senslope')

#db_connection = sql.connect(host='192.168.150.75', database='senslopedb', 
#                            user='pysys_local', password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg')


def query_rain(site,start,end):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.rain_%s" %(site)
    query += " WHERE ts BETWEEN '%s' AND '%s'" %(start, end)
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['ts', 'rain']]
    d.loc[:, 'ts'] = d.loc[:, 'ts'].dt.round('30min')
    
    d.columns = ['ts_rain', 'rain']
    d['id'] = 1
    return d

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


site = 'parta'
start = '2018-04-01'
end = '2018-04-15'

df_rain = query_rain(site, start, end)

df_rain.loc[:, 'one_day'] = df_rain.loc[:, 'rain'].rolling(48).sum()
df_rain.loc[:, 'three_day'] = df_rain.loc[:, 'rain'].rolling(144).sum()
df_rain = df_rain.sort_values('ts_rain')

df_movt = query_alert(site_code = 'par')

df_merge = pd.merge(df_rain, df_movt, on = 'id')
df_merge.loc[:, 'duration'] = df_merge.ts_landslide - df_merge.ts_rain
df_merge = df_merge.sort_values('duration')

matrix = df_merge.loc[:, ['ts_rain','duration','one_day','trigger_type']]
matrix.loc[:, 'duration'] = matrix.loc[:, 'duration'].dt.round('1D')

non_neg = matrix.loc[matrix.duration >= '1D', :]


################################################################# minimum day
non_neg.reset_index(inplace=True)
non_neg.loc[:, 'ts_rain'] = non_neg.loc[:, 'ts_rain'].dt.round('1D')

indices = non_neg.groupby('ts_rain')['duration'].idxmin
min_movt = non_neg.loc[indices].sort_values('ts_rain')

############################################################################### You are here
'''
Parta alerts:
    2017-08-23
    2017-12-15
    2018-01-17
    2018-04-11
'''

ts_alerts = np.array([pd.Timestamp('2017-08-23'), pd.Timestamp('2017-12-15'),
                      pd.Timestamp('2018-01-17'), pd.Timestamp('2018-04-11')])

gap = pd.Timedelta(days=5)
min_rain = 0.1
days = pd.Timedelta(days=15)
al_time = pd.Timedelta('4hours')



i = 0
duration = []
rain = []

for o in (df_rain.ts_rain):
    
    dis = df_rain[(df_rain.ts_rain >= o)&(df_rain.ts_rain < o + gap)]
    dis.reset_index(inplace=True)

    
    if dis['rain'].sum() == 0:
        continue
    
    
    for i in dis.index:
        if dis.rain[i] >= min_rain: ####pag nag start nang umulan
            dis2 = dis[(dis.ts_rain >= o)&(dis.ts_rain < o + gap)] ### magsisimula kung saan nagsimula ang ulan
            dis2.reset_index(inplace=True)
            temp_ts = []
            temp_rain = []
            for j in dis2.index: ############### cut niya yung pd duon sa may ulan lang                
                if dis2.rain[j] < min_rain: ###check niya within 3 hours kung may ulan na umabot sa min rain
                    x = np.arange(dis2.ts_rain[j], dis2.ts_rain[j] + al_time, pd.Timedelta('30minutes')) 
                    ext = dis2[(dis2.ts_rain >= x[0])&(dis2.ts_rain <= x[-1])]
                    l = ext.loc[ext['rain'] > min_rain]
                    
                    if l.empty:
                        break
                    else:
                        print(l)
                        
                temp_ts.append(dis2.ts_rain[j]) 
                temp_rain.append(dis2.rain[j])
    duration.append(temp_ts)
    rain.append(temp_rain)
    
            
final_dur = []
final_rain = []
last_ts = []
for k,l,m in zip(duration, rain, np.arange(len(rain))):
    if len(duration[m]) <= 1:
        final_dur.append(0)
        final_rain.append(0)
        last_ts.append(0)
    else:
        final_dur.append(k[-1] - k[0])
        final_rain.append(sum(l))
        last_ts.append(k[-1])
        
############################################################################################## edit this part
f_slide = []
for i in ts_alerts:
    temp_slide = []
    for j in np.arange(len(last_ts)):
        if (last_ts[j] - i <= pd.Timedelta(days=0)) & (last_ts[j] - i >= pd.Timedelta(days=-15)):
            temp_slide.append(1)
        else:
            temp_slide.append(0)
    f_slide.append(temp_slide)

f_slide = np.array(f_slide)

alerts = np.sum(f_slide, axis=0)

final = pd.DataFrame({'duration':final_dur, 'cum_rain':final_rain, 'alerts':alerts})
final['duration'] = pd.to_timedelta(final['duration'], unit='m')
#alert = [1 if x!=0 else 0 for x in final_movt]