# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 07:46:40 2019

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
    d = d[['ts', 'rain']]
    d['ts'] = d['ts'].dt.round('60min')
    
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
    d = d[['trigger_type', 'timestamp']]
    d.columns = ['trigger_type', 'ts_landslide']
    d['ts_landslide'] = d['ts_landslide'].dt.round('60min')
    d['id'] = 1
    return d


site = 'parta'
start = '2017-11-01'
end = '2018-01-01'

df_rain = query_rain(site, start, end)

df_rain['one_day'] = df_rain['rain'].rolling(48).sum()
df_rain['three_day'] = df_rain['rain'].rolling(144).sum()
df_rain = df_rain.sort_values('ts_rain')

df_movt = query_alert(site_code = 'par')

df_merge = pd.merge(df_rain, df_movt, on = 'id')
df_merge['duration'] = df_merge.ts_landslide - df_merge.ts_rain
df_merge = df_merge.sort_values('duration')

matrix = df_merge[['duration','three_day','trigger_type']]
matrix['duration'] = matrix['duration'].dt.round('1D')

non_neg = matrix[matrix.duration >= '1D']


d_u = non_neg.duration.unique()
rain_u = np.arange(1,600,20)


duration_u = d_u[0:50]
w = []
r = []
d = []
final = pd.DataFrame()
for i in range(len(duration_u)):
    try:
        for j in range(len(rain_u)):
            w_ = len(matrix[(matrix.duration <= duration_u[i]) &\
                             (matrix.duration >= '0D') &\
                             (matrix.three_day <= rain_u[j]) &\
                             (matrix.three_day >= 0)])
            w.append(w_)
            r.append(rain_u[j])
            d.append(duration_u[i])
            
            tem = pd.DataFrame({'w':w, 'rain':r, 'duration':d})
            
            final = pd.concat([final, tem])
    except:
        print('Fail', i, 'and', j)
        pass

final['wo'] = len(matrix) - final.w
final.reset_index(inplace=True)


'''
Bayesian proper
'''
tot_triggers = final.w.sum()
total = final.w.sum() + final.wo.sum()
p3 = tot_triggers / total

p1 = []
p2 = []
p_tot = []
new_duration = []
new_rain = []
for m in range(len(duration_u)):
    try:
        print('duration ', m)
        for n in range(len(rain_u)):
            try:
                p_1 = final.loc[(final['duration'] <= duration_u[m+1]) & \
                                 (final['duration'] >= duration_u[m]) & \
                                 (final['rain'] <= rain_u[n+1]) & \
                                 (final['rain'] >= rain_u[n])]
                triggers = p_1.w.sum()
                p_2 = len(p_1)/len(final)
                
                p1.append(triggers/tot_triggers)
                p2.append(p_2) 
                new_duration.append(duration_u[m])
                new_rain.append(rain_u[n])
            except:
                print('error rain ', n)
                pass
    except:
        print('error duration', m)
        pass

bayes = pd.DataFrame({'p1':p1, 'p2':p2, 'n_rain':new_rain, 'n_duration':new_duration})
bayes['p_tot'] = (bayes.p1 * p3) / (bayes.p2)

#pos = list((bayes.n_duration / pd.Timedelta(days=1)))
pos = list(bayes.n_rain)
width = 0.25
fig,ax = plt.subplots(figsize=(10,8))

plt.bar(pos,bayes.p_tot,width)


fig2 = plt.figure(figsize=(8, 3))
ax1 = fig2.add_subplot(111, projection='3d')


x3 = list((bayes.n_duration / pd.Timedelta(days=1)))
y3 = bayes.n_rain
z3 = np.zeros(len(bayes))

dx = np.ones(len(bayes))
dy = np.ones(len(bayes))
dz = bayes.p_tot

ax1.bar3d(x3, y3, z3, dx, dy, dz)
ax1.set_xlabel('Duration (Days)')
ax1.set_ylabel('Cumulative rainfall (mm)')
ax1.set_zlabel('P(L|C,D)')
ax1.set_title('3-Day Cumulative Rainfall', fontsize = 25)


'''
new algo needed
'''
matrix.loc[(matrix['duration'] >= '10D')&(matrix['duration']<='30D')]
plt.plot(bayes.n_duration/pd.Timedelta(days=1),bayes.p_tot)