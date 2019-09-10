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

gap = pd.Timedelta(days=1)
min_rain = 0.1
days = pd.Timedelta(days=15)
al_time = pd.Timedelta('4hours')



i = 0
duration = []
rain = []


#for i,j in zip(df_rain.ts_rain,np.arange(len(df_rain))):
for o in (df_rain.ts_rain):
    
    dis = df_rain[(df_rain.ts_rain >= o)&(df_rain.ts_rain <= df_rain.iloc[-1]['ts_rain'])]
    dis.reset_index(inplace=True)
    temp_ts = []
    temp_rain = []
    for p in range(len(dis)):
        if (dis.ts_rain[p]-o <= gap):
            lst_val = dis.ts_rain[p]
#            print(dis.ts_rain[p]-o)
            if dis.rain[p] < min_rain:
                x = np.arange(dis.ts_rain[p], dis.ts_rain[p] + al_time, pd.Timedelta('30minutes'))
                ext = dis[(dis.ts_rain >= x[0])&(dis.ts_rain <= x[-1])]
                l = ext.loc[ext['rain'] > min_rain]
#                print(l)
                if l.empty:
                    temp_ts.append([])
                    temp_rain.append([])
                    
                else:
                    t_ts = l.iloc[0]['ts_rain']
                    t_rain = l.iloc[0]['rain']
                    temp_ts.append(t_ts)
                    temp_rain.append(t_rain)
            else:
#                print('not okay')
                temp_ts.append(dis.ts_rain[p])
                temp_rain.append(dis.rain[p])

        else: 
            continue
    duration.append(temp_ts)
    rain.append(temp_rain)
#    print(rain)
    
dur = []
r_sum = []
#
for j in range(len(duration)):
    dur.append([x for x in duration[j] if x])
#    dur.append(ll)
#
#dur = [x for x in dur if x !=[]]
#
for k in range(len(rain)):
    r_sum.append([u for u in rain[k] if u])
#    r_sum.append(mm)

dur = [x for x in dur if x != []]
r_sum = [x for x in r_sum if x != []]

temp_df = pd.DataFrame({'dur':dur, 'r_sum':r_sum})

#plt.plot(ll)
#plt.plot(mm, color = 'red')
#plt.plot(df_rain.rain, color ='green')
        
        
#        if (o - df_rain.ts_rain[p] < gap):
#
#            if (df_rain.rain[p] < min_rain):
#                x = np.arange(df_rain.ts_rain[p], df_rain.ts_rain[p] + al_time, pd.Timedelta('30minutes'))
#                ext = df_rain[(df_rain.ts_rain >= x[0]) & (df_rain.ts_rain <= x[-1])]
#                l = ext.loc[ext['rain'] > min_rain]
#                if l.empty:
#                    temp_ts.append([])
#                    temp_rain.append([])
#                else:
#                    t_ts = l.iloc[0]['ts_rain']
#                    t_rain = l.iloc[0]['rain']
#                    temp_ts.append(t_ts)
#                    temp_rain.append(t_rain)
#                    
#            
#            temp_ts.append(df_rain.ts_rain[p])
#            temp_rain.append(df_rain.rain[p])
#        
#    
#    duration.append(temp_ts)
#    rain.append(temp_rain)
#        
    

#for i,j in zip(df_rain.ts_rain,np.arange(len(df_rain))):
#    print (i,j)
#
#    #    temp_rain = []
#    temp_ts = []
#    print(i)
#    if j == len(df_rain):
#        break
#    while (i - df_rain.ts_rain[j] < gap):
#        tem_t = df_rain.ts_rain[j]
#        r = j
#        k = 0
#        tm_ts = []
#        tm_rain = []
##        while (df_rain.ts_rain[j+k] - tem_t < al_time) & (df_rain.rain[r] < min_rain):
##            tm_ts.append(df_rain.ts_rain[j+k])
##            tm_rain.append(df_rain.rain[r])
###            print(r)
##            k+=1
##            r+=1            
##        
#        if tm_ts != []:
#            temp_ts.append(tm_ts)
#            temp_rain.append(tm_rain)
#        else:
#            mm = df_rain.ts_rain[j]
#            temp_ts.append(mm)
#            temp_rain.append(df_rain.rain[j])
#        
#        j+=1
#        if j == len(df_rain):
#            break
#        
#    dur.append(temp_ts)
#    r_sum.append(temp_rain)
#
#dur = [x for x in dur if x != []]
#r_sum = [x for x in r_sum if x != []]


final_dur = []
final_rain = []
last_ts = []
for k,l,m in zip(dur, r_sum, np.arange(len(dur))):
    if len(dur[m]) == 1:
        final_dur.append(0)
        final_rain.append(l[0])
        last_ts.append(k[0])
    else:
        final_dur.append(k[-1] - k[0])
        final_rain.append(sum(l))
        last_ts.append(k[-1])
        

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
#################################################################################
#'''
#Bayesian proper
#'''
#
#d_u = np.arange(pd.Timedelta('0D'), pd.Timedelta('2D'), pd.Timedelta('30min'))
#rain_u = np.arange(1,final.cum_rain.max(),10)
#
#tot_triggers = final.alerts.sum()
#total = len(final)
#p3 = tot_triggers / (total) #p(landslide)
#
#p1 = []
#p2 = []
#p_tot = []
#new_duration = []
#new_rain = []
#
#for m in range(len(d_u)):
#    try:
#        for n in range(len(rain_u)):
#            try:
#                p_1 = final.loc[(final['duration'] <= d_u[m+1]) &\
#                                 (final['duration'] >= d_u[m]) &\
#                                 (final['cum_rain'] <= rain_u[n+1]) &\
#                                 (final['cum_rain'] >= rain_u[n])]
#                triggers = p_1.alerts.sum()
#                p_2 = len(p_1)/len(final)
#                
#                p1.append(triggers/tot_triggers)
#                p2.append(p_2)
#                new_duration.append(d_u[m+1])
#                new_rain.append(rain_u[n+1])
#            except:
#                print('passing', m,n)
#                pass
#    except:
#        print('error', m)
#        pass
#
#bayes = pd.DataFrame({'p1':p1, 'p2':p2, 'n_rain':new_rain, 'n_duration':new_duration})
#bayes.sort_values('n_duration')
#
#try:
#    bayes['p_tot'] = (bayes.p1 * p3) / (bayes.p2)
#except:
#    bayes['p_tot'] = float('NaN')
#
#
#
#fig1 = plt.figure()
#ax1 = fig1.add_subplot(111, projection='3d')
#
#
#x3 = list((bayes.n_duration / pd.Timedelta(minutes=30)))
#y3 = bayes.n_rain
#z3 = np.zeros(len(bayes))
#
#dx = np.ones(len(bayes))
#dy = np.ones(len(bayes))
#dz = bayes.p_tot
#
#ax1.bar3d(x3, y3, z3, dx, dy, dz)
#ax1.set_xlabel('Duration (30min)')
#ax1.set_ylabel('Cumulative rainfall (mm)')
#ax1.set_zlabel('P(L|C,D)')
#ax1.set_title('Bayes theorem Intensity Duration', fontsize = 25)
#ax1.set_zlim(0,1)
