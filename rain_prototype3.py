# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 09:10:07 2019
"""

import mysql.connector as sql
import pandas as pd
import matplotlib.pyplot as plt
#import matplotlib.dates as md
import numpy as np
import time
start_time = time.time()

db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
                            user='root', password='senslope')

#db_connection = sql.connect(host='192.168.150.75', database='senslopedb', 
#                            user='pysys_local', password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg')

#db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
#                            user='root', password='alienware091394')

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
start = '2017-08-01'
end = '2018-05-30'

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

############################################################################## rain parameters
gap = pd.Timedelta(hours=10)
min_rain = 1.5
days_landslide = pd.Timedelta(days=10)
##############################################################################
i = 0 
ts = []
rain = []

ts_final = []
rain_final = []


f = df_rain.iloc[0,:]
l = df_rain.iloc[-1,:]

i = f.ts_rain
end_date = l.ts_rain
delta = pd.Timedelta('30minute')
while i <= end_date:
    n_df = df_rain.loc[df_rain.ts_rain == i]

    print(i)    
    if n_df.empty:
        i += delta
        continue
        
    elif n_df.rain.values[0] < min_rain:
        
        temp_df = df_rain.loc[(df_rain.ts_rain > i)\
                              & (df_rain.ts_rain <= i + gap)]
        w_rain = temp_df.loc[temp_df.rain >= min_rain]
        
        if temp_df.empty:
                        
            i += delta
            
            continue
        
        elif w_rain.empty:
            m = temp_df.iloc[-1,:]
            i = m.ts_rain
            
            ts_final.append(ts)
            rain_final.append(rain)
            
            ts = []
            rain = []
            
        
        else:
            k_rain = temp_df['rain'].sum()
            k_ts = w_rain.iloc[-1,:]
            
            ts.append(k_ts.ts_rain)
            rain.append(k_rain)
            
            m = temp_df.iloc[-1,:]
            i = m.ts_rain
        
    elif n_df.rain.values[0] >= min_rain:

        ts.append(n_df.reset_index().ts_rain[0])
        rain.append(n_df.rain.values[0])
        
        i += delta
    

ts_final = [x for x in ts_final if x != []]
rain_final = [x for x in rain_final if x != []]

duration = []
sum_rain = []
ts_last = []

for j in range(len(ts_final)):
    if len(ts_final[j]) == 1:
        duration.append(pd.Timedelta('30minute'))
        sum_rain.append(rain_final[j][0])
        ts_last.append(ts_final[j][0])
        
    else:
        dur = ts_final[j][-1] - ts_final[j][0]
        r_sum = sum(rain_final[j])
        
        duration.append(dur)
        sum_rain.append(r_sum)
        ts_last.append(ts_final[j][-1])
        
############################################################################### creating alerts
f_slide = []
for m in ts_alerts:
    temp_slide = []
    for k in range(len(ts_last)):
        if (pd.to_datetime(ts_last[k]) - m <= pd.Timedelta(days=0)) \
            & (pd.to_datetime(ts_last[k]) - m >= -days_landslide):
            temp_slide.append(1)
        else:
            temp_slide.append(0)
    f_slide.append(temp_slide)

f_slide = np.array(f_slide)

alerts = np.sum(f_slide,axis=0)
final = pd.DataFrame({'ts':ts_last, 'alerts':alerts, 'cum_rain':sum_rain, 'duration':duration})
final = final.sort_values('cum_rain', ascending=False).drop_duplicates('ts').sort_index()
################################################################################ reducing alerts
temp_final = final[final.alerts == 1]

o_alert = temp_final['ts'].iloc[0]
final['alerts'] = np.where((final['alerts'] == 1) & (final['ts'] - final['ts'].shift(1) >= pd.Timedelta(days=5)), 1, 0)

###################################################################################
'''
Bayesian proper
'''

d_u = np.arange(pd.Timedelta('0D'), pd.Timedelta('7D'), pd.Timedelta('1D'))
rain_u = np.arange(1,final.cum_rain.max(),10)

tot_triggers = final.alerts.sum()
total = len(final)


p1 = []
p2 = []
p_tot = []
new_duration = []
new_rain = []
###############################################################################

###############################################################################
for m in range(len(d_u)):
    try:
        for n in range(len(rain_u)):
            try:
                p_1 = final.loc[(final['duration'] <= d_u[m+1]) &\
                                 (final['duration'] >= d_u[m]) &\
                                 (final['cum_rain'] <= rain_u[n+1]) &\
                                 (final['cum_rain'] >= rain_u[n])]
                triggers = p_1.alerts.sum()
                p_2 = len(p_1)/len(final)
                
                p1.append(triggers/tot_triggers)
                p2.append(p_2)
                new_duration.append(d_u[m+1])
                new_rain.append(rain_u[n+1])
            except:
                print('passing', m,n)
                pass
    except:
        print('error', m)
        pass

bayes = pd.DataFrame({'p1':p1, 'p2':p2, 'n_rain':new_rain, 'n_duration':new_duration})
bayes.sort_values('n_duration')
p3 = tot_triggers / (len(final)) #p(landslide)
try:
    bayes['p_tot'] = (bayes.p1 * p3) / (bayes.p2)
except:
    bayes['p_tot'] = float('NaN')


from mpl_toolkits.mplot3d import axes3d, Axes3D #<-- Note the capitalization! 
fig1 = plt.figure()
ax1 = fig1.add_subplot(111, projection='3d')


x3 = list((bayes.n_duration / pd.Timedelta(minutes=30)))
y3 = bayes.n_rain
z3 = np.zeros(len(bayes))

dx = np.ones(len(bayes))
dy = np.ones(len(bayes))
dz = bayes.p_tot


ax1.bar3d(x3, y3, z3, dx, dy, dz)
ax1.set_xlabel('Duration')
ax1.set_ylabel('Cumulative rainfall (mm)')
ax1.set_zlabel('P(L|C,D)')
ax1.set_title('Bayes theorem Intensity Duration', fontsize = 25)
ax1.set_zlim(0,1)

print('{}'.format(bayes[bayes.p_tot > 0.1]))    
    
print("--- %s seconds ---" % (time.time() - start_time))


#import pyttsx3
#
#
#engine = pyttsx3.init()
#""" RATE"""
#rate = engine.getProperty('rate')   # getting details of current speaking rate
#print (rate)                        #printing current voice rate
#engine.setProperty('rate', 125)     # setting up new voice rate
#
#engine.say("Sir Marvin, your script is already done. Comeback now")
#engine.runAndWait()