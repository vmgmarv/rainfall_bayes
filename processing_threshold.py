# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 14:15:02 2019
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
import matplotlib.dates as md

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


a3_1 = pd.read_csv('alert_1.csv')
a3_1 = a3_1.loc[:, ~a3_1.columns.str.contains('^Unnamed')]
d1 = a3_1['n_duration'].values
a3_1 = a3_1.drop(['n_duration'],axis=1)

a3_2 = pd.read_csv('alert_2.csv')
a3_2 = a3_2.loc[:, ~a3_2.columns.str.contains('^Unnamed')]
d2 = a3_2['n_duration'].values
a3_2 = a3_2.drop(['n_duration'],axis=1)


a3_3 = pd.read_csv('alert_3.csv')
a3_3 = a3_3.loc[:, ~a3_3.columns.str.contains('^Unnamed')]
d3 = a3_3['n_duration'].values
a3_3 = a3_3.drop(['n_duration'],axis=1)


a3_4 = pd.read_csv('alert_4.csv')
a3_4 = a3_4.loc[:, ~a3_4.columns.str.contains('^Unnamed')]
d4 = a3_4['n_duration'].values
a3_4 = a3_4.drop(['n_duration'],axis=1)


######################################################################t

#k = pd.merge(a3_1, a3_2, on=['gap', 'lead_time']).set_index(['gap', 'lead_time']).sum(axis=1)

df1 = a3_1.set_index(['gap','lead_time'])
df2 = a3_2.set_index(['gap', 'lead_time'])
df3 = a3_3.set_index(['gap', 'lead_time'])
df4 = a3_4.set_index(['gap', 'lead_time'])

tot = df1 + df2 + df3 + df4
#tot['c_rain'] = [x/4 for x in tot.c_rain]
#tot['p'] = [x/4 for x in tot.p]'


df_p = pd.concat([df1[['p']],df2[['p']],df3[['p']],df4[['p']]], axis = 1)
df_p['tot_p'] = df_p.sum(axis=1, skipna=True)
df_p['median'] = df_p[['p']].median(axis=1, skipna=True)
top = df_p.sort_values(['median', 'tot_p'], ascending=[False, True])[0:4]

l = top.index.tolist()
#p_l = top.p.tolist()
##################################################################### check Duration and Cumulative
c_1,c_2,c_3,c_4 = [], [], [], []
d_1,d_2,d_3,d_4 = [], [], [], []
p_1,p_2,p_3,p_4 = [], [], [], []
gap_t, lead_t = [],[]

a3_1['duration'] = d1
a3_2['duration'] = d2
a3_3['duration'] = d3
a3_4['duration'] = d4

for i in range(len(l)):

    o_1 = a3_1[(a3_1.gap == l[i][0])& (a3_1.lead_time == l[i][1])]
    d_1.append(pd.Timedelta(o_1.duration.values[0]))
    c_1.append(o_1.c_rain.values[0])
    p_1.append(o_1.p.values[0])

    o_2 = a3_2[(a3_2.gap == l[i][0])& (a3_2.lead_time == l[i][1])]

    d_2.append(pd.Timedelta(o_2.duration.values[0]))
    c_2.append(o_2.c_rain.values[0])
    p_2.append(o_2.p.values[0])

    o_3 = a3_3[(a3_3.gap == l[i][0])& (a3_3.lead_time == l[i][1])]
    d_3.append(pd.Timedelta(o_3.duration.values[0]))
    c_3.append(o_3.c_rain.values[0])
    p_3.append(o_3.p.values[0])

    o_4 = a3_4[(a3_4.gap == l[i][0])& (a3_4.lead_time == l[i][1])]
    d_4.append(pd.Timedelta(o_4.duration.values[0]))
    c_4.append(o_4.c_rain.values[0])
    p_4.append(o_4.p.values[0])
    
    gap_t.append(l[i][0])
    lead_t.append(l[i][1])

final_result = pd.DataFrame({'rain':c_1+c_2+c_3+c_4,
                             'duration':d_1+d_2+d_3+d_4,
                             'p':p_1+p_2+p_3+p_4})



site_codes = ['par']
    
start = '2017-05-01'
end = '2017-12-30'
a3 = pd.Timestamp('2017-12-18 13:30:00')
min_rain = 1.5
days_landslide = pd.Timedelta(days=2,minutes=0)
gap = pd.Timedelta(days=2, hours = 1)
###############################################################################
final_df = pd.DataFrame()
for i in range(len(site_codes)):
    gauges = available_rg(site_codes[i])
    df_rain = pd.DataFrame()
    for n in range(len(gauges)):
        
        ###################################################################### remove spikes
        try:
            rain = rp.query_rain(gauges.gauge_name[n], start, end)
            rain['site'] = site_codes[i]
            rain.loc[(rain['rain']>=50),'rain'] = 0
            rain.loc[(rain['rain']<=0),'rain'] = 0
        except:
            rain = query_rain_noah(gauges.gauge_name[n], start, end)
            rain.loc[(rain['rain']>=50),'rain'] = 0
            rain.loc[(rain['rain']<=0),'rain'] = 0
        #####################################################################
            
        df_rain = df_rain.append(rain).drop_duplicates('ts_rain',keep='first')
    
    df_rain.sort_values(by='ts_rain',inplace=True)
    df_rain.reset_index(inplace=True)

    try:
        df_alerts = pd.Timestamp(a3) ###### check if there are alerts
    except:
        continue            
    
    ts_alerts = [df_rain.ts_rain[0], df_alerts, pd.Timestamp('2017-12-20 10:30:00')]
    
    ts_disc, rain_disc = rp.discretize(ts_alerts,df_rain,gap,min_rain)
    
    f,ts_last,sum_rain = rp.parameters(ts_disc, rain_disc)
    
    final = rp.alerts(df_rain,ts_disc, rain_disc, ts_alerts,days_landslide,min_rain, to_plot=0)
    final_df = pd.concat([final_df,final])


plt.plot(df_rain.ts_rain, df_rain.rain, color = 'b')
#plt.axvline(x=a3, color = 'red')
a3_lines = [pd.Timestamp('2017-12-15 05:30:00'), pd.Timestamp('2017-12-20 10:30:00'), 
            pd.Timestamp('2017-12-18 13:30:00'), pd.Timestamp('2017-12-23 15:30:00')]

xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
ax=plt.gca()
ax.xaxis.set_major_formatter(xfmt)
plt.xticks( rotation=25 )
plt.title('Gap:{}, Lead:{}'.format(gap, days_landslide), fontsize=20)

lines = list(final_df.loc[:,'ts'])
#lines = lines[:-1]
plt.vlines(x=lines,ymin=0,ymax=50, linestyle = '-.')
plt.vlines(x=a3_lines, ymin=0,ymax=50, color = 'red')

mm = list(final_df.loc[:,'ts'])
nn = list(final_df.loc[:,'cum_rain'])
oo = list(final_df.loc[:,'duration'])
for j in range(len(mm)):
    plt.text(x=mm[j], y=max(df_rain.rain)/2,s ='{},{}'.format(nn[j],oo[j]))
    

final_result = final_result.drop_duplicates(subset='p', keep = 'first').sort_values('p',ascending=False)
    