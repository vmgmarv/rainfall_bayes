# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:05:13 2019

@author: Dynaslope
"""

import mysql.connector as sql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as md
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


def data(site, start, end, site_name):
    df_rain = query_rain(site, start, end)

    df_rain = df_rain.sort_values('ts_rain')
    
    df_movt = query_alert(site_code = site_name)
    
    return df_rain, df_movt

def discretize(ts_alerts, df_rain,gap,min_rain):

    i = 0 
    
    ts_final2 = []
    rain_final2 = []
    while i in range(len(ts_alerts)):
        if i+1 >= len(ts_alerts):
            break
    
        df = df_rain.loc[(df_rain.ts_rain >= ts_alerts[i])&(df_rain.ts_rain <= ts_alerts[i+1])]
        
        if df.empty:
            i+=1
            continue
        
    
        f = df.iloc[0,:]
        l = df.iloc[-1,:]
        
        end_date  = l.ts_rain
        delta = pd.Timedelta(minutes=30)
        j = f.ts_rain
        
        ts = []
        rain = []
    
        ts_final = []
        rain_final = []
        
    
        while j <= end_date:
            n_df = df.loc[df.ts_rain == j]
            
            if n_df.empty:
                j+=delta
                continue
            
            elif n_df.rain.values[0] < min_rain:
                temp_df = df.loc[(df.ts_rain >= j)\
                                 &(df.ts_rain <= j + gap)]
                w_rain = temp_df.loc[temp_df.rain >= min_rain]
                
                if temp_df.empty: ###############################if j has no data
                    j+=delta
                    continue
                
                elif w_rain.empty: ################################## if no rain whithin gap
                    m = temp_df.iloc[-1,:]
                    if len(temp_df) == 1:
                        j = m.ts_rain + delta
                    else:
                        j = m.ts_rain
                    
                    ts_final.append(ts)
                    rain_final.append(rain)
                    
                    ts = []
                    rain = []
                
                else:
                    try:
                        k_rain = temp_df['rain'].sum()
                    except:
                        k_rain = np.array(temp_df['rain']).astype(float).tolist()
                        k_rain = sum(k_rain)
                        
                    k_ts = w_rain.iloc[-1,:]
                    
                    ts.append(k_ts.ts_rain)
                    rain.append(k_rain)
                    
                    m = temp_df.iloc[-1,:]
                    j = m.ts_rain
                    
            elif n_df.rain.values[0] >= min_rain:
                
                ts.append(n_df.reset_index().ts_rain[0])
                rain.append(n_df.rain.values[0])
                
                j+=delta
                
        
        ts_final = [x for x in ts_final if x != []]
        rain_final = [x for x in rain_final if x != []]
        
        ts_final2.append(ts_final)
        rain_final2.append(rain_final)
                
        i+=1
        
    return ts_final2, rain_final2

def parameters(ts_final2, rain_final2):

    duration = []
    sum_rain = []
    ts_last = []
    
    for k in range(len(ts_final2)):
        ts_a = ts_final2[k]
        r_a = rain_final2[k]
        t = ts_final2[k]
        dur = []
        s_rain = []
        lst = []
        for l in range(len(ts_a)):
            if len(ts_a[l]) == 1:
                dur.append(pd.Timedelta(minutes=30))
                s_rain.append(r_a[l][0])
                lst.append(t[l][0])
                
            else:
                t_d = ts_a[l][-1] - ts_a[l][0]
                try:
                    r = sum(r_a[l])
                except:
                    r = np.array(r_a[l]).astype(float).tolist()
                    r = sum(r)
                
                dur.append(t_d)
                s_rain.append(r)
                lst.append(t[l][-1])
        duration.extend(dur)
        sum_rain.extend(s_rain)
        ts_last.extend(lst)
    final = pd.DataFrame({'ts':ts_last, 'cum_rain':sum_rain, 'duration':duration})
    
    return final, ts_last, sum_rain

def alerts(df_rain, ts_final2, rain_final2, ts_alerts, days_landslide,min_rain,to_plot):
    
    final,ts_last, sum_rain = parameters(ts_final2, rain_final2)

    ts_al = []
    m=0
    while m in range(len(ts_alerts)):
        
        if m+1 >= len(ts_alerts):
            break
    
        df_a = final.loc[(final.ts >= ts_alerts[m])\
                          &(final.ts <= ts_alerts[m+1])\
                          &(final.ts >= ts_alerts[m+1] - days_landslide)]
        if df_a.empty:
            m+=1
            continue
    
        a = df_a.iloc[0,:]
        ts_al.append(a.ts)
        
        m+=1
    
    
    final['alerts'] = 0
    final.loc[final['ts'].isin(ts_al), 'alerts'] = 1
    
    
#    print(final[final.alerts == 1])

    
    if to_plot == 1:
        ####################################################################### minimum rain = 0
        df_rain.loc[(df_rain['rain']<min_rain),'rain'] = 0
        #######################################################################
        fig1, ax = plt.subplots()
        plt.plot(df_rain.ts_rain, df_rain.rain, marker = 'o')
        
        mm = list(final.ts)
        nn = list(final.cum_rain)
        oo = list(final.duration)
        for i in range(len(ts_alerts)):
            try:            
                plt.axvline(ts_alerts[i+1], color = 'r')
            except:
                pass
        
        for j in range(len(mm)):
            plt.axvline(mm[j], linestyle = 'dashed',color = 'g')
            plt.text(x=mm[j], y=max(df_rain.rain)/2,s ='{},{}'.format(nn[j],oo[j]))
        
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax=plt.gca()
        ax.xaxis.set_major_formatter(xfmt)
        plt.xticks( rotation=25 )
#        ax.legend(fontsize = 8, loc = 'upper right')
        plt.title('Instataneous rain', fontsize = 20)
        
        
        fig2, ax = plt.subplots()
        plt.plot(ts_last, sum_rain, marker = 'o')
        
        for k in range(len(ts_alerts)):
            try:
                plt.axvline(ts_alerts[k+1], color = 'r')
            except:
                pass
    
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax=plt.gca()
        ax.xaxis.set_major_formatter(xfmt)
        plt.xticks( rotation=25 )
#        ax.legend(fontsize = 8, loc = 'upper right')
        plt.title('Discretized rain', fontsize = 20)

    return final

def bayesian(final, to_plot):
    
    d_u = np.arange(pd.Timedelta(minutes=30), pd.Timedelta(days=7), pd.Timedelta(days=1))
    rain_u = np.arange(1,final.cum_rain.max(),10)
    
    tot_triggers = final.alerts.sum()
    
    
    p1 = []
    p2 = []
    new_duration = []
    new_rain = []

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
                    
                    pass
        except:
    
            pass
    
    bayes = pd.DataFrame({'p1':p1, 'p2':p2, 'n_rain':new_rain, 'n_duration':new_duration})
    bayes.sort_values('n_duration')
    p3 = tot_triggers / len(final) #p(landslide)
    try:
        bayes['p_tot'] = (bayes.p1 * p3) / (bayes.p2)
    except:
        bayes['p_tot'] = float('NaN')
    
    if to_plot == 1:
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

    
    return bayes

if __name__ == "__main__":
    

    site = 'parta'
    start = '2017-01-01'
    end = '2018-12-30'
    site_name = 'par'
    df_rain, df_movt = data(site,start,end,site_name)
    
    '''
    Parta alerts:
        2017-08-23
        2017-12-15
        2018-01-17
        2018-04-11
    '''
    
    ts_alerts = np.array([df_rain.ts_rain[0],pd.Timestamp('2017-08-23'), pd.Timestamp('2017-12-15'),
                          pd.Timestamp('2018-01-17'), pd.Timestamp('2018-04-11')])
        
    ############################################################################## rain parameters
    gap = pd.Timedelta(hours=50)
    min_rain = 1.5
    days_landslide = pd.Timedelta(days=15)
    ##############################################################################
    

    ts_final2, rain_final2 = discretize(ts_alerts,df_rain,gap,min_rain)
    final = alerts(df_rain, ts_final2, rain_final2, ts_alerts, days_landslide,min_rain,to_plot=1)
    bayes = bayesian(final, to_plot = 1)
    
    
    print('{}'.format(bayes[bayes.p_tot > 0.1]))    
        
    print("--- %s seconds ---" % (time.time() - start_time))
