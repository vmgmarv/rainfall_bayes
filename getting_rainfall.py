# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 09:20:00 2019
"""

import mysql.connector as sql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as md
import numpy as np
import time
start_time = time.time()

#db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
#                            user='root', password='senslope')

db_connection = sql.connect(host='192.168.150.75', database='senslopedb', 
                            user='pysys_local', password='NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg')

#db_connection = sql.connect(host='127.0.0.1', database='senslopedb', 
#                            user='root', password='alienware091394')


def query_rain(site):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.rain_%s" %(site)
#    query += " WHERE ts BETWEEN '%s' AND '%s'" %(start, end)
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['ts', 'rain']]
    d.loc[:, 'ts'] = d.loc[:, 'ts'].dt.round('30min')
    
    d.columns = ['ts_rain', 'rain']
#    d['id'] = 1
    return d

def query_alert(site_code):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.public_alert_trigger as PA"
    query += " INNER join senslopedb.public_alert_event as PV using (event_id)"
    query += " INNER join senslopedb.sites USING (site_id)"
    query += " WHERE site_code = '%s'" %(site_code)
#    query += " AND trigger_type != 'R'"
#    query += " AND trigger_type != 'D'"
#    query += " AND trigger_type != 'E'"
#    query += " AND timestamp >= '2017-02-01'"
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['trigger_type', 'timestamp']]
    d.columns = ['trigger_type', 'ts_landslide']
    d.loc[:, 'ts_landslide'] = d.loc[:, 'ts_landslide'].dt.round('30min')
    return d

def query_rain_noah(site):
    read = db_connection.cursor()
    query = "SELECT * FROM senslopedb.rain_noah_%s"%(site)
#    query += " WHERE ts BETWEEN '%s' AND '%s'" %(start, end)
    
    read.execute(query)
    d = pd.DataFrame(read.fetchall())
    d.columns = read.column_names
    d = d.loc[:, ['ts', 'rain']]
    d.loc[:, 'ts'] = d.loc[:, 'ts'].dt.round('30min')
    
    d.columns = ['ts_rain', 'rain']
#    d['id'] = 1
    
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


def data(site, site_name):
    df_rain = query_rain(site)

    df_rain = df_rain.sort_values('ts_rain')
    
    df_movt = query_alert(site_code = site_name)
    
    return df_rain, df_movt


site = 'bartb'
site_name = 'bar'

df_rain, df_alert = data(site,site_name)

gauges = available_rg(site_name)

for n in range(len(gauges)):
    try:
        rain = query_rain(gauges.gauge_name[n])
    except:
        rain = query_rain_noah(gauges.gauge_name[n])
    
    df_rain = df_rain.append(rain).drop_duplicates('ts_rain', keep = 'first')
    
    

df_rain['one_D'] = df_rain['rain'].rolling(48).sum()
df_rain['three_D'] = df_rain['rain'].rolling(144).sum()
df_rain['five_D'] = df_rain['rain'].rolling(240).sum()
df_rain['ten_D'] = df_rain['rain'].rolling(480).sum()
df_rain['fifteen_D'] = df_rain['rain'].rolling(720).sum()

df_rain.to_csv('{}_rain.csv'.format(site_name))
df_alert.to_csv('{}_alert.csv'.format(site_name))