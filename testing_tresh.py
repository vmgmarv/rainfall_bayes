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
    #query += " AND trigger_type != 'R'"
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

site_codes = ['bar','bol','hin','ime','lay','lte',
              'lip','lpa','lun','par','tal','jor']

one_day_t = [63.1114]

two_day_t = [81]

three_day_t = [126.2228]

what = 2
lead_time = pd.Timedelta(days=2, hours=0)

start = '2017-05-01'
end = '2019-12-30'

df_alerts = query_alert(site_codes[0])
rain_alerts = df_alerts[df_alerts['trigger_type'] == 'R'].ts_landslide

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
    mov_alerts = df_alerts[df_alerts['trigger_type'] != 'R'].ts_landslide.values
    m_alerts = [pd.Timestamp(x) for x in mov_alerts]
    
    #############################################################################
    
    semifinal = pd.DataFrame()
    
    
    if what == 1:
        day = 'one_D'
        thresh = one_day_t
        
    if what == 2:
        day = 'two_D'
        thresh = two_day_t
        
    if what == 3:
        day = 'three_D'
        thresh = three_day_t
    
    thresh = np.repeat(thresh, len(m_alerts))

    for i,j in zip(m_alerts,thresh):
        print(i,j)

        df_rain = df_rain[df_rain['ts_rain'] <= i]
        
        df_rain['one_D'] = df_rain['rain'].rolling(48).sum()
        df_rain['two_D'] = df_rain['rain'].rolling(97).sum()
        df_rain['three_D'] = df_rain['rain'].rolling(144).sum()
        
        df_rain['alert_current'] = np.where((df_rain[day] >= j),1,0)
        df_rain = df_rain[df_rain['alert_current'] == 1]
        
        df_rain['predicted'] = np.where((i - df_rain['ts_rain'] <= lead_time),1,0)
        semifinal = pd.concat([semifinal, df_rain])
    
    final_df = pd.concat([final_df,semifinal])
    

final_df2 = final_df[final_df['predicted'] == 1]

print('Day',what,'thresh({})'.format(thresh[0]),'=',len(final_df2)/ len(final_df))

#df_rain['one_D'] = df_rain['rain'].rolling(48).sum()
#df_rain['three_D'] = df_rain['rain'].rolling(144).sum()
#df_rain['new_D'] = df_rain['rain'].rolling(97).sum()
#
#df_rain['alert_current'] = np.where((df_rain['one_D'] >= 65.5725),1,0)
#df_rain['alert_new'] = np.where((df_rain['new_D'] >= 81),1,0)
#
#r_alerts = df_rain.loc[df_rain['alert_current'] == 1].ts_rain.values
#r_alerts = [pd.Timestamp(x) for x in r_alerts]
#n_alerts = df_rain.loc[df_rain['alert_new'] == 1].ts_rain.values
#n_alerts = [pd.Timestamp(x) for x in n_alerts]

#
#plt.plot(df_rain.ts_rain, df_rain.one_D, color = 'blue', label = '1Day', alpha = 0.8)
#plt.plot(df_rain.ts_rain, df_rain.new_D, color = 'red', label = '2Days,30min', alpha = 0.8)
#plt.plot(df_rain.ts_rain, df_rain.three_D, color = 'orange', label = '3Day', alpha = 0.8)
#plt.axhline(y=one_day_t, color = 'blue', label = '1-Day Current threshold', linestyle = '-.')
#plt.axhline(y=three_day_t, color = 'orange', label = '3-Day Current threshold', linestyle = '-.')
#plt.axhline(y=81, color = 'red', label = 'New Threshold', linestyle = '-.')
##plt.vlines(x=r_alerts,ymin=0,ymax=50, linestyle='-.', color = 'green', label ='Current rain events')
##plt.vlines(x=n_alerts,ymin=0,ymax=50, linestyle='-.', color = 'orange', label = 'New rain events')
#plt.vlines(x=m_alerts,ymin=0,ymax=max(df_rain.three_D.dropna()), color = 'black', label = 'movements')
#plt.legend(loc='upper right')
#plt.ylabel('Rainfall [mm]', fontsize = 15)
#plt.title('Site: {}'.format(site_codes[i]), fontsize=25)

