'''
The script identifies relevant locations of a business process using the system based MLS-ICE.
The script takes as input an event log (imported in line 74).
'''

import os
import time
import joblib
import pandas as pd
import numpy as np
import datetime as dt
from pm4py.objects.log.util import dataframe_utils

def get_lead_ts(log):
    
    temp_log = log.copy()
    temp_log = temp_log.sort_values(['case_id', 'event_id'])
    temp_log['ts_next'] = temp_log.ts.shift(-1)
    temp_log.loc[temp_log['activity'] == '<EOS>', 'ts_next'] = np.nan
    
    return log.merge(temp_log[['event_id', 'ts_next']], left_on='event_id', right_on='event_id')
    

def get_proc_time(merge):
    
    return merge.apply(lambda x: (x.ts_next - x.ts).seconds, axis=1)

def get_throughp(log, act, time_unit = 'second'):

    proc_time = get_proc_time(log[log.activity == act])
    
    if time_unit == 'second':
        print(f'{act} - average proctime {round(proc_time.mean(),2)} and median proctime {round(proc_time.median(),2)} seconds')
    
    elif time_unit == 'minute':
        proc_time = proc_time/60
        print(f'{act} - average proctime {round(proc_time.mean(),2)} and median proctime {round(proc_time.median(),2)} minutes')
    
    elif time_unit == 'hour':
        proc_time = proc_time/3600
        print(f'{act} - average proctime {round(proc_time.mean(),2)} and median proctime {round(proc_time.median(),2)} hours')
        
    else:
        print('time unit not available, choose second, minute, hour')
        
    return proc_time

def get_locations(log, thresh_time = 5, thresh_freq = 0.01, time_unit = 'minute'):
    
    log = get_lead_ts(log)
    load_locations = []
    total = len(log)
    
    for load, freq in zip(log.activity.value_counts().index, log.activity.value_counts()):
        
        if (load == '<EOS>') | (load == '<BOS>'):
            continue
    
        dur = get_throughp(log, load, time_unit)
        
        if np.mean(dur) < thresh_time:
            print('auto')
        
        else:
            if freq/total < thresh_freq:
                print('unfreq')
                
            else:
                print(load, f'{round(freq/total, 2)} added')
                load_locations.append(load)
                
    return load_locations

log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.ts = log_csv.ts.apply(lambda x: x[:-4]) 
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)

load_locations = get_locations(log_csv)

joblib.dump(load_locations, "load_locations.pickle")




