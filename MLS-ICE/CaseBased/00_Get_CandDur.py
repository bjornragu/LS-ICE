'''
The script creates a range of candidate duration values for all locations in a business process.
The script takes as input an event log (imported in line 83).
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
    
    return log_csv.merge(temp_log[['event_id', 'ts_next']], left_on='event_id', right_on='event_id')

def get_proc_time(merge):
    
    return merge.apply(lambda x: (x.ts_next - x.ts).seconds, axis=1)

def get_throughp_act(log, act, time_unit = 'minute'):

    proc_time = get_proc_time(log)
    
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

def myround(x, base=5):
    return base * round(x/base)

def get_dur_range(log, act='W_Completeren aanvraag', time_unit='minutes', base=15, step=30, quantile=0.95):
    
    
    proc_times = get_throughp_act(log[log.activity == act], act, time_unit='minute')
    print(len(proc_times))
    round_median = myround(np.median(proc_times), base)
    if round_median == 0:
        round_median = base
        
    round_quantile = myround(np.quantile(proc_times, q = 0.95), base)
    if round_quantile == 0:
        round_quantile = base
    
    duration_range = range(round_median, round_quantile+step, step)
    print('duration search range:', duration_range, 'length:', len(duration_range))
    
    return duration_range

def get_duration_range_dic(log):
    
    dur_range_dic = {}
    for load in [col for col in log_csv.activity.value_counts().index if ('<EOS>' not in col) &  ('<BOS>' not in col)]:
        dur_range = get_dur_range(log, load, 'minutes', 15, 30, 0.95) 
    
        if (dur_range != range(15, 45, 30)):
            dur_range_dic[load] = dur_range

        else:
            dur_range_dic[load] = range(15, 300, 30)
            
    return dur_range_dic
        
log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.ts = log_csv.ts.apply(lambda x: x[:-4]) #need to add this for some reason - otherwise search doesnt work
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)

log_csv = get_lead_ts(log_csv)
duration_dic = get_duration_range_dic(log_csv)
duration_dic['<EOS>'] = range(15, 300, 30)
duration_dic['<BOS>'] = range(15, 300, 30)

joblib.dump(duration_dic, "dur_range_dic.pickle")