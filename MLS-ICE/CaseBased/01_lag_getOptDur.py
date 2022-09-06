'''
The script identifies the optimal duration when computing a load state at the previous (lag) location of cases using 
the case based MLS-ICE.
The script takes as input a grid of candidate duration values for each location (imported in line 61) 
and an event log (imported in line 110)
'''

import pandas as pd
import numpy as np
import datetime as dt
from pm4py.objects.log.util import dataframe_utils
import os
from tqdm.auto import tqdm
import time
import joblib
from sklearn.ensemble import RandomForestRegressor

def get_rf_relation(target_log, load, depth = 3,  threshold = 50):
    remtimes, loads = target_log.align(load, join='inner', copy=False)
    
    if len(loads) > threshold:
        lr = RandomForestRegressor(max_depth=depth).fit(np.array(loads).reshape(-1,1), remtimes)
        r2 = lr.score(np.array(loads).reshape(-1,1), remtimes)
    
    else:
        r2 = np.nan

    return r2

def get_lag_locations(log, location):
    
    ev_ids = np.array(log[log.activity == location].event_id) -1 
    lags = log[log.event_id.isin(ev_ids)]['activity'].value_counts()

    return lags

def get_target_time(log, load_location, location):
    
    ids = np.array(log[log.activity == location].event_id)
    lag_ids= ids-1 
    
    cols = ['event_id', 'case_id', 'activity', 'ts_col', 'remtime']
    
    event_log = log[log.event_id.isin(ids)][cols].sort_values(['case_id', 'event_id'])
    prev_log = log[log.event_id.isin(lag_ids)][cols].sort_values(['case_id', 'event_id'])                    
    prev_log.columns = ['{}_prev'.format(col) for col in prev_log.columns]
    
    event_log = event_log.reset_index()
    prev_log = prev_log.reset_index(drop=True)  
    
    log_merge = pd.concat([event_log, prev_log], axis=1)
    log_merge = log_merge[(log_merge.activity == location) & (log_merge.activity_prev == load_location)]
    
    log_merge.set_index(['ts'], inplace=True)
    
    return log_merge.remtime
    
def get_config_location(log, location, threshold=50):
    
    config = {}
    dur_range_dic = joblib.load('dur_range_dic.pickle')
    
    if location == '<BOS>':
        config['NoPrevLocation'] = (np.nan, 0, np.nan)
        return config
    
    lags = get_lag_locations(log, location)
    
    for load_location in lags.index:
        
        if location == '<EOS>':
            config[load_location] = (np.nan, 0, np.nan)
            continue 
        
        duration_range = dur_range_dic[load_location]
        target = get_target_time(log, load_location, location)
        
        if len(target) <= threshold:
            config[load_location] = (np.nan, 0, duration_range[0])
        
        else:
            offset = 0
            best = None
            for diff in duration_range:

                load = log.loc[log.activity == load_location, ]\                   
                    .groupby(['ts']).count().asfreq('1S')\
                    .shift(1, freq=pd.DateOffset(hours=offset))\
                    .rolling(f"{diff}min").count()['ts_col']

                relation = get_rf_relation(target, load, depth = 3)
                if best is None or relation > best[0]:
                    best = (relation, offset, diff)
                    
            config[load_location] = best 
        
    return config
    
def get_config_dic(log):
    
    configs = {}
    locations =  log.activity.value_counts().index
    
    for location in tqdm(locations):
        
        configs[location] = get_config_location(log, location)
        
    return configs

log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.ts = log_csv.ts.apply(lambda x: x[:-4])
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('ts')
log_csv.set_index(log_csv.ts, inplace=True)
log_csv = log_csv.rename(columns={'ts':'ts_col'})

configurations = get_config_dic(log_csv)



