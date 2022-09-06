'''
The script identifies the optimal duration when computing a load state at the current location of cases using 
the case based MLS-ICE.
The script takes as input a grid of candidate duration values for each location (imported in line 32) 
and an event log (imported in line 71).
'''

import pandas as pd
import numpy as np
import datetime as dt
from pm4py.objects.log.util import dataframe_utils
import os
import joblib
from tqdm.auto import tqdm
import time
from sklearn.ensemble import RandomForestRegressor


def get_rf_relation(target_log, load, depth = 3,  threshold = 50):
    remtimes, loads = target_log.align(load, join='inner', copy=False)
    
    if len(loads) > threshold:
        lr = RandomForestRegressor(max_depth=depth).fit(np.array(loads).reshape(-1,1), remtimes)
        r2 = lr.score(np.array(loads).reshape(-1,1), remtimes)
    
    else:
        r2 = np.nan

    return r2

def get_location_config(log, location, threshold=50):
    
    dur_range_dic = joblib.load('dur_range_dic.pickle')
    duration_range = dur_range_dic[location]
    
    if location == '<EOS>':
        return (np.nan, 0, np.nan)
    
    target = log.loc[log.activity == location, ].remtime
    
    if len(target) <= threshold:
        best = (np.nan, 0, duration_range[0])
        
    else:
        best = None
        offset = 0

        for diff in duration_range:

            load = log_csv.loc[log.activity == location, ]\
                .groupby(['ts']).count().asfreq('1S')\
                .shift(1, freq=pd.DateOffset(hours=offset))\
                .rolling(f"{diff}min").count()['ts_col']

            relation = get_rf_relation(target, load, depth = 3)
            if best is None or relation > best[0]:
                best = (relation, offset, diff)
        
    return best

def get_config_dir(log):
    
    configurations = {}
    locations = log_csv.activity.value_counts().index
    
    for location in tqdm(locations):
        
        configurations[location] = get_location_config(log, location)
        
    return configurations

log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.ts = log_csv.ts.apply(lambda x: x[:-4])
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('ts')
log_csv.set_index(log_csv.ts, inplace=True)
log_csv = log_csv.rename(columns={'ts':'ts_col'})

configurations = get_config_dir(log_csv)