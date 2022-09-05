'''
The script computes case based loag loads as defined using the MLS-ICE framework for all events in log of interest.
The script takes as input a dictionary containing the optimal durations for locations (path set in line 15)
and an event log (imported in line 132).
'''

import os
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
import time
import joblib

def get_all_configs(conf_dir = 'configs/lag_configs/'):
        
    configurations = {}

    for conf in os.listdir(conf_dir):

        if 'configurations' in conf:
            configurations.update(joblib.load(conf_dir + conf))

    return configurations

def fill_na_configs(configs, na_val = 60*24):
    
    for locations in list(configs.keys()):
        
        for key, val in zip(configs['{}'.format(locations)].keys(), configs['{}'.format(locations)].values()):
            
            if pd.isna(val[0]):
                #print(locations, key, val)
                configs['{}'.format(locations)][key] = (configs['{}'.format(locations)][key][0], 
                                                        configs['{}'.format(locations)][key][1], 
                                                        (na_val))
    
    return configs

def get_lead_ts(log):
    
    temp_log = log.copy()
    temp_log = temp_log.sort_values(['case_id', 'event_id'])
    temp_log['ts_next'] = temp_log.ts.shift(-1)
    temp_log.loc[temp_log['activity'] == '<EOS>', 'ts_next'] = np.nan
    
    return log.merge(temp_log[['event_id', 'ts_next']], left_on='event_id', right_on='event_id')

def calc_load_optdur(x, previous, location, config):
    
    target_activity = x['activity']
    
    offset = pd.DateOffset(minutes=0)
    diff = pd.DateOffset(minutes=config[target_activity][location][2])
    
    return pd.Series([x.event_id, previous.loc[(previous.index >= x['ts']-diff-offset) & (previous.index < x['ts']-offset)].ts.count()])
    
def calc_load_activecase(x, previous):
    
    return pd.Series([x.event_id, previous.loc[(previous.ts <= x.ts) & (previous.ts_next >= x.ts)].ts.count()])

    
def calc_load_bos(x, previous, bosdur):
    
    if bosdur != None:
        offset = pd.DateOffset(minutes=0)
        diff = pd.DateOffset(hours=bosdur)

        return pd.Series([x.event_id, previous.loc[(previous.index >= x['ts']-diff-offset) & 
                                                   (previous.index < x['ts']-offset)].ts.count()])
    
    else:
        return pd.Series([x.event_id, 0])
    
def compute_load_bos(log, load_df, bosdur):
    
    previous = log_csv.loc[log_csv.activity.str.startswith('<BOS>')]
    load_comp = log[~log.event_id.isin(np.array(load_df.event_id))].apply(lambda x: calc_load_bos(x, previous, bosdur), axis=1)
    load_comp.columns = ['event_id', 'load']
    
    return load_comp

def compute_load(log, location, load_state):
    
    load_comp = None
    
    previous = log.loc[log.activity == location]
    target_log = log.loc[log.event_id.isin(np.array(previous.event_id+1))]
    
    if len(target_log) == 0:
        return print('target log empty')
    
    if load_state == 'actcase':
        load_comp = target_log.apply(lambda x: calc_load_activecase(x,previous), axis=1)
    
    elif load_state == 'optdur':
        config = get_all_configs()
        config = fill_na_configs(config)
        load_comp = target_log.apply(lambda x: calc_load_optdur(x, previous, location, config), axis=1)
        
    load_comp.columns = ['event_id', 'load']
    
    return load_comp

def compute_lag_load(log, load_state='optdur', bosdur=None):
    """
    Function computes a lag load state as defined by the MLS-ICE framework for all events in log.
    Load_state determines which approach for computing the load at a single location is used, i.e. either active number 
    of cases (actcase) or number of events in optimal duration (optdur). Additionally if "bosdur" parameter is specified the load at <BOS> 
    is computed by counting the number of cases in the past bosdur hours if specified as None then it simply returns 0 for 
    all <BOS> events since a <BOS> events has no previous event.
    """
    
    load_df = pd.DataFrame(columns=['event_id','load'])
    
    if load_state == 'actcase':
        log = get_lead_ts(log)
    
    
    locations = [col for col in list(set(log_csv.activity)) if ('EOS' not in col)]

    for location in tqdm(locations):

        load_loc = compute_load(log, location, load_state=load_state)
        load_df = load_df.append(load_loc)

    load_bos = compute_load_bos(log, load_df, bosdur)
    load_df = load_df.append(load_bos)

    return log_csv.merge(load_df, left_on='event_id', right_on='event_id')

log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.ts = log_csv.ts.apply(lambda x: x[:-4])
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('ts')
log_csv.set_index(log_csv.ts, inplace=True)

load_log = compute_lag_load(log_csv, load_state='actcase')
load_log = load_log.rename(columns = {'load':'lag_load'})

