'''
The script computes system based MLS-ICE features for all events in log of interest.
The script takes as input list containing the important locations (imported in line 76), 
the optimal duration for location (path set in line 78) and an event log (imported in line 98).
'''

import os
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
import time
import joblib

config_path = 'configs/'

def get_lead_ts(log):
    
    temp_log = log.copy()
    temp_log = temp_log.sort_values(['case_id', 'event_id'])
    temp_log['ts_next'] = temp_log.ts.shift(-1)
    temp_log.loc[temp_log['activity'] == '<EOS>', 'ts_next'] = np.nan
    
    return log.merge(temp_log[['event_id', 'ts_next']], left_on='event_id', right_on='event_id')
  
def get_all_configs(conf_dir = config_path):
        
    configurations = {}

    for conf in os.listdir(conf_dir):

        if 'configuration' in conf:
            configurations.update(joblib.load(conf_dir + conf))

    print('config keys:', list(configurations.keys()))
    
    return configurations

def fill_na_config(configs, na_val = 60*24):
    
    for load_activity in list(configs.keys()):
        for key, val in zip(configs['{}'.format(load_activity)].keys(), configs['{}'.format(load_activity)].values()):

            if pd.isna(val[0]):
                configs['{}'.format(load_activity)][key] = (configs['{}'.format(load_activity)][key][0],
                                                            configs['{}'.format(load_activity)][key][1], (na_val))
                
    return configs

def load_state_optdur(x, previous, load, configurations):
    offset = pd.DateOffset(minutes=0)
    diff = pd.DateOffset(minutes=configurations[load][x['activity']][2])
    return previous.loc[(previous >= x['ts']-diff-offset) & (previous < x['ts']-offset)].count()

def load_state_activecases(x, previous):
    
    return previous.loc[(previous.ts <= x.ts) & (previous.ts_next >= x.ts)].ts.count()    
   
def get_load_log(log, load_log=None, load_state = 'optdur'):
    """
    Function computes MLS-ICE features for all events in load log.
    If load_log=None functions compute MLS-ICE features for full log (log).
    Load_state determines which approach for computing the load at a single location is used, i.e. either active number of cases (actcase)
    or number of events in optimal duration (optdur). 
    """
    
    if load_state == 'actcase':
        log = get_lead_ts(log)
    
    elif load_state == 'optdur':
        configurations = get_all_configs()
        configurations = fill_na_config(configurations)
        
    else:
        print(f'load state: {load_state}, not supported')
        return None
    
    locations = joblib.load('load_locations.pickle')
    
    if load_log is None:
        load_log = log.copy()
        print('computing load for full log')
        
    for location in tqdm(locations):
        
        if load_state =='actcase':
            previous = log.loc[log.activity == location][['ts', 'ts_next']]
            load_comp = pd.DataFrame(load_log.apply(lambda x: load_state_activecases(x, previous), axis=1))
        
        elif load_state == 'optdur':
            previous = log.loc[log.activity == location, 'ts']
            load_comp = pd.DataFrame(load_log.apply(lambda x: load_state_optdur(x, previous, location, configurations), axis=1)) 

        load_log['load_{}'.format(location)] = load_comp
        
    return load_log
    
log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('ts')
log_csv.set_index(log_csv.ts, inplace=True)



load_log = log_csv[log_csv.case_id.isin(list(np.random.choice(log_csv.case_id.unique(), size=1, replace=False )))]

load_log = get_load_log(log=log_csv, load_log=load_log, load_state='actcase')





