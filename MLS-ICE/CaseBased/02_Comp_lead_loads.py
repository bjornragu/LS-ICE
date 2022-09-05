'''
The script computes case based location loads as defined using the MLS-ICE framework for all events in log of interest.
The script takes as input a dictionary containing the optimal durations for locations (path set in line 24)
and an event log (imported in line 146).
'''

import os
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
import time
import joblib

def get_lead_ts(log):
    
    temp_log = log.copy()
    temp_log = temp_log.sort_values(['case_id', 'event_id'])
    temp_log['ts_next'] = temp_log.ts.shift(-1)
    temp_log.loc[temp_log['activity'] == '<EOS>', 'ts_next'] = np.nan
    
    return log.merge(temp_log[['event_id', 'ts_next']], left_on='event_id', right_on='event_id')

def get_all_configs(conf_dir = 'configs/lead_configs/'):
        
    configurations = {}

    for conf in os.listdir(conf_dir):

        if 'configuration' in conf:
            configurations.update(joblib.load(conf_dir + conf))

    #print('config keys:', list(configurations.keys()))
    
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
            
def get_lead_location(log, location):
    
    #ev_ids = np.array(log[log.activity.str.startswith(target_activity)].event_id)+1 
    ev_ids = np.array(log[log.activity == location].event_id)+1 
    leads = log[log.event_id.isin(ev_ids)]['activity'].value_counts()

    return leads

def get_lead_location_dict(log, lead):
    
    lead_dic = {}
    locations = log.activity.value_counts().index
    
    for location in locations:
        
        lead_dic[location] = {}
        
        if location == '<EOS>':
            lead_dic[location] = ['NoLeadLocation']
            continue
        
        leads = get_lead_location(log, location)
        lead_dic[location] = list(leads.index[:lead+1])
        
    return lead_dic

def comp_loadstate_optdur(x, previous, configs, lead):
    
    if len(previous) > 0:
        
        location = x['activity']

        offset = pd.DateOffset(minutes=0)
        diff = pd.DateOffset(minutes=configs[location][list(configs[location].keys())[lead]][2])
        
        return pd.Series([x.event_id, previous.loc[(previous.index >= x['ts']-diff-offset) & 
                                                   (previous.index < x['ts']-offset)].ts.count()])
    
    else:
        return pd.Series([x.event_id, 0])
    
def comp_loadstate_activecases(x, previous):
    
    return pd.Series([x.event_id, previous.loc[(previous.ts <= x.ts) & (previous.ts_next >= x.ts)].ts.count()])
    
def compute_lead_load(log, location, lead_dic, lead, load_state):
    
    load_comp = None
    
    if len(lead_dic[location]) -1 >= lead:
        print(location, '->', lead_dic[location][lead])
        previous = log.loc[log.activity == lead_dic[location][lead]]
    
    else:
        print(' {} likely next location does not exist for the location {}, return load = 0 for all events:'.format(lead, location))
        previous = log.loc[log.activity.str.startswith('NoNextEvent')]
    
    target_log = log.loc[(log.activity == location)]
    
    if len(target_log) == 0:
        return print('target log empty')
    
    if load_state == 'actcase':
        load_comp = target_log.apply(lambda x: comp_loadstate_activecases(x, previous), axis=1)
    
    if load_state == 'optdur':
        configs = get_all_configs()
        configs = fill_na_configs(configs)
        
        load_comp = target_log.apply(lambda x: comp_loadstate_optdur(x, previous, configs, lead), axis=1) ##add configs here
         
    load_comp.columns = ['event_id', 'lead_{}_load'.format(lead+1)]
    
    return load_comp
    
def get_lead_loads(log, lead=0, load_state='actcase'):
    """
    Function computes a lead load state as defined by the MLS-ICE framework for all events in log.
    Load_state determines which approach for computing the load at a single location is used, i.e. either active number 
    of cases (actcase) or number of events in optimal duration (optdur). Additionally "lead" parameter needs to be set 
    where e.g. lead = 0 returns the load state at the most likely next location given the current location of an event.
    """
    
    load_df = pd.DataFrame(columns=['event_id', 'lead_{}_load'.format(lead+1)])
    lead_dic = get_lead_location_dict(log, lead)
      
    if load_state == 'actcase':
        log = get_lead_ts(log)
        
    for location in tqdm(lead_dic):
        
        load_comp = compute_lead_load(log, location, lead_dic, lead, load_state)
        load_df = load_df.append(load_comp)
    
    return log.merge(load_df, left_on='event_id', right_on='event_id')

log_csv = pd.read_csv('evlog.csv', sep=',')
log_csv.drop(log_csv.columns[0], axis=1, inplace=True)
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('ts')
log_csv.set_index(log_csv.ts, inplace=True)

load_log = get_lead_loads(log_csv, lead=0, load_state='actcase')