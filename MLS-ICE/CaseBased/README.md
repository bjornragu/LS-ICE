Directory provides scripts which can be used to encoded inter-case features using the case based version of the MLS-ICE framework.  <br> 

- 00_Get_CandDur.py: Can be used to obtain a range of candidate duration values used during search in 01_lag_getOptDur.py, 01_lead_getOptDur.py and 01_location_getOptDur.py. The results from running this script on the provided event log (evlog.csv) are provided in dur_range_dic.pickle <br> 
- 01_lag_getOptDur.py: Identifies the optimal duration when computing a load state at the previous (lag) location of cases (results from running this script is provided in configs/lag_configs/) <br>
- 01_location_getOptDur.py: Identifies the optimal duration when computing a load state at the current location of cases (results from running this script is provided in configs/location_configs/) <br>
- 01_lead_getOptDur.py: Identifies the optimal duration when computing a load state at likley next locations for cases (results from running this script is provided in configs/lead_configs/) <br>


02_Comp_lag_load.py
Add files via upload
2 days ago
02_Comp_lead_loads.py
Add files via upload
2 days ago
02_Comp_location_load.py
Add files via upload
2 days ago

