Directory provides scripts which can be used to encoded inter-case features using the system based version of the MLS-ICE framework.  <br> 

- 00_Get_CandDur.py: Can be used to obtain a range of candidate duration values used during search (i.e. in 01_lag_getOptDur.py, 01_lead_getOptDur.py and 01_location_getOptDur.py), the results from running this script on the provided event log (evlog.csv) are provided in dur_range_dic.pickle
- 00_Get_RelLocations.py.py: The script identifies relevant locations of a business process using the system based MLS-ICE <br> 
- 01_Get_OptDur.py.py: The script identifies the optimal duration for all locations deemed important by system based MLS-ICE <br>
- 02_Compute_SysLoads.py: The script computes system based MLS-ICE features for events in log of interest <br>
