import os
import math
import glob
import pandas as pd
import shapefile  # pip install pyshp
from shapely.geometry import LineString, Point  # Pip install shapely
from rtree import index  # Wheel from Chris Gohlke's  website
import parameters as Param
from time import clock
from operator import itemgetter
import sys

file_output = 'D:/select_link_analysis_single_direction.csv'
PATH_INPUT_FILE = 'MAG_JAN14_R_trajectories.h5'
link_id = 9277
direction = 1
both_directions = True

retrieve = pd.HDFStore(P.matching_results_folder + PATH_INPUT_FILE)
traj = retrieve['/trajectories']
retrieve.close()

# If you need to filter date or day of the week, the code to add those fields are below
# traj['stop_date'] = pd.to_datetime(traj.readdate).map(lambda x: x.strftime('%d-%m-%Y'))
# traj['weekday'] = traj['readdate'].apply(lambda x: x.weekday())

if both_directions:
    unique_trucks_on_link = traj.truckid[traj.link == link_id].unique()

    traces_of_trucks_on_link = traj[traj.truckid.isin(unique_trucks_on_link)]
    ab_flows = traces_of_trucks_on_link[traces_of_trucks_on_link.direction==1].groupby(['link']).agg(['count'])['truckid']
    ab_flows.columns=['AB_FLOW']

    ba_flows = traces_of_trucks_on_link[traces_of_trucks_on_link.direction==-1].groupby(['link']).agg(['count'])['truckid']
    ba_flows.columns=['BA_FLOW']

    flows = ab_flows.join(ba_flows, how='outer')
    flows = flows.fillna(0)
    flows['TOT_FLOW'] = flows.AB_FLOW + flows.BA_FLOW

    flows.to_csv(file_output)

else:
    unique_trucks_on_link = traj.truckid[(traj.link == link_id) & (traj.direction == direction)].unique()
    traces_of_trucks_on_link = traj[traj.truckid.isin(unique_trucks_on_link)]
    ab_flows = traces_of_trucks_on_link[traces_of_trucks_on_link.direction==1].groupby(['link']).agg(['count'])['truckid']
    ab_flows.columns=['AB_FLOW']

    ba_flows = traces_of_trucks_on_link[traces_of_trucks_on_link.direction==-1].groupby(['link']).agg(['count'])['truckid']
    ba_flows.columns=['BA_FLOW']

    flows = ab_flows.join(ba_flows, how='outer')
    flows = flows.fillna(0)
    flows['TOT_FLOW'] = flows.AB_FLOW + flows.BA_FLOW

    flows.to_csv(file_output)

