import os
import numpy as np
from time import clock
import pandas as pd
import parameters as P


os.chdir(P.matching_results_folder)

frames = ['MAG_JAN14_R_trajectories.h5', 'MAG_JAN14_S_trajectories.h5', 'MAG_APR14_R_trajectories.h5',
          'MAG_APR14_S_trajectories.h5', 'MAG_JUL14_R_trajectories.h5', 'MAG_JUL14_S_trajectories.h5',
          'MAG_OCT14_R_trajectories.h5', 'MAG_OCT14_S_trajectories.h5']

traj=None

for f in frames:
    print f
    retrieve = pd.HDFStore(f)
    a = retrieve['trajectories']
    retrieve.close()
    if traj is None:
        traj = a.copy(deep=True)
    else:
        traj = traj.append(a, ignore_index=True)


print 'creating new hour field'
traj['hour'] = traj.readdate.dt.hour

print 'creating new weekday field'
traj['weekday'] = traj.readdate.dt.weekday

print 'creating new date field'
traj['stop_date'] = traj.readdate.dt.date

print 'creating new ID'
traj["new_id"] = traj.groupby(['truckid', 'File', 'stop_date']).grouper.group_info[0]

traj.drop('File', axis=1, inplace=True)
traj.drop('x', axis=1, inplace=True)
traj.drop('y', axis=1, inplace=True)

for h in range(25):
    ab_flows = traj[(traj.direction==1) & (traj.hour==h)].groupby(['link']).agg(['count'])['truckid']
    ab_flows.columns=['AB_FLOW']

    ba_flows = traj[(traj.direction==-1) & (traj.hour==h)].groupby(['link']).agg(['count'])['truckid']
    ba_flows.columns=['BA_FLOW']

    flows = ab_flows.join(ba_flows, how='outer')
    flows = flows.fillna(0)
    flows['TOT_FLOW'] = flows.AB_FLOW + flows.BA_FLOW


    file_output = 'D:/Full_Assignment-Hour ' + str(h) + '.csv'
    flows.to_csv(file_output)


ab_flows = traj[traj.direction==1].groupby(['link']).agg(['count'])['truckid']
ab_flows.columns=['AB_FLOW']

ba_flows = traj[traj.direction==-1].groupby(['link']).agg(['count'])['truckid']
ba_flows.columns=['BA_FLOW']

flows = ab_flows.join(ba_flows, how='outer')
flows = flows.fillna(0)
flows['TOT_FLOW'] = flows.AB_FLOW + flows.BA_FLOW


file_output = 'D:/Full_Assignment- 24 Hour.csv'
flows.to_csv(file_output)