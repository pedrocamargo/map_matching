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

print 'creating new minute field'
traj['minute'] = traj.readdate.dt.minute

print 'creating new weekday field'
traj['weekday'] = traj.readdate.dt.weekday

print 'creating new date field'
traj['stop_date'] = traj.readdate.dt.date

print 'creating new ID'
traj["new_id"] = traj.groupby(['truckid', 'File', 'stop_date']).grouper.group_info[0]

traj.drop('File', axis=1, inplace=True)
traj.drop('x', axis=1, inplace=True)
traj.drop('y', axis=1, inplace=True)


#Get only weekdays
traj = traj[traj.weekday<5]
total_week_days = traj.stop_date.unique().shape[0]

timerange = pd.date_range("00:00", "23:50", freq="5min").to_pydatetime()
intervals = timerange.shape[0]
variation = 2 # Half of the size of the interval we are considering for averaging for a single minute
             #  Each unit represents the size of the interval used in the timerange above
flows = None

for period in xrange(variation, intervals-variation):
    print timerange[period].time()

    # Get only the appropriate time period
    from_time = timerange[period - variation].time()
    to_time = timerange[period + variation].time()
    period_flow = traj[(traj.time>=from_time) & (traj.time<to_time)]

    # Total flows for the period in hand
    ab_flows = period_flow[period_flow.direction==1].groupby(['link']).agg(['count'])['truckid'] / total_week_days
    ab_flows.columns=['AB_' + str(period)]

    ba_flows = period_flow[period_flow.direction==-1].groupby(['link']).agg(['count'])['truckid'] / total_week_days
    ba_flows.columns=['BA_' + str(period)]

    #Totalize them
    if flows is None:
        flows = ab_flows.join(ba_flows, how='outer')
        flows = flows.fillna(0)
        flows['TOT_' + str(period)] = flows['AB_' + str(period)] + flows['BA_' + str(period)]
    else:
        flows = flows.join(ab_flows, how='outer')
        flows = flows.join(ba_flows, how='outer')
        flows = flows.fillna(0)
        flows['TOT_' + str(period)] = flows['AB_' + str(period)] + flows['BA_' + str(period)]


flows.to_csv(P.matching_results_folder + 'minute_flows.csv')