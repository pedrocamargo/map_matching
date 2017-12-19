#-------------------------------------------------------------------------------
# Name:        Step 1 in map matching
# Purpose:     Finds the stops for path building
#
# Author:      Pedro Camargo
#
# Created:     09/04/2017
# Copyright:   (c) pcamargo 2017
# Licence:     APACHE 2.0
#-------------------------------------------------------------------------------
import pandas as pd
import numpy as np
from geopy.distance import great_circle
import warnings
warnings.filterwarnings("ignore")

# Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/
def find_stops(trip, stops_table=None):

    # Create data quality fields
    trip.gps_trace['distance'] = trip.gps_trace.apply(lambda x: gc(x['longitude'], x['latitude'], x['prev_long'], x['prev_lat']), axis=1)
    trip.gps_trace['traveled_time'] = trip.gps_trace.apply(lambda row: timediff(row['timestamp'], row['prev_timestamp']), axis=1)
    trip.gps_trace['speed'] = trip.gps_trace.apply(lambda row: fspeed(row['distance'], row['traveled_time']), axis=1)

# Verify data quality
    w = int(trip.gps_trace.traveled_time[trip.gps_trace.speed > trip.data_quality_parameters['max_speed']].sum())
    if w > trip.data_quality_parameters['max_speed_time']:
        # If there is evidence of speeding for longer than tolerated, we will see if it happens continuously
        with_error = trip.gps_trace[trip.gps_trace.speed > trip.data_quality_parameters['max_speed']]
        w = 0
        prev = with_error.index[0] - 1
        for we in with_error.index:
            if we == prev + 1:
                w += with_error.traveled_time[we]
                prev = we
            else:
                w = with_error.traveled_time[we]
            if w > trip.data_quality_parameters['max_speed_time']:
                trip.error = 'Max speed surpassed for', w, 'seconds'
                break

    # Check number of pings
    if trip.gps_trace.prev_lat.shape[0] < trip.data_quality_parameters['minimum_pings']:
        trip.error = 'Vehicle with only', trip.gps_trace.prev_lat.shape[0], 'pings'

    # Test ping coverage
    p1 = [np.max(trip.gps_trace['latitude']), np.max(trip.gps_trace['longitude'])]
    p2 = [np.min(trip.gps_trace['latitude']), np.min(trip.gps_trace['longitude'])]
    if gc(p1[1], p1[0], p2[1], p2[0]) < trip.data_quality_parameters['minimum_coverage']:
        trip.error = 'Vehicle covers only', gc(p1[1], p1[0], p2[1], p2[0]), 'km'

# If no error, we start the processing
    if trip.error is None:
        trip.stops = []
        if trip.stops_algorithm == 'Maximum space':
            par_time = trip.stops_parameters['time']
            max_distance = trip.stops_parameters['max distance']
            min_distance = trip.stops_parameters['min distance']
            trip.gps_trace['delivery_stop'] = -1

            cd = 0
            ct = 0
            # Append the first stop
            trip.stops.append([trip.gps_trace['latitude'].iloc[0], trip.gps_trace['longitude'].iloc[0],
                          trip.gps_trace['timestamp'].iloc[0], -99999999, 0.0])

            for i in xrange(0, trip.gps_trace.index.shape[0]-1):
                cd += trip.gps_trace.distance[i]
                ct += trip.gps_trace.traveled_time[i]
                if ct > par_time and cd > min_distance or cd > max_distance:
                    cd = 0
                    ct = 0
                    trip.stops.append([trip.gps_trace['latitude'].iloc[i], trip.gps_trace['longitude'].iloc[i],
                                  trip.gps_trace['timestamp'].iloc[i], 0, 0.0])

            if trip.stops[-1][0:3] != [trip.gps_trace['latitude'].iloc[-1], trip.gps_trace['longitude'].iloc[-1],
                          trip.gps_trace['timestamp'].iloc[-1]]:
                trip.stops.append([trip.gps_trace['latitude'].iloc[-1], trip.gps_trace['longitude'].iloc[-1],
                              trip.gps_trace['timestamp'].iloc[-1], 99999999, 0.0])
            else:
                trip.stops[-1][3] = 99999999

        elif trip.stops_algorithm == 'Delivery stop':

            # compute how long the vehicle was stopped for each 
            trip.gps_trace['stopped'] = trip.gps_trace.apply(lambda row: fstop(row['speed'], trip.stops_parameters['stopped speed']), axis=1)
            trip.gps_trace['delivery_stop'] = 0
            trip.gps_trace['traveled_time'] = trip.gps_trace["traveled_time"].shift(-1)

            # We check if the vehicle was speeding too much
            all_stopped = trip.gps_trace.index[trip.gps_trace.stopped == 1]
            if all_stopped.shape[0] > 0:

                # Will look for all the points where the stop ended and loop between them only
                start_events = np.array(all_stopped)[:-1] - np.array(all_stopped)[1:]
                start_events = list(np.nonzero(start_events[:]+1)[0] + 1)
                start_events.insert(0, 0)
                start_events = all_stopped[start_events]

                end_events = np.array(all_stopped)[1:] - np.array(all_stopped)[:-1]
                end_events = list(np.nonzero(end_events[:]-1)[0])
                end_events.append(all_stopped.shape[0]-1)
                end_events = all_stopped[end_events]+1

                for i in range(len(end_events)):
                    tot_time = np.sum(trip.gps_trace.traveled_time[start_events[i]:end_events[i]])
                    if trip.stops_parameters['min time stopped'] < tot_time:
                        x_min = np.min(trip.gps_trace['longitude'][start_events[i]:end_events[i]])
                        x_max = np.max(trip.gps_trace['longitude'][start_events[i]:end_events[i]])
                        y_min = np.min(trip.gps_trace['latitude'][start_events[i]:end_events[i]])
                        y_max = np.max(trip.gps_trace['latitude'][start_events[i]:end_events[i]])
                        coverage = gc(x_min, y_max, x_max, y_min)
                        if coverage <= trip.stops_parameters['max stop coverage']:
                            trip.gps_trace.ix[start_events[i]:end_events[i],'delivery_stop'] = 1
                            x_avg = np.average(trip.gps_trace['longitude'][start_events[i]:end_events[i]])
                            y_avg = np.average(trip.gps_trace['latitude'][start_events[i]:end_events[i]])
                            stop_time = trip.gps_trace.timestamp[start_events[i]]
                            trip.stops.append([x_avg, y_avg, stop_time, tot_time, coverage])
            else:
                # We append the first and last ping for each vehicle
                trip.stops.insert(0,[trip.gps_trace['latitude'].iloc[-0], trip.gps_trace['timestamp'].iloc[-0], -99999999, 0.0])
                trip.stops.append([trip.gps_trace['longitude'].iloc[-1], trip.gps_trace['latitude'].iloc[-1], trip.gps_trace['timestamp'].iloc[-1], 99999999, 0.0])

            trip.gps_trace.delivery_stop = trip.gps_trace.delivery_stop * trip.gps_trace.stopped

        elif trip.stops_algorithm == 'Exogenous':
            trip.stops = stops_table
        trip.stops = pd.DataFrame(trip.stops, columns=['latitude', 'longitude', 'stop_time', 'duration', 'coverage'])

def fstop(speed, stopped_speed):
    if speed < stopped_speed:
        return 1
    else:
        return 0

def timediff(time1, time2):
    return float((time1 - time2).seconds)

def fspeed(dist, tim):
    if tim > 0:
        return dist/(tim/3600)
    else:
        return -1

#Great circle distance function
def gc(a, b, c, d):
    p1 = (b, a)
    p2 = (d, c)
    return great_circle(p1, p2).kilometers
