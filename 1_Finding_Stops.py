#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Pedro Camargo
#
# Created:     01/10/2015
# Copyright:   (c) pcamargo 2015
# Licence:     ALL RIGHTS RESERVED. NOT TO BE USED OR DISTRIBUTED WITHOUT THE AUTHOR'S PERMISSION
#-------------------------------------------------------------------------------
import ntpath
import os
import glob
import pandas as pd
import numpy as np
from geopy.distance import great_circle
import warnings
warnings.filterwarnings("ignore")
from time import clock, sleep
import multiprocessing as mp
from parameters import load_parameter
from math import ceil
# Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/


def main():
    global_time = clock()

    # Retrieves folder structure parameters
    data_folder = load_parameter('folders', 'hdf5 folder')
    stops_folder = load_parameter('folders','stops folder')

    if not os.path.exists(stops_folder):
        os.makedirs(stops_folder)

    # For each of the gps_data datasets
    cores = load_parameter('processing', 'cores')
    pool = mp.Pool(cores)

    for ProcessingFile in glob.glob(data_folder + "*.h5"):
        #process(ProcessingFile)
        pool.apply_async(process, args=(ProcessingFile,))
    pool.close()
    pool.join()

    m, s = divmod(clock() - global_time, 60)
    h, m = divmod(m, 60)
    print '  Processed in', "%d:%02d:%02d" % (h, m, s)
    print 'Total processing time was ', "%d:%02d:%02d" % (h, m, s)
    print 'DONE!!'


def process(ProcessingFile):
    iter_time = clock()
    head, ProcessingFile = ntpath.split(ProcessingFile)

    # folders
    data_folder = load_parameter('folders', 'hdf5 folder')
    log_folder = load_parameter('folders', 'log folder')
    stops_folder = load_parameter('folders', 'stops folder')

    # Data fields
    vehicle_id = load_parameter('fields', 'vehicle id')
    latitude = load_parameter('fields', 'latitude')
    longitude = load_parameter('fields', 'longitude')
    timestamp = load_parameter('fields', 'timestamp')

    # Stops algorithm selection
    stops_algorithm = load_parameter('stops algorithm', 'algorithm')
    stops_parameters = load_parameter('stops algorithm', stops_algorithm)

    # Data quality parameters
    max_speed = load_parameter('data quality', 'max speed')
    max_speed_time = load_parameter('data quality', 'max speed time')
    minimum_pings = load_parameter('data quality', 'minimum pings')
    minimum_coverage = load_parameter('data quality', 'minimum coverage')

    # Load the dataset
    retrieve = pd.HDFStore(data_folder + ProcessingFile)
    gps_data = retrieve['gps_data']
    retrieve.close()

    # Sort dataset
    gps_data = gps_data.sort([vehicle_id, timestamp])

    # Preparing auxiliary data fields
    gps_data['prev_lat'] = gps_data[latitude].shift(1)
    gps_data['prev_long'] = gps_data[longitude].shift(1)
    gps_data['prev_timestamp'] = gps_data[timestamp].shift(1)

    # Load the unique vehicle ids
    list_of_vehicles = pd.DataFrame(gps_data[vehicle_id].unique())

    # Count the total number of vehicles for statistics and reporting purposes
    tot_vehs = list_of_vehicles.index.shape[0]
    h = 0
    f = open(log_folder + 'FindingStops for ' + ProcessingFile[0:-3] + '.log', 'w')
    full_dataframe = None
    all_errors = 0

    print >>f, 'Processing vehicles'

    for ti in list_of_vehicles[0]:
        h += 1
        stops = []
        error = None

        print >>f, '\n Analyzing vehicle:', ti, ' --> ', round((100.0*h)/tot_vehs, 3), '%%'
        f.flush()
        # We retrieve the trace of the vehicle
        vehicle_trace = gps_data[gps_data[vehicle_id] == ti]
        vehicle_trace = vehicle_trace.reset_index(drop=True)

        # Now we correct the auxiliary fields for the first record of each vehicle. It so happens that these
        # are the only records wrong, since we had ordered the pings database by vehicle ID and then by timestamp
        vehicle_trace.loc[0, 'prev_lat'] = vehicle_trace.loc[0, latitude]
        vehicle_trace.loc[0, 'prev_long'] = vehicle_trace.loc[0, longitude]
        vehicle_trace.loc[0, 'prev_timestamp'] = vehicle_trace.loc[0, timestamp]

        vehicle_trace['distance'] = vehicle_trace.apply(lambda x: gc(x[longitude], x[latitude], x['prev_long'], x['prev_lat']), axis=1)
        vehicle_trace['traveled_time'] = vehicle_trace.apply(lambda row: timediff(row[timestamp], row['prev_timestamp']), axis=1)
        vehicle_trace['speed'] = vehicle_trace.apply(lambda row: fspeed(row['distance'], row['traveled_time']), axis=1)

        # Verify data quality
        w = int(vehicle_trace.traveled_time[vehicle_trace.speed > max_speed].sum())
        if w > max_speed_time:
            # If there is evidence of speeding for longer than tolerated, we will see if it happens continuously
            with_error = vehicle_trace[vehicle_trace.speed > max_speed]
            w = 0
            prev = with_error.index[0] - 1
            for we in with_error.index:
                if we == prev + 1:
                    w += with_error.traveled_time[we]
                    prev = we
                else:
                    w = with_error.traveled_time[we]
                if w > max_speed_time:
                    error = 'Max speed surpassed for', w, 'seconds'
                    break

        # Check number of pings
        if vehicle_trace.prev_lat.shape[0] < minimum_pings:
            error = 'Vehicle with only', vehicle_trace.prev_lat.shape[0], 'pings'

        # Test ping coverage
        p1 = [np.max(vehicle_trace[latitude]), np.max(vehicle_trace[longitude])]
        p2 = [np.min(vehicle_trace[latitude]), np.min(vehicle_trace[longitude])]
        if gc(p1[1], p1[0], p2[1], p2[0]) < minimum_coverage:
            error = 'Vehicle covers only', gc(p1[1], p1[0], p2[1], p2[0]), 'km'

        if error is not None:
            all_errors += 1
            print >> f, error
            f.flush()
        else:
            if stops_algorithm == 'Maximum space':
                par_time = stops_parameters['time']
                max_distance = stops_parameters['max distance']
                min_distance = stops_parameters['min distance']

                cd = 0
                ct = 0
                stops.append([vehicle_trace[latitude].iloc[0], vehicle_trace[longitude].iloc[0],
                              vehicle_trace[timestamp].iloc[0], -99999999, 0.0])

                for i in xrange(0, vehicle_trace.index.shape[0]-1):
                    cd += vehicle_trace.distance[i]
                    ct += vehicle_trace.traveled_time[i]
                    if ct > par_time and cd > min_distance or cd > max_distance:
                        cd = 0
                        ct = 0
                        stops.append([vehicle_trace[latitude].iloc[i], vehicle_trace[longitude].iloc[i],
                                      vehicle_trace[timestamp].iloc[i], 0, 0.0])

                stops.append([vehicle_trace[latitude].iloc[-1], vehicle_trace[longitude].iloc[-1],
                              vehicle_trace[timestamp].iloc[-1], 99999999, 0.0])

            if stops_algorithm == 'Delivery stop':
                # algorithm parameters
                stopped_speed = stops_parameters['stopped speed']
                max_speed = stops_parameters['max speed']
                max_speed_time = stops_parameters['max speed time']
                max_stop_coverage = stops_parameters['max stop coverage']
                min_time_stopped = stops_parameters['min time stopped']

                vehicle_trace['stopped'] = vehicle_trace.apply(lambda row: fstop(row['speed'], stopped_speed), axis=1)

                # We check if the vehicle was speeding too much
                if error is None:
                    all_stopped = vehicle_trace.index[vehicle_trace.stopped == 1]
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
                            tot_time = np.sum(vehicle_trace.traveled_time[start_events[i]:end_events[i]])
                            if min_time_stopped < tot_time:
                                x_min = np.min(vehicle_trace[longitude][start_events[i]:end_events[i]])
                                x_max = np.max(vehicle_trace[longitude][start_events[i]:end_events[i]])
                                y_min = np.min(vehicle_trace[latitude][start_events[i]:end_events[i]])
                                y_max = np.max(vehicle_trace[latitude][start_events[i]:end_events[i]])
                                coverage = gc(x_min, y_max, x_max, y_min)
                                if coverage <= max_stop_coverage:
                                    x_avg = np.average(vehicle_trace[longitude][start_events[i]:end_events[i]])
                                    y_avg = np.average(vehicle_trace[latitude][start_events[i]:end_events[i]])
                                    stop_time = vehicle_trace.readdate[start_events[i]]
                                    stops.append([x_avg, y_avg, stop_time, tot_time, coverage])
                else:
                    # We append the first and last ping for each vehicle
                    stops.insert(0,[vehicle_trace[longitude].iloc[-0], vehicle_trace[latitude].iloc[-0], vehicle_trace[timestamp].iloc[-0], -99999999, 0.0])
                    stops.append([vehicle_trace[longitude].iloc[-1], vehicle_trace[latitude].iloc[-1], vehicle_trace[timestamp].iloc[-1], 99999999, 0.0])

            df = pd.DataFrame(stops, columns=[latitude, longitude, 'stop_time', 'duration', 'coverage'])
            df[vehicle_id] = ti
            df['original_file'] = ProcessingFile[0:-3]
            if full_dataframe is None:
                full_dataframe = df.copy(deep=True)
            else:
                full_dataframe = full_dataframe.append(df, ignore_index=True)
            if len(stops) > 0:
                print >>f, '     Internal stops found: ', len(stops)
            else:
                print >>f, '     No stops found'

    print >> f, 'Saving Output'
    # Create the file to hold the stops for this particular vehicle id
    if full_dataframe is not None:
        store = pd.HDFStore(stops_folder + ProcessingFile[0:-3] + '_Stops.h5')
        store['/STOPS'] = full_dataframe
        store.close()
    print >> f,  'VEHICLE WITH ERRORS:', all_errors
    m, s = divmod(clock() - iter_time, 60)
    h, m = divmod(m, 60)
    print >> f,  'Running time:', "%d:%02d:%02d" % (h, m, s)
    f.flush()
    f.close()
    del gps_data
    del full_dataframe
    del list_of_vehicles
    print ProcessingFile, 'processed successfully. Total errors:', all_errors


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


if __name__ == '__main__':
     main()
