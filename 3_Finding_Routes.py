__author__  =  'pcamargo'


import sys, os
import ntpath
import numpy as np
import pandas as pd
import glob
from time import clock
from datetime import timedelta
from rtree import index  # Wheel from Chris Gohlke's  website
import shapefile

sys.path.append("C:/Users/pedro.camargo.VEITCHLISTER/.qgis2/python/plugins/AequilibraE/")
from aequilibrae.paths import Graph, PathResults, path_computation
from parameters import load_parameter
from geopy.distance import vincenty as gc

def main():

    vehicle_id = load_parameter('fields', 'vehicle id')
    speed = load_parameter('fields', 'speed')
    latitude = load_parameter('fields', 'latitude')
    longitude = load_parameter('fields', 'longitude')
    azimuth = load_parameter('fields', 'azimuth')
    timestamp = load_parameter('fields', 'timestamp')

    tolerance = load_parameter('geoprocessing', 'azimuth tolerance')
    node_layer_fields = load_parameter('geoprocessing', 'node file fields')

    shape_folder = load_parameter('folders', 'layer folder')
    net_file = load_parameter('file names', 'network file')
    nodes_layer = load_parameter('file names', 'nodes layer')
    buffer_size = load_parameter('geoprocessing', 'buffer size')
    network_parameters =  load_parameter('geoprocessing', 'network file fields')
    links_per_route_folder =  load_parameter('folders', 'links per route folder')
    stops_folder = load_parameter('folders', 'stops folder')
    temp_folder = load_parameter('folders', 'temp folder')
    
    data_folder = load_parameter('folders', 'hdf5 folder')
    aequilibrae_graph = load_parameter('file names', 'aequilibrae graph')

# Preparing GRAPH
    print 'Reading graph'
    graph = Graph()
    graph.load_from_disk(shape_folder + aequilibrae_graph)
    graph.set_graph()
    results= PathResults()
    results.prepare(graph)
    results.reset()

    print 'Reading network nodes'
    nodes = shapefile.Reader(shape_folder + nodes_layer)
    NodeRecs = nodes.shapeRecords()

    nid = node_layer_fields['id']
    for i, f in enumerate(nodes.fields):
        if f[0] == nid:
            nid = i - 1
            break

    print "Assembling spatial index for network nodes"
    idx = index.Index()
    for q in NodeRecs:
        y, x = q.shape.points[0]
        bbox = (y, x, y, x)
        idx.insert(q.record[nid], bbox)

    err = 0
    for ProcessingFile in glob.glob(links_per_route_folder + "*.h5"):
        head, ProcessingFile = ntpath.split(ProcessingFile)

        print 'Processing file:', ProcessingFile[0:-8]

        print '     Loading GPS pings'
        retrieve = pd.HDFStore(data_folder + ProcessingFile[0:-9] + '.h5')
        gps_data = retrieve['gps_data']
        retrieve.close()

        print '     Computing unique vehicle IDs'
        vehicle_ids = gps_data[vehicle_id].unique()
        tot_vehicles = vehicle_ids.shape[0]

        print '     Loading USED LINKS'
        used_links = pd.HDFStore(links_per_route_folder + ProcessingFile)

        print '     Loading vehicle stops'
        vehicle_stops = pd.HDFStore(stops_folder + ProcessingFile[0:-8] + 'stops.h5')
        vs = vehicle_stops['/STOPS']
        vehicle_stops.close()

        T = clock()
        for z, vi in enumerate(vehicle_ids):
            if z % 50 == 0:
                print 'Analyzing vehicles:', round((100.0*z)/tot_vehicles, 2), '%%', int(clock() - T), 's / Errors:', err
            # try:
            vehicle_trace = gps_data[gps_data[vehicle_id] == vi]

            stops = vs[vs[vehicle_id] == vi]

            # We re-factor the graph cost to make it cheaper to use the flagged links if any links were flagged
            try:
                a = used_links['/' + str(vi)].unique()
                graph.cost[graph.ids[a]] /= 10
            except:
                pass

            # # We select the nodes that are part of the signalized links
            # nodes_a = graph.graph['a_node'][graph.ids[a]]
            # nodes_b = graph.graph['b_node'][graph.ids[a]]
            # all_nodes = np.hstack((nodes_a, nodes_b))
            #
            # all_nodes = np.bincount(all_nodes)

            # We correlate the stop sequence with the nodes signalized
            # to find the node from which we will trace the paths from/to

            stop_sequence = []
            for i in stops.index:
                x = stops.x[i]
                y = stops.y[i]
                t = stops.stop_time[i]
                dur = stops.duration[i]
                #closest_nodes = list(idx.nearest((y, x, y, x), 5000))
                closest_nodes = list(idx.nearest((y, x, y, x), 5000))
                stop_sequence.append((closest_nodes[0], t, dur))

                # for j in closest_nodes:
                #     if j < all_nodes.shape[0]:
                #         if all_nodes[j]>0:
                #             stop_sequence.append((j,t,dur)) # The time stamp and duration will be needed to setup
                #                                             # the time stamps for all the nodes along the path
                #             break

            for i in stop_sequence:
                #Now we need to compute the paths with the sequence of stops
                all_links = []
                all_nodes = []
                all_arrives_tstamps = []
                all_leaves_tstamps = []

                x = np.zeros(1, np.int64)
                x[0] = -1

                for i in range(len(stop_sequence) - 1):
                    distances = {}
                    origin = stop_sequence[i][0]
                    destination = stop_sequence[i + 1][0]
                    if origin != destination:
                        arrives_tstamps = {}
                        leaves_tstamps = {}
                        path_computation(origin, destination, graph, results)
                        if results.path is not None:
                            all_links.append(results.path.copy())
                            all_links.append(x)
                            all_nodes.append(results.pathnodes)

                            #Now we build a spatial index with the subset of pings in the truck trace between the origin and destination
                            ping_subset = vehicle_trace[(vehicle_trace[timestamp] > stop_sequence[i][1] + timedelta(seconds=stop_sequence[i][2])) & (vehicle_trace[timestamp] < stop_sequence[i + 1][1])]

                            temp_idx = index.Index()
                            for q in all_nodes[-1]:
                                x = nodes.X[q]
                                y = nodes.Y[q]
                                bbox=(x, y, x, y)
                                temp_idx.insert(q, bbox)

                            for k in ping_subset.index:
                                x = vehicle_trace.x[k]
                                y = vehicle_trace.y[k]
                                near = list(temp_idx.nearest((x, y, x, y), 1))
                                if len(near) > 0:
                                    q = near[0]
                                    x2 = nodes.X[q]
                                    y2 = nodes.Y[q]
                                    d = gc((y, x), (y2, x2))
                                    if q not in distances:
                                        leaves_tstamps[q] = vehicle_trace[timestamp][k]
                                        arrives_tstamps[q] = vehicle_trace[timestamp][k]
                                        distances[q] = d
                                    else:
                                        if d < distances[q]:
                                            leaves_tstamps[q] = vehicle_trace[timestamp][k]
                                            arrives_tstamps[q] = vehicle_trace[timestamp][k]
                                            distances[q] = d

                            # Time stamps for the origin and destination of the path

                            leaves_tstamps[origin] = stop_sequence[i][1] + timedelta(seconds=stop_sequence[i][2])
                            arrives_tstamps[destination] = stop_sequence[i + 1][1]
                            InterpolateTS(arrives_tstamps, leaves_tstamps, results.pathnodes, results.path.copy(), graph)

                            # For complitude
                            if 0 < stop_sequence[i + 1][2] < 100000:
                                leaves_tstamps[destination] = arrives_tstamps[destination] + timedelta(seconds=stop_sequence[i + 1][2])
                            else:
                                leaves_tstamps[destination] = np.nan

                            if 0 < stop_sequence[i][2] < 100000:
                                arrives_tstamps[origin] = stop_sequence[i][1]
                            else:
                                arrives_tstamps[origin] = np.nan

                            q1 = []
                            q2 = []
                            for k in all_nodes[-1]:
                                q1.append(arrives_tstamps[k])
                                q2.append(leaves_tstamps[k])
                            all_arrives_tstamps.append(q1)
                            all_leaves_tstamps.append(q2)
                        results.reset()
            all_links = np.hstack(tuple(all_links))
            all_nodes = np.hstack(tuple(all_nodes))
            all_arrives_tstamps = np.hstack(tuple(all_arrives_tstamps))
            all_leaves_tstamps = np.hstack(tuple(all_leaves_tstamps))

            # We add the truck's path to a dataframe
            df = []
            mp = 0
            for i, q in enumerate(all_nodes):
                if all_links[i] != -1:
                    link = graph.graph['link_id'][all_links[i]]
                else:
                    link = np.nan
                direc = graph.graph['direction'][all_links[i]]

                df.append([str(vi),filename, all_arrives_tstamps[i], all_leaves_tstamps[i], nodes.Orig_ID[q], link, direc, nodes.Y[q], nodes.X[q], mp])
                if all_links[i] > 0:
                    mp = mp + graph.graph['real_length'][all_links[i]]

            df_head = ['VehicleID', 'File', 'Timestamp', 'leavenode', 'node', 'link','direction','x', 'y', 'milepost']
            df = pd.DataFrame(df, columns=df_head)
            df.to_hdf(P.matching_results_folder + ProcessingFile[0:-8] + 'trajectories.h5', 'trajectories', append=True )
            # We reset the graph costs
            graph.cost = orig_cost.copy()
            # except:
            #      err += 1
            #      a = truck_stats[truck_stats.index == vi]
            #      b = float(a.pings)
            #      c = float(a.duration)
            #      d = float(a.coverage)
            #      print err, vi
            #      df = pd.DataFrame([[str(vi), filename, b, c, d]], columns=['truckid', 'File', 'pings', 'duration', 'coverage'])
            #      df.to_hdf(P.matching_results_folder + ProcessingFile[0:-8] + 'trajectories.h5', 'errors', append=True )

        vehicle_stops.close()
        retrieve.close()


def InterpolateTS(arrives_tstamps, leaves_tstamps, pathnodes, all_links,  graph):
    consistent = False
    while not consistent:
        consistent = True
        for i in range(len(pathnodes)-1):
            o = pathnodes[i]
            if o in arrives_tstamps.keys():
                for j in xrange(i + 1, len(pathnodes)):
                    d = pathnodes[j]
                    if d in arrives_tstamps.keys():
                        if arrives_tstamps[o] > arrives_tstamps[d]:
                            if i > 0:
                                leaves_tstamps.pop(o, None)
                                arrives_tstamps.pop(o, None)
                            if d != pathnodes[-1]:
                                arrives_tstamps.pop(d, None)
                                leaves_tstamps.pop(d, None)
                            consistent = False
                            break
                if not consistent:
                    break
    i = 0
    while i < len(pathnodes)-2:
        j = i + 1
        mp = graph.graph['real_length'][all_links[j - 1]]
        while pathnodes[j] not in arrives_tstamps.keys():
            mp += graph.graph['real_length'][all_links[j]]
            j += 1
        
        if j > i + 1:  # Means we have at least one node in the path that does not have any path written to it
            time_diff = (arrives_tstamps[pathnodes[j]] - leaves_tstamps[pathnodes[i]]).total_seconds()
            if time_diff < 0:
                del arrives_tstamps
                del leaves_tstamps
                break
            mp2 = 0
            for k in xrange(i + 1, j):
                mp2 += graph.graph['real_length'][all_links[k - 1]]
                j_time = leaves_tstamps[pathnodes[i]] + timedelta(seconds=time_diff*mp2/mp)
                arrives_tstamps[pathnodes[k]] = j_time
                leaves_tstamps[pathnodes[k]] = j_time
        i += 1

if __name__ == '__main__':
    main()

