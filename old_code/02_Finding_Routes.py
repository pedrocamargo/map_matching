__author__  =  'pcamargo'

import glob
import os
from datetime import timedelta
from time import clock

import numpy as np
import pandas as pd
from geopy.distance import vincenty as gc
from rtree import index  # Wheel from Chris Gohlke's  website

import parameters as P
from map_matching.aequilibrae import Graph, PathResults, path_computation


def main():
    already_done = []
    #already_done = ['MAG_JAN14_R_links.h5', 'MAG_JAN14_S_links.h5', 'MAG_APR14_R_links.h5', 'MAG_JUL14_R_links.h5', 'MAG_OCT14_R_links.h5']
    already_done = ['MAG_JAN14_R_links.h5',\
                    'MAG_JAN14_S_links.h5',\
                    'MAG_APR14_R_links.h5',\
                    'MAG_APR14_S_links.h5',\
                    'MAG_JUL14_R_links.h5',\
                    'MAG_JUL14_S_links.h5',\
                    'MAG_OCT14_S_links.h5']
    #already_done = ['MAG_APR14_R_links.h5', 'MAG_APR14_S_links.h5', 'MAG_JAN14_R_links.h5', 'MAG_JAN14_S_links.h5', 'MAG_JUL14_R_links.h5', 'MAG_JUL14_S_links.h5']
    # []

# Preparing GRAPH
    print 'Reading graph'
    graph = Graph()
    graph.add_single_field('real_length')
    graph.load_graph_from_disk(P.shape_folder + P.graph_file)
    graph.set_graph(10, cost_field = 'length')
    orig_cost=graph.cost.copy()

    results= PathResults()
    results.prepare(graph)
    results.reset()

    print 'Reading network nodes'
    nodes = pd.read_csv(P.shape_folder + P.node_list)
    nodes = nodes.set_index('ID')
    print "Assembling spatial index"
    idx = index.Index()
    for q in nodes.index:
        x = nodes.X[q]
        y = nodes.Y[q]
        bbox=(y, x, y, x)
        idx.insert(q, bbox)


    os.chdir(P.links_per_route_folder)
    err = 0

    for ProcessingFile in glob.glob("*.h5"):
        if ProcessingFile not in already_done:
            print '     Loading USED LINKS'
            used_links = pd.HDFStore(P.links_per_route_folder + ProcessingFile)

            filename = ProcessingFile[0:-8]
            print 'Processing trucks:', ProcessingFile

            print '     Loading unique truck IDs'
            retrieve = pd.HDFStore(P.truck_stats_folder + ProcessingFile[0:-8] + 'truck_stats.h5')
            truck_stats = retrieve['truck_stats']
            tot_trucks = truck_stats.shape[0]
            retrieve.close()

            # Collect all truck IDs
            truck_ids = list(truck_stats.index.values)

            print '     Loading GPS pings'
            retrieve = pd.HDFStore(P.data_folder + ProcessingFile[0:-9] + '.h5')
            atri = retrieve['atri']
            retrieve.close()

            print '     Loading truck stops'
            truck_stops = pd.HDFStore(P.stops_per_route_folder + ProcessingFile[0:-8] + 'stops.h5')
            ts = truck_stops['/STOPS']
            truck_stops.close()

            print '     Loading used links'
            retrieve = pd.HDFStore(ProcessingFile)
            T = clock()

            #truck_ids = ['0149111ab88e0a2600a86479c161']

            for z, ti in enumerate(truck_ids):
                if z % 50 == 0:
                    print 'Analyzing trucks:', round((100.0*z)/tot_trucks, 2), '%%', int(clock() - T), 's / Errors:', err
                try:
                #if True:
                    truck_trace = atri[atri.truckid == ti]
                    #if True:
                        # All links that are part of the truck route
                    stops = ts[ts.truckid == ti]

                    # We re-factor the graph cost to make it cheaper to use the flagged links
                    a = used_links['/' + str(ti)].unique()
                    graph.cost[graph.ids[a]] /= 10

                    # We select the nodes that are part of the signalized links
                    nodes_a = graph.graph['a_node'][graph.ids[a]]
                    nodes_b = graph.graph['b_node'][graph.ids[a]]
                    all_nodes = np.hstack((nodes_a, nodes_b))

                    all_nodes = np.bincount(all_nodes)


                    # We correlate the stop sequence with the nodes signalized
                    # to find the node from which we will trace the paths from/to
                    stop_sequence = []
                    for i in stops.index:
                        x = stops.x[i]
                        y = stops.y[i]
                        t = stops.stop_time[i]
                        dur = stops.duration[i]
                        closest_nodes = list(idx.nearest((y, x, y, x), 5000))
                        for j in closest_nodes:
                            if j < all_nodes.shape[0]:
                                if all_nodes[j]>0:
                                    stop_sequence.append((j,t,dur)) # The time stamp and duration will be needed to setup
                                                                    # the time stamps for all the nodes along the path
                                    break
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
                                    ping_subset = truck_trace[(truck_trace.readdate > stop_sequence[i][1] + timedelta(seconds=stop_sequence[i][2])) & (truck_trace.readdate < stop_sequence[i + 1][1])]

                                    temp_idx = index.Index()
                                    for q in all_nodes[-1]:
                                        x = nodes.X[q]
                                        y = nodes.Y[q]
                                        bbox=(x, y, x, y)
                                        temp_idx.insert(q, bbox)

                                    for k in ping_subset.index:
                                        x = truck_trace.x[k]
                                        y = truck_trace.y[k]
                                        near = list(temp_idx.nearest((x, y, x, y), 1))
                                        if len(near) > 0:
                                            q = near[0]
                                            x2 = nodes.X[q]
                                            y2 = nodes.Y[q]
                                            d = gc((y, x), (y2, x2))
                                            if q not in distances:
                                                leaves_tstamps[q] = truck_trace.readdate[k]
                                                arrives_tstamps[q] = truck_trace.readdate[k]
                                                distances[q] = d
                                            else:
                                                if d < distances[q]:
                                                    leaves_tstamps[q] = truck_trace.readdate[k]
                                                    arrives_tstamps[q] = truck_trace.readdate[k]
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

                        df.append([str(ti),filename, all_arrives_tstamps[i], all_leaves_tstamps[i], nodes.Orig_ID[q], link, direc, nodes.Y[q], nodes.X[q], mp])
                        if all_links[i] > 0:
                            mp = mp + graph.graph['real_length'][all_links[i]]

                    df_head = ['truckid', 'File', 'readdate', 'leavenode', 'node', 'link','direction','x', 'y', 'milepost']
                    df = pd.DataFrame(df, columns=df_head)
                    df.to_hdf(P.matching_results_folder + ProcessingFile[0:-8] + 'trajectories.h5', 'trajectories', append=True )
                    # We reset the graph costs
                    graph.cost = orig_cost.copy()
                except:
                     err += 1
                     a = truck_stats[truck_stats.index == ti]
                     b = float(a.pings)
                     c = float(a.duration)
                     d = float(a.coverage)
                     print err, ti
                     df = pd.DataFrame([[str(ti), filename, b, c, d]], columns=['truckid', 'File', 'pings', 'duration', 'coverage'])
                     df.to_hdf(P.matching_results_folder + ProcessingFile[0:-8] + 'trajectories.h5', 'errors', append=True )

            truck_stops.close()
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

