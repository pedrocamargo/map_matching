__author__  =  'pcamargo'

from datetime import timedelta
import numpy as np
import pandas as pd
from rtree import index  # Wheel from Chris Gohlke's  website

from map_matching.aequilibrae import Graph, PathResults, path_computation
from geopy.distance import vincenty as gc

def find_route(trip, network):
    vehicle_trace = trip.gps_trace
    stops = trip.stops
    a = pd.Series(trip.graph_links).unique()
    aux_neg1 = np.zeros(1, np.int_)
    aux_neg1.fill(-1)
    network.reset_costs()
    network.graph.cost[a[:]] /= 10

    # # We select the nodes that are part of the signalized links
    nodes_a = network.graph.graph['a_node'][network.graph.ids[a]]
    nodes_b = network.graph.graph['b_node'][network.graph.ids[a]]
    all_nodes = np.hstack((nodes_a, nodes_b))
    all_nodes = np.bincount(all_nodes)

    # Correlate the stop sequence with the nodes signalized to find the node from which we will trace the paths from/to
    stop_sequence = []
    for i in stops.index:
        x = stops.longitude[i]
        y = stops.latitude[i]
        t = stops.stop_time[i]
        dur = stops.duration[i]
        closest_nodes = list(network.idx_nodes.nearest((y, x, y, x), 5000))

        found_node = False
        for j in closest_nodes:
            if j < all_nodes.shape[0]:
                if all_nodes[j] > 0:
                    found_node = True
                    same = False
                    if stop_sequence:
                        if stop_sequence[-1][0] == j:
                            same = True
                    if not same:
                        stop_sequence.append((j, t, dur))  # The time stamp and duration will be needed to setup
                        # the time stamps for all the nodes along the path
                    break
        if not found_node:
            stop_sequence.append((closest_nodes[0], t, dur))

    results = PathResults()
    results.prepare(network.graph)
    #Now we need to compute the paths with the sequence of stops
    all_links = []
    all_nodes = []
    all_arrives_tstamps = []
    all_leaves_tstamps = []

    for i in range(len(stop_sequence) - 1):
        distances = {}
        origin = stop_sequence[i][0]
        destination = stop_sequence[i + 1][0]
        if origin != destination:
            arrives_tstamps = {}
            leaves_tstamps = {}
            path_computation(origin, destination, network.graph, results)
            if results.path is not None:
                #Now we build a spatial index with the subset of pings in the truck trace between the origin and destination
                ping_subset = vehicle_trace[
                    (vehicle_trace.timestamp > stop_sequence[i][1] + timedelta(seconds=int(stop_sequence[i][2])))
                    & (vehicle_trace.timestamp < stop_sequence[i + 1][1])]

                temp_idx = index.Index()
                for q in list(results.path_nodes):
                    x = network.nodes.X[q]
                    y = network.nodes.Y[q]
                    bbox = (x, y, x, y)
                    temp_idx.insert(q, bbox)

                for k in ping_subset.index:
                    x = vehicle_trace.longitude[k]
                    y = vehicle_trace.latitude[k]
                    near = list(temp_idx.nearest((x, y, x, y), 1))
                    if len(near) > 0:
                        q = near[0]
                        x2 = network.nodes.X[q]
                        y2 = network.nodes.Y[q]
                        d = gc((y, x), (y2, x2))
                        if q not in distances:
                            leaves_tstamps[q] = vehicle_trace.timestamp[k]
                            arrives_tstamps[q] = vehicle_trace.timestamp[k]
                            distances[q] = d
                        else:
                            if d < distances[q]:
                                leaves_tstamps[q] = vehicle_trace.timestamp[k]
                                arrives_tstamps[q] = vehicle_trace.timestamp[k]
                                distances[q] = d

                                # Time stamps for the origin and destination of the path
                arrives_tstamps[origin] = stop_sequence[i][1]
                leaves_tstamps[origin] = stop_sequence[i][1] + timedelta(seconds=int(stop_sequence[i][2]))

                arrives_tstamps[destination] = stop_sequence[i + 1][1]
                leaves_tstamps[destination] = stop_sequence[i+1][1] + timedelta(seconds=int(stop_sequence[i+1][2]))

                InterpolateTS(arrives_tstamps, leaves_tstamps, results.path_nodes, results.path.copy(), network)

                # For completeness
                if 0 < stop_sequence[i + 1][2] < 100000:
                    leaves_tstamps[destination] = arrives_tstamps[destination] + timedelta(
                        seconds=int(stop_sequence[i + 1][2]))
                else:
                    leaves_tstamps[destination] = np.nan

                if 0 < stop_sequence[i][2] < 100000:
                    arrives_tstamps[origin] = stop_sequence[i][1]
                else:
                    arrives_tstamps[origin] = np.nan

                q1 = []
                q2 = []
                for k in list(results.path_nodes):
                    q1.append(arrives_tstamps[k])
                    q2.append(leaves_tstamps[k])
                all_arrives_tstamps.append(q1)
                all_leaves_tstamps.append(q2)

                all_links.append(results.path.copy())
                all_links.append(aux_neg1)
                all_nodes.append(results.path_nodes.copy())

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
            link = all_links[i]
        else:
            link = np.nan
        direc = network.graph.graph['direction'][all_links[i]]
        df.append([all_arrives_tstamps[i], all_leaves_tstamps[i], network.nodes.index[q], link, direc, network.nodes.Y[q], network.nodes.X[q], mp])
        if all_links[i] > 0:
            mp = mp + network.interpolation_cost[all_links[i]]

    df_head = ['Timestamp', 'leavenode', 'node', 'link','direction','x', 'y', 'milepost']
    df = pd.DataFrame(df, columns=df_head)
    trip.path = df
    # We reset the graph costs
    network.graph.cost = network.orig_cost.copy()

def InterpolateTS(arrives_tstamps, leaves_tstamps, pathnodes, all_links,  network):
    consistent = False
    while not consistent:
        consistent = True
        for i in range(len(pathnodes)-1):
            o = pathnodes[i]
            if o in leaves_tstamps.keys():
                for j in xrange(i + 1, len(pathnodes)):
                    d = pathnodes[j]
                    if d in arrives_tstamps.keys():
                        if leaves_tstamps[o] > arrives_tstamps[d]:
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
        mp = network.interpolation_cost[all_links[j - 1]]
        while pathnodes[j] not in arrives_tstamps.keys():
            mp += network.interpolation_cost[all_links[j]]
            j += 1
        
        if j > i + 1:  # Means we have at least one node in the path that does not have any timestamp written to it
            time_diff = (arrives_tstamps[pathnodes[j]] - leaves_tstamps[pathnodes[i]]).total_seconds()
            if time_diff < 0:
                del arrives_tstamps
                del leaves_tstamps
                break
            mp2 = 0
            for k in xrange(i + 1, j):
                mp2 += network.interpolation_cost[all_links[k - 1]]
                j_time = leaves_tstamps[pathnodes[i]] + timedelta(seconds=time_diff*mp2/mp)
                arrives_tstamps[pathnodes[k]] = j_time
                leaves_tstamps[pathnodes[k]] = j_time
        i = j


