from typing import List

from load_parameters import load_parameters
from network import Network
from trip import Trip

import pandas as pd
import numpy as np
from geopy.distance import great_circle
import warnings

warnings.filterwarnings("ignore")
from shapely.geometry import LineString, Point  # Pip install shapely

from datetime import timedelta

from aequilibrae.paths import PathResults, path_computation
from geopy.distance import vincenty as gc


class MapMatcher:
    def __init__(self):
        self.network: Network()
        self.trips = []  # type: List[Trip]
        self.output_folder = None
        self.parameters = Parameters()

        self.get_default_parameters()

    def get_default_parameters(self):
        keys = [
            "data quality",
            "geoprocessing parameters",
            "network file fields",
            "nodes file fields",
            "stops parameters",
            "map matching",
        ]
        for k in keys:
            self.parameters[k] = load_parameters(k)

    def set_output_folder(self, output_folder):
        # Name of the output folder
        self.output_folder = output_folder

    def set_stop_algorithm(self, stop_algorithm):
        self.trip.set_stop_algorithm(stop_algorithm)
        if stop_algorithm != "Exogenous":
            self.trip.stops_parameters = self.parameters["stops parameters"][stop_algorithm]

    def load_network(self, network_file):
        self.network.output_folder = self.output_folder
        par = self.parameters["geoprocessing parameters"]
        self.network.set_geometry_parameters(par)

        p = self.parameters["network file fields"]
        self.network.load_network(network_file, p)

    def load_nodes(self, nodes_file, id_field):
        self.network.load_nodes(nodes_file, id_field)

    def load_gps_pings(self, gps_data, field_dictionary):
        self.trip.set_data_quality_parameters(self.parameters["data quality"])
        self.trip.populate_with_dataframe(gps_data, field_dictionary)
        if self.trip.error is not None:
            raise ValueError(self.trip.error)

    def load_stops(self, stops_table):
        self.trip.stops = stops_table

    def execute(self):
        self.find_stops()
        if self.trip.stops is not None:
            self.find_network_links()
            self.find_route()
        else:
            raise ValueError("No trip stops to compute paths from")

    # Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/
    def find_stops(self):
        # If no error and if we actually need to find stops, we start the processing
        if self.trip.error is None and self.trip.stops_algorithm in ["Maximum space", "Delivery stop"]:
            self.trip.stops = []
            if self.trip.stops_algorithm == "Maximum space":
                par_time = self.trip.stops_parameters["time"]
                max_distance = self.trip.stops_parameters["max distance"]
                min_distance = self.trip.stops_parameters["min distance"]
                self.trip.gps_trace["delivery_stop"] = -1

                cd = 0
                ct = 0
                # Append the first stop
                self.trip.stops.append(
                    [
                        self.trip.gps_trace["latitude"].iloc[0],
                        self.trip.gps_trace["longitude"].iloc[0],
                        self.trip.gps_trace["timestamp"].iloc[0],
                        0.0,
                        0.0,
                    ]
                )

                for i in range(0, self.trip.gps_trace.index.shape[0] - 1):
                    cd += self.trip.gps_trace.distance[i]
                    ct += self.trip.gps_trace.traveled_time[i]
                    if ct > par_time and cd > min_distance or cd > max_distance:
                        cd = 0
                        ct = 0
                        self.trip.stops.append(
                            [
                                self.trip.gps_trace["latitude"].iloc[i],
                                self.trip.gps_trace["longitude"].iloc[i],
                                self.trip.gps_trace["timestamp"].iloc[i],
                                0,
                                0.0,
                            ]
                        )

                if self.trip.stops[-1][0:3] != [
                    self.trip.gps_trace["latitude"].iloc[-1],
                    self.trip.gps_trace["longitude"].iloc[-1],
                    self.trip.gps_trace["timestamp"].iloc[-1],
                ]:
                    self.trip.stops.append(
                        [
                            self.trip.gps_trace["latitude"].iloc[-1],
                            self.trip.gps_trace["longitude"].iloc[-1],
                            self.trip.gps_trace["timestamp"].iloc[-1],
                            99999999,
                            0.0,
                        ]
                    )
                else:
                    self.trip.stops[-1][3] = 99999999

            elif self.trip.stops_algorithm == "Delivery stop":
                # compute how long the vehicle was stopped for each
                self.trip.gps_trace["stopped"] = (
                    self.trip.gps_trace["speed"] < self.trip.stops_parameters["stopped speed"]
                ) * 1
                self.trip.gps_trace["delivery_stop"] = 0
                self.trip.gps_trace["traveled_time"] = self.trip.gps_trace["traveled_time"].shift(-1)

                # We check if the vehicle was speeding too much
                all_stopped = self.trip.gps_trace.index[self.trip.gps_trace.stopped == 1]
                if all_stopped.shape[0] > 0:
                    # Will look for all the points where the stop ended and loop between them only
                    start_events = np.array(all_stopped)[:-1] - np.array(all_stopped)[1:]
                    start_events = list(np.nonzero(start_events[:] + 1)[0] + 1)
                    start_events.insert(0, 0)
                    start_events = all_stopped[start_events]

                    end_events = np.array(all_stopped)[1:] - np.array(all_stopped)[:-1]
                    end_events = list(np.nonzero(end_events[:] - 1)[0])
                    end_events.append(all_stopped.shape[0] - 1)
                    end_events = all_stopped[end_events] + 1

                    for i in range(len(end_events)):
                        tot_time = np.sum(self.trip.gps_trace.traveled_time[start_events[i] : end_events[i]])
                        if self.trip.stops_parameters["min time stopped"] < tot_time:
                            x_min = np.min(self.trip.gps_trace["longitude"][start_events[i] : end_events[i]])
                            x_max = np.max(self.trip.gps_trace["longitude"][start_events[i] : end_events[i]])
                            y_min = np.min(self.trip.gps_trace["latitude"][start_events[i] : end_events[i]])
                            y_max = np.max(self.trip.gps_trace["latitude"][start_events[i] : end_events[i]])
                            coverage = self.gc(x_min, y_max, x_max, y_min)
                            if coverage <= self.trip.stops_parameters["max stop coverage"]:
                                self.trip.gps_trace.ix[
                                    start_events[i] : min(end_events[i], self.trip.gps_trace.shape[0] - 1),
                                    "delivery_stop",
                                ] = 1
                                x_avg = np.average(self.trip.gps_trace["longitude"][start_events[i] : end_events[i]])
                                y_avg = np.average(self.trip.gps_trace["latitude"][start_events[i] : end_events[i]])
                                stop_time = self.trip.gps_trace.timestamp[start_events[i]]
                                self.trip.stops.append([y_avg, x_avg, stop_time, tot_time, coverage])
                else:
                    # We append the first and last ping for each vehicle
                    self.trip.stops.insert(
                        0,
                        [
                            self.trip.gps_trace["latitude"].iloc[-0],
                            self.trip.gps_trace["longitude"].iloc[-0],
                            self.trip.gps_trace["timestamp"].iloc[-0],
                            0.0,
                            0.0,
                        ],
                    )
                    self.trip.stops.append(
                        [
                            self.trip.gps_trace["latitude"].iloc[-1],
                            self.trip.gps_trace["longitude"].iloc[-1],
                            self.trip.gps_trace["timestamp"].iloc[-1],
                            99999999,
                            0.0,
                        ]
                    )

                self.trip.gps_trace.delivery_stop = self.trip.gps_trace.delivery_stop * self.trip.gps_trace.stopped

            self.trip.stops = pd.DataFrame(
                self.trip.stops, columns=["latitude", "longitude", "stop_time", "duration", "coverage"]
            )

    # Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/
    def find_network_links(self):
        veh_speed = -1
        veh_azimuth = -1
        poly = []
        poly_time = []  # An array of [link graph id, timestamp the link is possibly used by a ping]
        all_links = []
        for g, t in enumerate(self.trip.gps_trace.index):
            # Collects all info on a ping
            if self.trip.has_speed:
                veh_speed = self.trip.gps_trace.at[t, "speed"]
            if self.trip.has_azimuth:
                veh_azimuth = self.trip.gps_trace.at[t, "azimuth"]

            y = self.trip.gps_trace.at[t, "latitude"]
            x = self.trip.gps_trace.at[t, "longitude"]
            timestamp = self.trip.gps_trace.at[t, "timestamp"]
            P = (x, y)

            # Finds which links are likely to have been used
            l = self.network.idx_links.intersection(P)
            P = Point(x, y)

            # Loops through them to make sure they are within the buffers
            for j in l:
                direc = self.network.links_df.dir[j]
                graph_id = self.network.links_df.graph_ab[j]
                if direc < 0:
                    graph_id = self.network.links_df.graph_ba[j]

                if P.within(self.network.buffers[j]):
                    if self.trip.has_azimuth:
                        azim = self.network.links_df.azim[j]
                        if direc < 0:
                            azim = self.reverse_azim(azim)

                        if self.check_if_inside(veh_azimuth, azim, self.network.azimuth_tolerance):
                            poly.append(graph_id)
                            poly_time.append([graph_id, timestamp])
                            all_links.append(int(j))
                        if direc == 0:
                            azim = self.reverse_azim(azim)
                            if self.check_if_inside(veh_azimuth, azim, self.network.azimuth_tolerance):
                                poly.append(self.network.links_df.graph_ba[j])
                                poly_time.append([self.network.links_df.graph_ba[j], timestamp])
                    else:
                        poly.append(graph_id)
                        poly_time.append([graph_id, timestamp])
                        all_links.append(int(j))
                        if direc == 0:
                            poly.append(self.network.links_df.graph_ba[j])
                            poly_time.append([self.network.links_df.graph_ba[j], timestamp])

        self.trip.graph_links = poly
        self.trip.graph_links_time = pd.DataFrame(poly_time, columns=["graph_links", "timestamp"])
        self.trip.used_links = all_links

    def find_route(self):
        vehicle_trace = self.trip.gps_trace
        stops = self.trip.stops
        a = pd.Series(self.trip.graph_links).unique()
        aux_neg1 = np.zeros(1, np.int_)
        aux_neg1.fill(-1)

        # We select the nodes that are part of the signalized links
        nodes_a = self.network.graph.graph["a_node"][self.network.graph.ids[a]]
        nodes_b = self.network.graph.graph["b_node"][self.network.graph.ids[a]]
        all_nodes = np.hstack((nodes_a, nodes_b))
        all_nodes = np.bincount(all_nodes)

        # Correlate the stop sequence with the nodes signalized to find the node from which we will trace the paths from/to
        stop_sequence = []
        for i in stops.index:
            x = stops.longitude[i]
            y = stops.latitude[i]
            t = stops.stop_time[i]
            dur = stops.duration[i]
            closest_nodes = list(self.network.idx_nodes.nearest((x, y, x, y), 5000))

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
        results.prepare(self.network.graph)
        # Now we need to compute the paths with the sequence of stops
        all_links = []
        all_nodes = []
        all_arrives_tstamps = []
        all_leaves_tstamps = []

        for i in range(len(stop_sequence) - 1):
            distances = {}
            origin = stop_sequence[i][0]
            o_time = stop_sequence[i][1]
            destination = stop_sequence[i + 1][0]
            d_time = stop_sequence[i + 1][1]

            # Reduce costs of possibly used links between O and D.
            self.network.reset_costs()
            self.network.graph.cost[
                self.trip.graph_links_time[self.trip.graph_links_time["timestamp"].between(o_time, d_time)][
                    "graph_links"
                ].unique()
            ] /= self.parameters["map matching"]["cost discount"]

            if origin != destination:
                arrives_tstamps = {}
                leaves_tstamps = {}
                path_computation(origin, destination, self.network.graph, results)
                if results.path is not None:
                    # Now we build a spatial index with the subset of pings in the truck trace between the origin and destination
                    ping_subset = vehicle_trace[
                        (vehicle_trace.timestamp > stop_sequence[i][1] + timedelta(seconds=int(stop_sequence[i][2])))
                        & (vehicle_trace.timestamp < stop_sequence[i + 1][1])
                    ]

                    temp_idx = index.Index()
                    for q in list(results.path_nodes):
                        x = self.network.nodes.X[q]
                        y = self.network.nodes.Y[q]
                        bbox = (x, y, x, y)
                        temp_idx.insert(q, bbox)

                    for k in ping_subset.index:
                        x = vehicle_trace.longitude[k]
                        y = vehicle_trace.latitude[k]
                        near = list(temp_idx.nearest((x, y, x, y), 1))
                        if len(near) > 0:
                            q = near[0]
                            x2 = self.network.nodes.X[q]
                            y2 = self.network.nodes.Y[q]
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
                    leaves_tstamps[destination] = stop_sequence[i + 1][1] + timedelta(
                        seconds=int(stop_sequence[i + 1][2])
                    )

                    self.InterpolateTS(arrives_tstamps, leaves_tstamps, results.path_nodes, results.path.copy())

                    # For completeness
                    if 0 < stop_sequence[i + 1][2] < 100000:
                        leaves_tstamps[destination] = arrives_tstamps[destination] + timedelta(
                            seconds=int(stop_sequence[i + 1][2])
                        )
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
        if all_links:
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
                direc = self.network.graph.graph["direction"][all_links[i]]
                df.append(
                    [
                        all_arrives_tstamps[i],
                        all_leaves_tstamps[i],
                        self.network.nodes.index[q],
                        link,
                        direc,
                        self.network.nodes.Y[q],
                        self.network.nodes.X[q],
                        mp,
                    ]
                )
                if all_links[i] > 0:
                    mp = mp + self.network.interpolation_cost[all_links[i]]
        else:
            df = [[-1, -1, -1, -1, -1, -1, -1, -1]]
        df_head = ["Timestamp", "leavenode", "node", "link", "direction", "y", "x", "milepost"]
        df = pd.DataFrame(df, columns=df_head)
        self.trip.path = df
        # We reset the graph costs
        self.network.graph.cost = self.network.orig_cost.copy()

    def InterpolateTS(self, arrives_tstamps, leaves_tstamps, pathnodes, all_links):
        consistent = False
        while not consistent:
            consistent = True
            for i in range(len(pathnodes) - 1):
                o = pathnodes[i]
                if o in leaves_tstamps.keys():
                    for j in range(i + 1, len(pathnodes)):
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
        while i < len(pathnodes) - 2:
            j = i + 1
            mp = self.network.interpolation_cost[all_links[j - 1]]
            while pathnodes[j] not in arrives_tstamps.keys():
                mp += self.network.interpolation_cost[all_links[j]]
                j += 1

            if j > i + 1:  # Means we have at least one node in the path that does not have any timestamp written to it
                time_diff = (arrives_tstamps[pathnodes[j]] - leaves_tstamps[pathnodes[i]]).total_seconds()
                if time_diff < 0:
                    del arrives_tstamps
                    del leaves_tstamps
                    break
                mp2 = 0
                for k in range(i + 1, j):
                    mp2 += self.network.interpolation_cost[all_links[k - 1]]
                    j_time = leaves_tstamps[pathnodes[i]] + timedelta(seconds=time_diff * mp2 / mp)
                    arrives_tstamps[pathnodes[k]] = j_time
                    leaves_tstamps[pathnodes[k]] = j_time
            i = j

    @staticmethod
    def reverse_azim(azim):
        if azim > 180:
            return azim - 180
        else:
            return azim + 180

    @staticmethod
    def check_if_inside(azimuth, polygon_azimuth, tolerance):
        inside = False

        # If checking the tolerance interval will make the angle bleed the [0,360] interval, we have to fix it

        # In case the angle is too big

        if polygon_azimuth + tolerance > 360:
            if polygon_azimuth - tolerance > azimuth:
                azimuth += 360

        # In case the angle is too small
        if polygon_azimuth - tolerance < 0:
            polygon_azimuth += 360
            if azimuth < 180:
                azimuth += 360

        if polygon_azimuth - tolerance <= azimuth <= polygon_azimuth + tolerance:
            inside = True

        # Several data points do NOT have an azimuth associated, so we consider the possibility that all the links are valid
        if azimuth == 0:
            inside = True

        return inside

    # Great circle distance function
    @staticmethod
    def gc(a, b, c, d):
        p1 = (b, a)
        p2 = (d, c)
        return great_circle(p1, p2).kilometers
