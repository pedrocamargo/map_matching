import pandas as pd
from aequilibrae.paths import PathResults
import numpy as np


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
            results.compute_path(origin=origin, destination=destination)
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
                leaves_tstamps[destination] = stop_sequence[i + 1][1] + timedelta(seconds=int(stop_sequence[i + 1][2]))

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
