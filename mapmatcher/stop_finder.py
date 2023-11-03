from math import ceil, floor

import geopandas as gpd
import numpy as np
import pandas as pd

from .parameters import MaximumSpace


def stops_maximum_space(trace: gpd.GeoDataFrame, parameters: MaximumSpace):
    intervals_time = floor((trace.ping_posix_time.max() - trace.ping_posix_time.min()) / parameters.max_avg_time)
    intervals_distance = floor(trace.trace_segment_dist.sum() / parameters.max_avg_distance)
    intervals = 1 + max(intervals_time, intervals_distance)

    interval_length = ceil(trace.trace_segment_dist.sum() / intervals)
    interval_finder = np.floor(trace.trace_segment_dist.cumsum() / interval_length)
    a = interval_finder - interval_finder.shift(1).fillna(0)
    interv_time = ceil((trace.ping_posix_time.max() - trace.ping_posix_time.min()) / intervals)
    interval_finder = np.floor((trace.ping_posix_time - trace.ping_posix_time.min()) / interv_time)
    b = interval_finder - interval_finder.shift(1).fillna(0)

    stop_indices = [
        floor(np.mean([np.arange(a.shape[0])[a > 0], np.arange(b.shape[0])[b > 0]])) for i in range(intervals - 1)
    ]
    ping_indices = [a.index.values[i] for i in stop_indices]
    ping_indices.extend([trace.index.values[0], trace.index.values[-1]])
    ping_indices = list(set(ping_indices))

    stops = trace.loc[ping_indices, :].sort_values(by=["timestamp"]).assign(stop_index=np.arange(len(ping_indices)) + 1)
    return gpd.GeoDataFrame(stops[["stop_index", "timestamp"]], geometry=stops.geometry, crs=stops.crs)


def delivery_stop(self):
    # If no error and if we actually need to find stops, we start the processing
    if self.trip.error is None and self.trip.stops_algorithm in ["Maximum space", "Delivery stop"]:
        self.trip.stops = []
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


# if self.trip.stops_algorithm == "Maximum space":
#     par_time = self.trip.stops_parameters["time"]
#     max_distance = self.trip.stops_parameters["max distance"]
#     min_distance = self.trip.stops_parameters["min distance"]
#     self.trip.gps_trace["delivery_stop"] = -1

#     cd = 0
#     ct = 0
#     # Append the first stop
#     self.trip.stops.append(
#         [
#             self.trip.gps_trace["latitude"].iloc[0],
#             self.trip.gps_trace["longitude"].iloc[0],
#             self.trip.gps_trace["timestamp"].iloc[0],
#             0.0,
#             0.0,
#         ]
#     )

#     for i in range(0, self.trip.gps_trace.index.shape[0] - 1):
#         cd += self.trip.gps_trace.distance[i]
#         ct += self.trip.gps_trace.traveled_time[i]
#         if ct > par_time and cd > min_distance or cd > max_distance:
#             cd = 0
#             ct = 0
#             self.trip.stops.append(
#                 [
#                     self.trip.gps_trace["latitude"].iloc[i],
#                     self.trip.gps_trace["longitude"].iloc[i],
#                     self.trip.gps_trace["timestamp"].iloc[i],
#                     0,
#                     0.0,
#                 ]
#             )

#     if self.trip.stops[-1][0:3] != [
#         self.trip.gps_trace["latitude"].iloc[-1],
#         self.trip.gps_trace["longitude"].iloc[-1],
#         self.trip.gps_trace["timestamp"].iloc[-1],
#     ]:
#         self.trip.stops.append(
#             [
#                 self.trip.gps_trace["latitude"].iloc[-1],
#                 self.trip.gps_trace["longitude"].iloc[-1],
#                 self.trip.gps_trace["timestamp"].iloc[-1],
#                 99999999,
#                 0.0,
#             ]
#         )
#     else:
#         self.trip.stops[-1][3] = 99999999
