from typing import List

import pandas as pd
from shapely.geometry import Point

from mapmatcher.network import Network
from mapmatcher.parameters import Parameters
from mapmatcher.trip import Trip


def find_network_links(list_trips: List(Trip), parameters: Parameters, network: Network):
    veh_speed = -1
    veh_heading = -1
    poly = []
    poly_time = []  # An array of [link graph id, timestamp the link is possibly used by a ping]
    all_links = []
    for g, trip in enumerate(list_trips):
        # Collects all info on a ping
        if trip.has_speed:
            veh_speed = trip.gps_trace.at[trip, "speed"]
        if trip.has_heading:
            veh_heading = trip.gps_trace.at[trip, "heading"]

        y = trip.gps_trace.at[trip, "latitude"]
        x = trip.gps_trace.at[trip, "longitude"]
        timestamp = trip.gps_trace.at[trip, "timestamp"]
        P = (x, y)

        # Finds which links are likely to have been used
        l = network.idx_links.intersection(P)
        P = Point(x, y)

        # Loops through them to make sure they are within the buffers
        for j in l:
            direc = network.links_df.dir[j]
            graph_id = network.links_df.graph_ab[j]
            if direc < 0:
                graph_id = network.links_df.graph_ba[j]

            if P.within(network.buffers[j]):
                if trip.has_heading:
                    azim = network.links_df.azim[j]
                    if direc < 0:
                        azim = self.reverse_azim(azim)

                    if self.check_if_inside(veh_heading, azim, network.heading_tolerance):
                        poly.append(graph_id)
                        poly_time.append([graph_id, timestamp])
                        all_links.append(int(j))
                    if direc == 0:
                        azim = self.reverse_azim(azim)
                        if self.check_if_inside(veh_heading, azim, network.heading_tolerance):
                            poly.append(network.links_df.graph_ba[j])
                            poly_time.append([network.links_df.graph_ba[j], timestamp])
                else:
                    poly.append(graph_id)
                    poly_time.append([graph_id, timestamp])
                    all_links.append(int(j))
                    if direc == 0:
                        poly.append(network.links_df.graph_ba[j])
                        poly_time.append([network.links_df.graph_ba[j], timestamp])

    trip.graph_links = poly
    trip.graph_links_time = pd.DataFrame(poly_time, columns=["graph_links", "timestamp"])
    trip.used_links = all_links
