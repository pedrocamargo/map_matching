import pandas as pd
from shapely.geometry import Point

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