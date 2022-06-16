#-------------------------------------------------------------------------------
# Name:        Step 2 in map matching
# Purpose:     Finds the links likely corresponding to each GPS ping
#
# Author:      Pedro Camargo
#
# Created:     09/04/2017
# Copyright:   (c) pcamargo 2017
# Licence:     APACHE 2.0
#-------------------------------------------------------------------------------


from shapely.geometry import Point  # Pip install shapely


# Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/


def find_network_links(trip, network):

    veh_speed = -1
    veh_azimuth = -1
    poly = []
    all_links = []
    for g, t in enumerate(trip.gps_trace.index):
        # Collects all info on a ping
        if trip.has_speed:
            veh_speed = trip.gps_trace.at[t, 'speed']
        if trip.has_azimuth:
            veh_azimuth = trip.gps_trace.at[t, 'azimuth']

        y = trip.gps_trace.at[t, 'latitude']
        x = trip.gps_trace.at[t, 'longitude']
        P = (x, y)

        # Finds which links are likely to have been used
        l = network.idx_links.intersection(P)
        P = Point(x,y)

        # Loops through them to make sure they are within the buffers
        for j in l:
            direc = network.links_df.dir[j]
            graph_id = network.links_df.graph_ab[j]
            if direc < 0:
                graph_id = network.links_df.graph_ba[j]

            if graph_id not in poly:
                if P.within(network.buffers[j]):
                    if trip.has_azimuth:
                        azim = network.links_df.azim[j]
                        if direc < 0:
                            azim = reverse_azim(azim)

                        if check_if_inside(veh_azimuth, azim, network.azimuth_tolerance):
                            poly.append(graph_id)
                            all_links.append(int(j))
                        if direc == 0:
                            azim = reverse_azim(azim)
                            if check_if_inside(veh_azimuth, azim, network.azimuth_tolerance):
                                poly.append(network.links_df.graph_ba[j])

                    else:
                        poly.append(graph_id)
                        all_links.append(int(j))
                        if direc == 0:
                            poly.append(network.links_df.graph_ba[j])

    trip.graph_links = poly
    trip.used_links = all_links

def reverse_azim(azim):
    if azim > 180:
        return azim - 180
    else:
        return azim + 180

def check_if_inside(azimuth, polygon_azimuth, tolerance):
    inside=False

    # If checking the tolerance interval will make the angle bleed the [0,360] interval, we have to fix it

    #In case the angle is too big

    if polygon_azimuth + tolerance > 360:
        if polygon_azimuth - tolerance > azimuth:
            azimuth += 360

    #In case the angle is too small
    if polygon_azimuth-tolerance < 0:
        polygon_azimuth += 360
        if azimuth < 180:
            azimuth += 360

    if polygon_azimuth - tolerance <= azimuth <= polygon_azimuth + tolerance:
        inside = True

    # Several data points do NOT have an azimuth associated, so we consider the possibility that all the links are valid
    if azimuth == 0:
        inside = True

    return inside

if __name__ == '__main__':
     main()
