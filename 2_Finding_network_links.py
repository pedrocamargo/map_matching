# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Pedro Camargo
#
# Created:     06/05/2015
# Copyright:   (c) pcamargo 2015
# Licence:     <your licence>
# -------------------------------------------------------------------------------

import ntpath
import os, sys
import glob
import pandas as pd
import shapefile  # pip install pyshp
from shapely.geometry import LineString, Point  # Pip install shapely
from rtree import index  # Wheel from Chris Gohlke's  website
import warnings
warnings.filterwarnings("ignore")
from time import clock
from operator import itemgetter
from parameters import load_parameter

sys.path.append("C:/Users/pedro.camargo.VEITCHLISTER/.qgis2/python/plugins/AequilibraE/")
from aequilibrae.paths import Graph

# Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/


def main():
    vehicle_id = load_parameter('fields', 'vehicle id')
    speed = load_parameter('fields', 'speed')
    latitude = load_parameter('fields', 'latitude')
    longitude = load_parameter('fields', 'longitude')
    azimuth = load_parameter('fields', 'azimuth')
    tolerance = load_parameter('geoprocessing', 'azimuth tolerance')

    shape_folder = load_parameter('folders', 'layer folder')
    net_file = load_parameter('file names', 'network file')
    buffer_size = load_parameter('geoprocessing', 'buffer size')
    network_parameters =  load_parameter('geoprocessing', 'network file fields')
    links_per_route_folder =  load_parameter('folders', 'links per route folder')
    data_folder = load_parameter('folders', 'hdf5 folder')
    aequilibrae_graph = load_parameter('file names', 'aequilibrae graph')

    res_buffer = 5
    # Load the shapefile and build network buffers
    print 'Reading shapefile'
    network = shapefile.Reader(shape_folder + net_file)
    NetRecs = network.shapeRecords()

    def find_field_index(fields, field_name):
        for i, f in enumerate(fields):
            if f[0] == field_name:
                return i -1
        return -1
    azim = find_field_index(network.fields, network_parameters['azimuth'])
    linkID = find_field_index(network.fields, network_parameters['id'])
    di = find_field_index(network.fields, network_parameters['direction'])

    # Build a spatial index based on the bounding boxes of the buffers
    print "Assembling spatial index and saving buffers"
    idx = index.Index()
    azims = []
    dirs = []
    ids = []
    graph_ids = []
    buffers = {}

    w = shapefile.Writer(shapefile.POLYGON)
    w.field('FIRST_FLD', 'C', '40')
    for q in NetRecs:
        b = LineString(q.shape.points).buffer(buffer_size, resolution=res_buffer)
        x, y = b.exterior.coords.xy
        w.poly(parts=[[[x, y] for x, y in zip(x, y)]])
        w.record('analysis buffer')

        idx.insert(q.record[linkID], list(b.bounds))
        ids.append(q.record[linkID])
        buffers[q.record[linkID]] = b
        if azim != -1:
            azims.append(q.record[azim])
        else:
            azims.append(-1)
        dirs.append(q.record[di])
        graph_ids.append(-1)

    w.save(shape_folder + 'BUFFERS_USED_IN_ANALYSIS.SHP')
    del w

    graph = Graph()
    graph.create_from_geography(shape_folder + net_file, network_parameters['id'], network_parameters['direction'],
                            network_parameters['cost'], [network_parameters['interpolation']])
    graph.save_to_disk(shape_folder + aequilibrae_graph)

    # Builds the dataframe
    d = {'ID': ids,
         'azim': azims,
         'dir': dirs,
         'graph_ab': graph_ids,
         'graph_ba': graph_ids}
    D = pd.DataFrame(d)
    D = D.set_index('ID')
    del d

    # Brings the graph info for the Dataframe
    for i in range(graph.num_links):
        link_id = graph.graph['link_id'][i]
        direc = graph.graph['direction'][i]
        graph_id = graph.graph['id'][i]
        if direc == -1:
            D.at[link_id, 'graph_ba'] = graph_id
        else:
            D.at[link_id, 'graph_ab'] = graph_id

    # Starts searching for " used links"  for each GPS ping
    for ProcessingFile in glob.glob(data_folder + "*.h5"):
        head, ProcessingFile = ntpath.split(ProcessingFile)
        print 'Loading GPS pings:', ProcessingFile
        retrieve = pd.HDFStore(data_folder + ProcessingFile)
        gps_data = retrieve['gps_data']
        gps_data.fillna(-1, inplace=True)
        retrieve.close()

        # Collect all truck IDs
        vehicle_ids = gps_data[vehicle_id].unique()

        # for all ti in vehicle_ids()
        tot_vehs = len(vehicle_ids)

        print 'Starting analysis'
        store = pd.HDFStore(links_per_route_folder + ProcessingFile[0:-3]+'_links.h5')

        Tim = clock()
        veh_azimuth = -1
        for h, ti in enumerate(vehicle_ids):
            if h % 100 == 0:
                print 'Analyzing vehicles:', round((100.0*(h+1))/tot_vehs, 2), '%% (', round(clock() - Tim, 0), 's)'

            veh_trace = gps_data[gps_data[vehicle_id] == ti]
            poly = []
            for g, t in enumerate(veh_trace.index):

                # Collects all info on a ping
                if speed != -1:
                    veh_speed = veh_trace.at[t, speed]
                if azimuth != -1:
                    veh_azimuth = veh_trace.at[t, azimuth]

                y = veh_trace.at[t, latitude]
                x = veh_trace.at[t, longitude]
                P = (x, y)

                # Finds which links are likely to have been used
                l = idx.intersection(P)
                P = Point(x,y)

                # Loops through them to make sure they are within the buffers
                for j in l:
                    azim = D.azim[j]
                    direc = D.dir[j]
                    graph_id = D.graph_ab[j]
                    if direc < 0:
                        graph_id = D.graph_ba[j]

                    if graph_id not in poly:
                        if -1 in [int(azim), int(veh_azimuth)]:
                            if P.within(buffers[j]):
                                poly.append(graph_id)
                                if direc == 0:
                                    poly.append(D.graph_ba[j])
                        else:
                            if P.within(buffers[j]):
                                if direc < 0:
                                    azim = reverse_azim(azim)

                                if check_if_inside(veh_azimuth, azim, tolerance):
                                    poly.append(graph_id)

                                if direc == 0:
                                    azim = reverse_azim(azim)
                                    if check_if_inside(veh_azimuth, azim, Param.tolerance):
                                        poly.append(D.graph_ba[j])

            if len(poly) > 0:
                store['/' + str(ti)] = pd.Series(poly)
        store.close()

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
