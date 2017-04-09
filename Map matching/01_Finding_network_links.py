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


# Somewhat based on http://rexdouglass.com/fast-spatial-joins-in-python-with-a-spatial-index/


def main():

    shape_folder = load_parameter('folders', 'layer_folder')
    net_file = load_parameter('file names', 'network file')
    buffer_size

    # Load the shapefile of polygons
    print 'Reading shapefile'
    network = shapefile.Reader(shape_folder + net_file)
    NetRecs = network.shapeRecords()
    buffers = [LineString(q.shape.points).buffer(Param.buffer_size, resolution=5) for q in NetRecs]

    # We save the buffers we created to disk just so we can appreciate in GIS software how big was the buffer
    w = shapefile.Writer(shapefile.POLYGON)
    w.field('FIRST_FLD', 'C', '40')
    for q in buffers:
        x, y = q.exterior.coords.xy
        w.poly(parts=[[[x, y] for x, y in zip(x, y)]])
        w.record('test')
    w.save(Param.shape_folder + 'BUFFERS_USED_IN_ANALYSIS.SHP')
    del w

    anode = -1
    bnode = -1
    azim = -1
    linkID = -1
    di = -1
    factype = -1
    length = -1
    for i, f in enumerate(network.fields):
        if f[0] == 'A_NODE':
            anode = i - 1
        elif f[0] == 'B_NODE':
            bnode = i - 1
        elif f[0]== 'AZIMUTH':
            azim = i - 1
        elif f[0]== 'ID':
            linkID = i - 1
        elif f[0]== 'DIR':
            di = i - 1
        elif f[0]== 'FACTYPE':
            factype = i - 1
        elif f[0]== 'LENGTH':
            length = i - 1


    # Build a spatial index based on the bounding boxes of the buffers


    # We also need to build the graph for the next step of the map-matching algorithm
    # In order to build the graph we need to number the links from 0 to M, with the graph
    # sorted by A_Node and B_Node
    # We also need to re-number the nodes with numbers ranging from 1 to N
    # This need for re-numbering is what causes the code to be much longer
    print "Assembling spatial index and building graph"
    idx = index.Index()
    counts = []
    azims = []
    dirs = []
    ids = []
    graph_ids = []
    graph_id = 0
    graph_list = []
    graph_nodes = {}
    current_node = 1
    nodes = open(Param.shape_folder + Param.node_list, 'w')
    print >> nodes, 'ID,Orig_ID,X,Y'
    for i, q in enumerate(NetRecs):
        idx.insert(i, list(LineString(q.shape.points).buffer(Param.buffer_size, resolution=5).bounds))
        counts.append(i)
        azims.append(q.record[azim])
        dirs.append(q.record[di])
        ids.append(q.record[linkID])
        graph_ids.append(graph_id)

        pen_length = q.record[length] * Param.link_penalties[q.record[factype]]

        # We re-number the nodes
        my_a_node = q.record[anode]
        if my_a_node not in graph_nodes:
            graph_nodes[my_a_node] = current_node
            my_a_node = current_node
            current_node += 1
            a_coord = q.shape.points[-1]
            text = str(my_a_node) + ',' + str(q.record[anode]) + ',' + str(a_coord[0]) + ',' + str(a_coord[1])
            print >> nodes, text
        else:
            my_a_node = graph_nodes[my_a_node]


        my_b_node = q.record[bnode]
        if my_b_node not in graph_nodes:
            graph_nodes[my_b_node] = current_node
            my_b_node = current_node
            current_node += 1
            b_coord = q.shape.points[-1]
            text = str(my_b_node) + ',' + str(q.record[bnode]) + ',' + str(b_coord[0]) + ',' + str(b_coord[1])
            print >> nodes, text
        else:
            my_b_node = graph_nodes[my_b_node]

        a = [graph_id, graph_id]
        # We build the graph that will be used for assignment
        if q.record[di] >= 0:
            graph_list.append([1, q.record[linkID], my_a_node, my_b_node, pen_length, q.record[length], graph_id])
            graph_id +=1

        if q.record[di] <= 0:
            graph_list.append([-1, q.record[linkID], my_b_node, my_a_node, pen_length, q.record[length], graph_id])
            graph_id +=1


    nodes.flush()
    nodes.close()

# Builds a dataframe for the shapefile fields
    d = {'ID': ids,
         'azim': azims,
         'dir': dirs,
         'graph_ab': graph_ids,
         'graph_ba': graph_ids}
    D = pd.DataFrame(d, index=counts)

    #Build the graph and correct the graph IDs in the dataframe
    graph = open(Param.shape_folder + Param.graph_file, 'w')
    sorted_graph = sorted(graph_list, key=itemgetter(2,3))
    print >> graph, 'ID,DIRECTION,LINK_ID,A_NODE,B_NODE,Length,real_length'
    graph_id = 0
    for l in sorted_graph:
        text = str(graph_id) + ',' + str(l[0]) + ',' + str(l[1]) + ',' + str(l[2]) + ',' + str(l[3]) + ',' + str(l[4]) + ',' + str(l[5])
        if l[0] == 1:
            D.set_value(D.ID == l[1], 'graph_ab', graph_id)
        else:
            D.set_value(D.ID == l[1], 'graph_ba', graph_id)

        print >> graph, text
        graph_id += 1

    graph.flush()
    graph.close()

    os.chdir(Param.data_folder)
    for ProcessingFile in glob.glob("*.h5"):
        if ProcessingFile not in ALREADY_RAN:
            print 'Loading GPS pings:', ProcessingFile
            retrieve = pd.HDFStore(Param.data_folder + ProcessingFile)
            atri = retrieve['atri']
            retrieve.close()

            print 'Loading unique truck IDs'
            retrieve = pd.HDFStore(Param.truck_stats_folder + ProcessingFile[0:-3] + '_truck_stats.h5')
            truck_stats = retrieve['truck_stats']
            retrieve.close()

            # Collect all truck IDs
            truck_ids = list(truck_stats.index.values)

            # for all ti in truck_ids()
            tot_trucks = len(truck_ids)

            print 'Starting analysis'
            store = pd.HDFStore(Param.links_per_route_folder+ProcessingFile[0:-3]+'_links.h5')
            Tim = clock()
            for h, ti in enumerate(truck_ids):
                if h % 100 == 0:
                    print 'Analyzing trucks:', round((100.0*h)/tot_trucks, 2), '%% (', round(clock() - Tim, 0), 's)'

                truck_trace = atri[atri.truckid==ti]
                poly = []

                for g, t in enumerate(truck_trace.index):
                    speed = truck_trace.speed[t]

                    y = truck_trace.y[t]
                    x = truck_trace.x[t]
                    azimuth = truck_trace.heading[t]
                    P = (x,y)

                    l = idx.intersection(P)
                    P = Point(x,y)

                    if isinstance(azimuth, basestring) and speed != 0:
                        azimuth = cardinals[azimuth]
                        azimuth = int(azimuth)

                    f = []
                    for j in l:
                        if j not in f:
                            f.append(j)
                            i_d = D.ID[j]
                            azim = float(D.azim[j])
                            direc = D.dir[j]
                            graph_id = D.graph_ab[j]
                            if direc < 0:
                                graph_id = D.graph_ba[j]

                            # For points with zero speed, we get all links whose buffers include the point
                            if speed == 0 or azimuth == -1000:
                                if graph_id not in poly:
                                    if P.within(buffers[j]):
                                        poly.append(graph_id)
                                        if direc == 0:
                                            poly.append(D.graph_ba[j])
                            else:
                                if direc < 0:
                                    azim = reverse_azim(azim)

                                if check_if_inside(azimuth, azim, Param.tolerance):
                                    # Verify that point is within the polygon itself not just the bounding box
                                    if graph_id not in poly:
                                        if P.within(buffers[j]):
                                            poly.append(graph_id)

                                if direc == 0:
                                    azim = reverse_azim(azim)
                                    if check_if_inside(azimuth, azim, Param.tolerance):
                                        # Verify that point is within the polygon itself not just the bounding box
                                        if D.graph_ba[j] not in poly:
                                            if P.within(buffers[j]):
                                                poly.append(D.graph_ba[j])
                # print new_id, poly
                if len(poly) > 0:
                    #print ti
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
