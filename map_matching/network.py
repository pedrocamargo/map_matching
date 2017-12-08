import numpy as np
import pandas as pd
import shapefile  # pip install pyshp
from rtree import index  # Wheel from Chris Gohlke's  website
import sys
import fiona
from shapely.geometry import shape
import os
from aequilibrae import Graph
from shapely.ops import cascaded_union
from linebearing import compute_line_bearing
from shutil import copyfile

# writing to SQLITE comes largely from http://gis.stackexchange.com/questions/141818/insert-geopandas-geodataframe-into-spatialite-database
class Network:
    def __init__(self, output_folder):

        # Creates the properties for the outputs
        self.links = None
        self.nodes = None
        self.graph = Graph()
        self.buffer_resolution = 5
        self.buffer_size = None
        self.azimuth_tolerance = None
        self.buffers = {}
        self.links_df = None
        self.network_fields = None
        self.orig_cost = None
        self.interpolation_cost = None

        # Fields necessary for running the algorithm
        self.mandatory_fields = ["trip_id", "ping_id", "latitude", "longitude", "timestamp"]
        self.optional_fields = ["azimuth", "speed"]
        self.all_fields = self.mandatory_fields + self.optional_fields

        # Name of the output folder
        self.output_folder = output_folder

        # Creates the dataframe for the GPS trace
        self.idx_links = index.Index()
        self.idx_nodes = index.Index()

        # Indicators to show if we have the optional fields in the data
        self.has_speed = False
        self.has_azimuth = False

    def set_geometry_parameters(self, parameters):
        self.buffer_size = parameters['buffer size']
        self.azimuth_tolerance = parameters['azimuth tolerance']

    def load_network(self, network_file, network_fields):
        self.network_fields = network_fields
    # First we load the graph itself
        print 'Creating graph from shapefile'
        link_id = network_fields['link_id']
        direction = network_fields['direction']
        cost_field = network_fields['cost']
        skims=[]
        if network_fields['interpolation'] != cost_field:
            skims.append(network_fields['interpolation'])

        self.graph.create_from_geography(network_file, link_id, direction, cost_field, skims)
        self.graph.cost = self.graph.graph[cost_field].astype(np.float64)
        self.orig_cost = np.copy(self.graph.cost)
        if network_fields['interpolation'] == cost_field:
            self.interpolation_cost = self.orig_cost
        else:
            self.interpolation_cost = self.graph.skims[:, 0]

        self.graph.set_graph(cost_field=cost_field, skim_fields=[network_fields['interpolation']])
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        self.graph.save_to_disk(os.path.join(self.output_folder, 'GRAPH USED IN ANALYSIS.aeg'))

        self.create_buffers(network_file, network_fields)

    def create_buffers(self, network_file, network_fields):
        link_id = network_fields['link_id']
        direction = network_fields['direction']

    # Now we load the layer as geographic objects for the actual analysis
        source = fiona.open(network_file, 'r')
        source_schema = source.schema.copy() # Copy the source schema
        source_crs = source.crs['init'][5:]

        projection_file = network_file[:-3] + 'prj'
        w = shapefile.Writer(shapefile.POLYGON)
        w.field(link_id, 'I', '40')
        azims = []
        dirs = []
        ids = []

        print 'Creating network spatial index'
        for feature in source:
            geom = shape(feature['geometry'])
            i_d = int(feature["properties"][link_id])
            direc = int(feature["properties"][direction])
            link_buffer = cascaded_union(geom.buffer(self.buffer_size, resolution=self.buffer_resolution))
            self.idx_links.insert(i_d, link_buffer.bounds)

            if network_fields['azimuth']:
                if network_fields['azimuth'] == "AUTO":
                    azim = compute_line_bearing(feature['geometry']['coordinates'][0], feature['geometry']['coordinates'][-1])
                else:
                    azim = feature["properties"][network_fields['azimuth']]
            else:
                azim = -1

            ids.append(i_d)
            dirs.append(direc)
            azims.append(azim)
            self.buffers[i_d] = link_buffer
            try:
                x, y = link_buffer.exterior.coords.xy
                w.poly(parts=[[[x, y] for x, y in zip(x, y)]])
                w.record(i_d)
            except:
                print '     ','Link', i_d, 'could not have its buffer computed'


            # Builds the dataframe
        d = {'ID': ids,
             'azim': azims,
             'dir': dirs}
        D = pd.DataFrame(d)
        D = D.set_index('ID')
        D['graph_ab'] = -1
        D['graph_ba'] = -1
        del d

        buffer_name = 'BUFFERS_USED_IN_ANALYSIS'
        if os.path.isfile(projection_file):
            copyfile(projection_file, os.path.join(self.output_folder, buffer_name + '.prj'))
        w.save(os.path.join(self.output_folder, buffer_name + '.SHP'))
        del w

        # Brings the graph info for the Dataframe
        for i in range(self.graph.num_links):
            link_id = self.graph.graph['link_id'][i]
            direc = self.graph.graph['direction'][i]
            graph_id = self.graph.graph['id'][i]
            if direc == -1:
                D.at[link_id, 'graph_ba'] = graph_id
            else:
                D.at[link_id, 'graph_ab'] = graph_id


        self.links_df = D
        print '     LINKS spatial index created successfully'

    def reset_costs(self):
        cost_field = self.network_fields['cost']
        self.graph.cost = self.graph.graph[cost_field].astype(np.float64)

    def load_nodes(self, nodes_file, id_field):

        # Now we load the layer as geographic objects for the actual analysis
        source = fiona.open(nodes_file, 'r')

        azims = []
        dirs = []
        ids = []
        self.nodes = []
        print 'Creating nodes spatial index'
        for feature in source:
            geom = shape(feature['geometry'])
            i_d = int(feature["properties"][id_field])
            self.idx_nodes.insert(i_d, geom.bounds)
            x, y = geom.centroid.coords.xy
            self.nodes.append([i_d, float(x[0]), float(y[0])])
        self.nodes = pd.DataFrame(self.nodes, columns = ['ID', 'X', 'Y']).set_index(['ID'])
        print '     NODES spatial index created successfully'

