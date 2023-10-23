import os
from shutil import copyfile
from typing import Optional

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from aequilibrae import Graph
from linebearing import compute_line_bearing
from rtree import index  # Wheel from Chris Gohlke's  website
from shapely.geometry import shape
from shapely.ops import cascaded_union

from mapmatcher.parameters import Parameters


class Network:
    def __init__(
        self, graph: Graph, links: gpd.GeoDataFrame, parameters: Parameters, nodes: Optional[gpd.GeoDataFrame] = None
    ):
        # Creates the properties for the outputs
        self.links = links
        self.nodes = nodes
        self.graph = graph
        self.azimuth_tolerance = None
        self.buffers = {}
        self.links_df = None
        self.network_fields = None
        self.orig_cost = None
        self.interpolation_cost = None
        self.output_folder = None

        # Fields necessary for running the algorithm
        self.mandatory_fields = ["trip_id", "ping_id", "latitude", "longitude", "timestamp"]
        self.optional_fields = ["azimuth", "speed"]
        self.all_fields = self.mandatory_fields + self.optional_fields

        # Indicators to show if we have the optional fields in the data
        self.has_speed = False
        self.has_heading = False

        pgeo = parameters.geoprocessing
        buffers = self.links.to_crs(3857).buffer(distance=pgeo.buffer_size, resolution=pgeo.buffer_resolution)
        self.buffers = buffers.to_crs(4326)
