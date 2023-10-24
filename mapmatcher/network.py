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
        self._speed_field = ""

    @property
    def has_speed(self) -> bool:
        return len(self._speed_field) > 0

    def set_speed_field(self, speed_field: str):
        if speed_field not in self.links:
            raise ValueError("Speed field NOT in the links table")
        self._speed_field = speed_field
