from typing import Optional, List, Union

from os import PathLike
from .parameters import Parameters
from network import Network
from trip import Trip
from aequilibrae.paths import Graph
import pandas as pd
import numpy as np
import geopandas as gpd

from geopy.distance import great_circle
from shapely.geometry import LineString, Point

from datetime import timedelta

from aequilibrae.paths import PathResults, path_computation
from geopy.distance import vincenty as gc


class MapMatcher:
    def __init__(self):
        self.network: Network()
        self.trips = []  # type: List[Trip]
        self.output_folder = None
        self.parameters = Parameters()

    def set_output_folder(self, output_folder: str):
        # Name of the output folder
        self.output_folder = output_folder

    def set_stop_algorithm(self, stop_algorithm):
        if stop_algorithm not in self.parameters.algorithm_parameters:
            raise ValueError(f"Unknown Stop algorithm: {stop_algorithm}")
        self.parameters.stop_algorithm = stop_algorithm

    def load_network(self, graph: Graph, links: gpd.GeoDataFrame, nodes: Optional[gpd.GeoDataFrame] = None):
        self.network = Network(graph=graph, links=links, nodes=nodes)

    def load_gps_pings(self, gps_traces: Union[gpd.GeoDataFrame, PathLike]):
        """Coordinate system for GPS pings must ALWAYS be 4326"""

        if isinstance(gps_traces, pd.GeoDataFrame):
            traces = gps_traces.to_crs(4326)
        else:
            traces = pd.read_csv(gps_traces)
            traces = gpd.GeoDataFrame(
                traces, geometry=gpd.points_from_xy(traces.longitude, traces.latitude), crs="EPSG:4326"
            )

    def load_stops(self, stops: Union[gpd.GeoDataFrame, PathLike], data_dictionary: Optional[dict] = None):
        if isinstance(stops, pd.GeoDataFrame):
            stops = stops.to_crs(4326)
        else:
            stops = pd.read_csv(stops)
            stops = gpd.GeoDataFrame(
                stops, geometry=gpd.points_from_xy(stops.longitude, stops.latitude), crs="EPSG:4326"
            )

    def execute(self):
        self.find_stops()
        if self.trip.stops is not None:
            self.find_network_links()
            self.find_route()
        else:
            raise ValueError("No trip stops to compute paths from")

    @staticmethod
    def reverse_azim(azim):
        if azim > 180:
            return azim - 180
        else:
            return azim + 180

    @staticmethod
    def check_if_inside(azimuth, polygon_azimuth, tolerance):
        inside = False

        # If checking the tolerance interval will make the angle bleed the [0,360] interval, we have to fix it

        # In case the angle is too big

        if polygon_azimuth + tolerance > 360:
            if polygon_azimuth - tolerance > azimuth:
                azimuth += 360

        # In case the angle is too small
        if polygon_azimuth - tolerance < 0:
            polygon_azimuth += 360
            if azimuth < 180:
                azimuth += 360

        if polygon_azimuth - tolerance <= azimuth <= polygon_azimuth + tolerance:
            inside = True

        # Several data points do NOT have an azimuth associated, so we consider the possibility that all the links are valid
        if azimuth == 0:
            inside = True

        return inside

    # Great circle distance function
    @staticmethod
    def gc(a, b, c, d):
        p1 = (b, a)
        p2 = (d, c)
        return great_circle(p1, p2).kilometers
