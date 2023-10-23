from os import PathLike
from typing import Optional, Union, List

import geopandas as gpd
import pandas as pd
from aequilibrae.paths import Graph
from network import Network
from trip import Trip

from .parameters import Parameters


class MapMatcher:
    __mandatory_fields = ["trace_id", "ping_id", "latitude", "longitude", "timestamp"]

    def __init__(self):
        self.__orig_crs = 4326
        self.network: Network()
        self.trips: List[Trip] = []
        self.output_folder = None
        self.__exogeous_stops = False
        self.__traces: gpd.GeoDataFrame
        self.__stops: Optional[gpd.GeoDataFrame] = None
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

    def load_gps_traces(self, gps_traces: Union[gpd.GeoDataFrame, PathLike]):
        f"""Coordinate system for GPS pings must ALWAYS be 4326 when loading from CSV.
        Required fields are:  {self.__mandatory_fields}"""

        if isinstance(gps_traces, pd.GeoDataFrame):
            self.__orig_crs = traces.crs
            traces = gps_traces
        else:
            traces = pd.read_csv(gps_traces)
            traces = gpd.GeoDataFrame(
                traces, geometry=gpd.points_from_xy(traces.longitude, traces.latitude), crs="EPSG:4326"
            )

        for fld in self.__mandatory_fields:
            if fld not in traces:
                raise ValueError(f"Field {fld} is mising from the data")

        self.__traces = traces.to_crs(3857).sort_values(by=["trace_id", "timestamp"])

    def load_stops(self, stops: Union[gpd.GeoDataFrame, PathLike]):
        if isinstance(stops, pd.GeoDataFrame):
            self.__stops = stops.to_crs(4326)
        else:
            stops = pd.read_csv(stops)
            self.__stops = gpd.GeoDataFrame(
                stops, geometry=gpd.points_from_xy(stops.longitude, stops.latitude), crs="EPSG:4326"
            )
        self.__exogeous_stops = True

    def _build_trips(self):
        self.trips.clear()
        for trace_id, gdf in self.__traces.groupby(["trace_id"]):
            stops = None if self.__exogeous_stops else self.__stops[self.__stops.trace_id == trace_id]
            self.trips.append(Trip(self.parameters, gps_trace=gdf, stops=stops))

    def execute(self):
        self.find_stops()
        if self.trip.stops is not None:
            self.find_network_links()
            self.find_route()
        else:
            raise ValueError("No trip stops to compute paths from")

    @staticmethod
    def reverse_azim(azim):
        return azim - 180 if azim > 180 else azim + 180

    @staticmethod
    def check_if_inside(azimuth, polygon_azimuth, tolerance):
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
            return True

        # Several data points do NOT have an azimuth associated, so we consider the possibility that all the links are valid
        if azimuth == 0:
            return True

        return False
