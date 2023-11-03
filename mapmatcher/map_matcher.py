from os import PathLike
from typing import List, Optional, Union

import geopandas as gpd
import pandas as pd
from aequilibrae.paths import Graph
from .network import Network
from .trip import Trip

from .parameters import Parameters


class MapMatcher:
    __mandatory_fields = ["trace_id", "ping_id", "latitude", "longitude", "timestamp"]

    def __init__(self):
        self.__orig_crs = 4326
        self.network: Network() = None
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
        self.network = Network(graph=graph, links=links, nodes=nodes, parameters=self.parameters)

    def load_gps_traces(self, gps_traces: Union[gpd.GeoDataFrame, PathLike], crs: Optional[int] = None):
        f"""Coordinate system for GPS pings must ALWAYS be 4326 when loading from CSV.
        Required fields are:  {self.__mandatory_fields}"""

        if isinstance(gps_traces, gpd.GeoDataFrame):
            self.__orig_crs = gps_traces.crs
            traces = gps_traces
        else:
            traces = pd.read_csv(gps_traces)
            traces = gpd.GeoDataFrame(
                traces, geometry=gpd.points_from_xy(traces.longitude, traces.latitude), crs=f"EPSG:{crs}"
            )

        for fld in self.__mandatory_fields:
            if fld not in traces:
                raise ValueError(f"Field {fld} is mising from the data")

        self.__traces = traces.to_crs(self.parameters.geoprocessing.projected_crs)

    def load_stops(self, stops: Union[gpd.GeoDataFrame, PathLike]):
        if isinstance(stops, gpd.GeoDataFrame):
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
            gdf = gdf.sort_values(by=["timestamp"]).reset_index()
            stops = None if self.__exogeous_stops else self.__stops[self.__stops.trace_id == trace_id]
            self.trips.append(Trip(self.parameters, gps_trace=gdf, stops=stops))

    def execute(self):
        self.network._orig_crs = self.__orig_crs
        for trip in self.trips:  # type: Trip
            trip.map_match()
