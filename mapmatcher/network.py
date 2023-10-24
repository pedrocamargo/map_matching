import geopandas as gpd
from aequilibrae import Graph

from mapmatcher.parameters import Parameters


class Network:
    def __init__(self, graph: Graph, links: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame):
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
