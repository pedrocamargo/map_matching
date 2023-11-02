import geopandas as gpd
import numpy as np
from aequilibrae import Graph

from mapmatcher.parameters import Parameters


class Network:
    def __init__(self, graph: Graph, links: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, parameters: Parameters):
        # Creates the properties for the outputs
        self.links = links if links.index.name == "link_id" else links.set_index("link_id", drop=True)
        self.nodes = nodes if nodes.index.name == "node_id" else nodes.set_index("node_id", drop=True)
        self.graph = graph
        self._speed_field = ""
        self._pars = parameters
        self._orig_crs = 4326

    @property
    def has_speed(self) -> bool:
        return len(self._speed_field) > 0

    def set_speed_field(self, speed_field: str):
        if speed_field not in self.links:
            raise ValueError("Speed field NOT in the links table")
        self._speed_field = speed_field

    def discount_graph(self, links: np.ndarray):
        self.graph.graph.loc[self.graph.graph.link_id.isin(links), "distance"] *= self._pars.map_matching.cost_discount
        self.graph.set_graph("distance")

    def reset_graph(self):
        self.graph.prepare_graph(self.graph.centroids)
        self.graph.set_graph("distance")
