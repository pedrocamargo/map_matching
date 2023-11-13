from os.path import join
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from aequilibrae import AequilibraeMatrix, Graph
from geopandas import GeoDataFrame as gdf


# TODO: REMOVE FROM REPO
class GraphBuilder:
    def __init__(self):
        self.graph = Graph()
        self.skim = AequilibraeMatrix()

    def from_layers(self, network: gdf, network_nodes: gdf, cache_path: Optional[Path] = None):
        cols = ["edge_id", "start_node_id", "end_node_id", "oneway", "edge_length_km", "geometry"]
        self.network = gpd.GeoDataFrame(network[cols], geometry="geometry", crs=network.crs).to_crs(7842)

        cols = ["node_id", "geometry"]
        self.network_nodes = gpd.GeoDataFrame(network_nodes[cols], geometry="geometry", crs=network_nodes.crs).to_crs(
            7842
        )

        self.__clean_nodes()
        self.__build_graph(cache_path)

    def from_cache(self, cache_path: Path):
        file_path = join(cache_path, "graph_cache.h5")
        network = pd.read_hdf(file_path, key="network")
        self.__zone_info = pd.read_hdf(file_path, key="intrazonals")
        self.__prepare_graph(network)

    def __clean_nodes(self):
        all_nodes = np.hstack([self.network.start_node_id.values, self.network.end_node_id.values])
        self.network_nodes = self.network_nodes[self.network_nodes.node_id.isin(all_nodes)]

    def __build_graph(self, cache_path: Optional[Path]):
        cols = ["edge_id", "start_node_id", "end_node_id", "oneway", "edge_length_km"]
        network = pd.DataFrame(self.network[cols])
        network.rename(
            columns={
                "edge_id": "link_id",
                "start_node_id": "a_node",
                "end_node_id": "b_node",
                "edge_length_km": "distance",
            },
            inplace=True,
        )
        network = network.assign(direction=0)
        network.loc[network["oneway"], "direction"] = 1
        network.drop(columns=["oneway"], inplace=True)

        if cache_path is not None:
            file_path = join(cache_path, "graph_cache.h5")
            network.to_hdf(file_path, key="network", mode="w")

        self.__prepare_graph(network)

    def __prepare_graph(self, network):
        self.graph.network_ok = True
        self.graph.status = "OK"
        self.graph.network = network
        self.graph.prepare_graph(np.array([network.a_node.values[0]], dtype=np.int64))
        self.graph.set_graph("distance")
        self.graph.set_skimming(["distance"])
        self.graph.set_blocked_centroid_flows(False)
