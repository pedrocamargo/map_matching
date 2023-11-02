import uuid
from os.path import join
from pathlib import Path
from tempfile import gettempdir

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from aequilibrae.utils.create_example import create_example

from mapmatcher.parameters import Parameters
from mapmatcher.map_matcher import MapMatcher
from mapmatcher.network import Network


@pytest.fixture
def gps_traces() -> gpd.GeoDataFrame:
    df = pd.read_csv(Path(__file__).parent / "data" / "traces.csv")
    df.timestamp = pd.to_datetime(df.timestamp)
    df.rename(columns={"x": "longitude", "y": "latitude"}, inplace=True)
    geometry = gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry)


@pytest.fixture
def network() -> Network:
    proj = create_example(join(gettempdir(), uuid.uuid4().hex), "nauru")
    proj.network.build_graphs(modes=["c"])
    graph = proj.network.graphs["c"]
    graph.prepare_graph(np.array([1]))
    graph.set_graph("distance")
    link_sql = "SELECT link_id, Hex(ST_AsBinary(geometry)) as geometry FROM links;"
    nodes_sql = "SELECT node_id, Hex(ST_AsBinary(geometry)) as geometry FROM nodes;"
    links = gpd.GeoDataFrame.from_postgis(link_sql, proj.conn, geom_col="geometry", crs=4326)
    nodes = gpd.GeoDataFrame.from_postgis(nodes_sql, proj.conn, geom_col="geometry", crs=4326)

    return Network(graph=graph, links=links, nodes=nodes, parameters=Parameters())


def test_mapmatcher(gps_traces, network):
    mm = MapMatcher()
    mm.load_network(network.graph, network.links, network.nodes)
    mm.load_gps_traces(gps_traces)
