import uuid
from os.path import join
from pathlib import Path
from tempfile import gettempdir

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from aequilibrae.utils.create_example import create_example

from mapmatcher.network import Network
from mapmatcher.parameters import Parameters
from mapmatcher.trip import Trip


@pytest.fixture
def param() -> Parameters:
    par = Parameters()
    par.stop_algorithm = "maximum_space"
    return par


@pytest.fixture
def gps_trace(param) -> gpd.GeoDataFrame:
    df = pd.read_csv(Path(__file__).parent / "data" / "traces.csv")
    df.timestamp = pd.to_datetime(df.timestamp, unit="s")
    df.rename(columns={"x": "longitude", "y": "latitude"}, inplace=True)
    df = df[df.trace_id == 12]
    geometry = gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry).to_crs(param.geoprocessing.projected_crs)


@pytest.fixture
def network(param) -> Network:
    proj = create_example(join(gettempdir(), uuid.uuid4().hex), "nauru")
    proj.network.build_graphs(modes=["c"])
    graph = proj.network.graphs["c"]
    graph.prepare_graph(np.array([1]))
    graph.set_graph("distance")
    link_sql = "SELECT link_id, Hex(ST_AsBinary(geometry)) as geometry FROM links;"
    nodes_sql = "SELECT node_id, Hex(ST_AsBinary(geometry)) as geometry FROM nodes;"
    links = gpd.GeoDataFrame.from_postgis(link_sql, proj.conn, geom_col="geometry", crs=4326)
    nodes = gpd.GeoDataFrame.from_postgis(nodes_sql, proj.conn, geom_col="geometry", crs=4326)
    nodes = nodes.set_index(["node_id"])
    nodes = gpd.GeoDataFrame(nodes, geometry=nodes.geometry, crs=4326)

    return Network(graph=graph, links=links, nodes=nodes, parameters=param)


def test_map_match(gps_trace, network):
    trp = Trip(gps_trace=gps_trace, parameters=network._pars, network=network)
    trp.compute_stops()

    # We know we need at least three stops (extremities plus 1)
    len(trp._stop_nodes) == 3
    trp.map_match()
    assert gps_trace.distance(trp.path_shape).max() < 100
