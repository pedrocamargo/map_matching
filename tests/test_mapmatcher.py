from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from mapmatcher.parameters import Parameters
from mapmatcher.map_matcher import MapMatcher


@pytest.fixture
def gps_traces() -> gpd.GeoDataFrame:
    df = pd.read_csv(Path(__file__).parent / "data" / "traces.csv")
    df.timestamp = pd.to_datetime(df.timestamp)
    df.rename(columns={"x": "longitude", "y": "latitude"}, inplace=True)
    geometry = gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry)


def test_mapmatcher(gps_traces):
    mm = MapMatcher()
    mm.load_gps_traces(gps_traces)
