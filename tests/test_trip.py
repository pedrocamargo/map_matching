from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from mapmatcher.parameters import Parameters
from mapmatcher.trip import Trip


@pytest.fixture
def gps_trace() -> gpd.GeoDataFrame:
    df = pd.read_csv(Path(__file__).parent / "data" / "trace_all_fields.csv")
    df.timestamp = pd.to_datetime(df.timestamp)
    geometry = gpd.points_from_xy(df.x, df.y, crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry)


def test_trip(gps_trace):
    trp = Trip(gps_trace=gps_trace, parameters=Parameters())
    assert trp.has_heading
    assert trp.has_speed
    print(trp.trace.dist.max())
    print(trp.trace.traveled_time.max())

