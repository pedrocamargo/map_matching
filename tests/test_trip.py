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


@pytest.fixture
def param() -> Parameters:
    par = Parameters()
    par.data_quality.maximum_jittery = 20000
    par.data_quality.max_speed = 41
    return par


def test_trip(gps_trace, param):
    trp = Trip(gps_trace=gps_trace, parameters=param, network=None)
    assert trp.has_heading
    assert not trp.has_error


def test_fail_on_jitter(gps_trace, param):
    param.data_quality.maximum_jittery = 0.01
    trp = Trip(gps_trace=gps_trace, parameters=param, network=None)
    assert trp.has_error
    assert "jitter" in trp._error_type


def test_fail_on_speed_time(gps_trace, param):
    param.data_quality.max_speed_time = 0
    trp = Trip(gps_trace=gps_trace, parameters=param, network=None)
    assert trp.has_error
    assert "surpassed" in trp._error_type


def test_fail_on_speed_time2(gps_trace, param):
    param.data_quality.max_speed = 120 / 3.6
    trp = Trip(gps_trace=gps_trace, parameters=param, network=None)
    assert trp.has_error
    assert "surpassed" in trp._error_type
