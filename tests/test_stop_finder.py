from pathlib import Path
from math import ceil

import geopandas as gpd
import pandas as pd
import pytest

from mapmatcher.parameters import MaximumSpace, Parameters
from mapmatcher.stop_finder import stops_maximum_space
from mapmatcher.trip import Trip


def test_compute_stop():
    df = pd.read_csv(Path(__file__).parent / "data" / "traces.csv")

    df.timestamp = pd.to_datetime(df.timestamp, unit="s")
    df.rename(columns={"x": "longitude", "y": "latitude"}, inplace=True)
    df = df[df.trace_id == 12]
    geometry = gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    trace = gpd.GeoDataFrame(df, geometry=geometry)

    par = Parameters()
    algo_parameters = par.algorithm_parameters["maximum_space"]  # type: MaximumSpace
    trp = Trip(gps_trace=trace, parameters=par, network=None)

    stops = stops_maximum_space(trp.trace, algo_parameters)

    assert stops.shape[0] == 3
    for x, y in zip(stops.geometry[:-1], stops.geometry[1:]):
        x.distance(y) < algo_parameters.max_avg_distance
        # This is a trace that violates distance only, so we make sure that it is correct
        x.distance(y) > algo_parameters.max_avg_distance / stops.shape[0]

    for x, y in zip(stops.timestamp[:-1], stops.timestamp[1:]):
        abs(x - y).seconds < algo_parameters.max_avg_time

    # Let's explode te average distance
    algo_parameters.max_avg_distance = 500000
    stops = stops_maximum_space(trp.trace, algo_parameters)
    assert stops.shape[0] == 2

    # And reduce the travel time

    algo_parameters.max_avg_time = ceil((trp.trace.ping_posix_time.max() - trp.trace.ping_posix_time.min()) - 5)
    stops = stops_maximum_space(trp.trace, algo_parameters)
    assert stops.shape[0] == 3
