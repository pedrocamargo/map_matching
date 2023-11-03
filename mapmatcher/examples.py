from pathlib import Path

import geopandas as gpd
import pandas as pd


def nauru_data() -> gpd.GeoDataFrame:
    df = pd.read_csv(Path(__file__).parent / "example_data" / "traces_nauru.csv")
    df.timestamp = pd.to_datetime(df.timestamp, unit="s")
    geometry = gpd.points_from_xy(df.x, df.y, crs="EPSG:4326")
    return gpd.GeoDataFrame(df, geometry=geometry)
