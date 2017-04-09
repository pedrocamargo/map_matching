from src import Trip
from parameters import load_parameters
import pandas as pd
import numpy as np

t = Trip()

# data quality parameters
p = load_parameters('data quality')
t.set_data_quality_parameters(p)

t.set_stop_algorithm('Maximum space')
p = load_parameters('stops parameters')[t.stops_algorithm]
t.set_stops_parameters(p)

df = pd.read_csv('example_data/example_data_generic_gps_data.csv')
df['datetime']= df.Date.astype(str) + ' ' + df.time.astype(str)
df.datetime = pd.to_datetime(df.datetime)

field_dict = {"latitude": "LATITUDE",
              "longitude": "LONGITUDE",
              "timestamp": "datetime"}

t.populate_with_dataframe(df, field_dict)
