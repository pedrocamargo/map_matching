import os, sys
import pandas as pd
from map_matching import *

out_folder = load_parameters('output_folder')

single_trip = Trip()

# data quality parameters
p = load_parameters('data quality')
single_trip.set_data_quality_parameters(p)

single_trip.set_stop_algorithm('Maximum space')
p = load_parameters('stops parameters')[single_trip.stops_algorithm]
single_trip.set_stops_parameters(p)

# Loading the GPS DATA
df = pd.read_csv('example_data/MPO_DATA_EXAMPLE/tr_183_5000748601_2016-10-20.csv')
df['ping_id'] = df.index # We create ping ids, because the database doesn't have it
df['timestamp'] = 0
df.timestamp[:] = pd.to_datetime(df.LocalTime)[:]

# Data source specific
field_dict = {"latitude": "Latitude",
              "longitude": "Longitude",
              "timestamp": "timestamp"}

single_trip.populate_with_dataframe(df, field_dict)

# Loading the NETWORK
net = Network(out_folder)
par = load_parameters('geoprocessing parameters')
net.set_geometry_parameters(par)

p = load_parameters('network file fields')
net.load_network('example_data/MPO_DATA_EXAMPLE/Links_2017_wgs_main.shp', p)

net.load_nodes('example_data/MPO_DATA_EXAMPLE/Peoria_stops_wgs.shp', 'ID')

map_match(single_trip, net)

print single_trip.path.to_csv(os.path.join(out_folder, 'result.csv'), index=False)
