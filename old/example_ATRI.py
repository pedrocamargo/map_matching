import os

# os.chdir('/home/pedro/SourceCode/map_matching')
import pandas as pd

from map_matching import *

out_folder = load_parameters("output_folder")

trips = Trip()

# data quality parameters
p = load_parameters("data quality")
trips.set_data_quality_parameters(p)

trips.set_stop_algorithm("Delivery stop")
p = load_parameters("stops parameters")[trips.stops_algorithm]
trips.set_stops_parameters(p)

# Loading the GPS DATA
df = pd.read_csv("example_data/ATRI-LIKE-SAMPLE.csv")
df.readdate = pd.to_datetime(df.readdate)

# Data source specific
field_dict = {"latitude": "y", "longitude": "x", "timestamp": "readdate", "azimuth": "heading", "ping_id": "PingID"}

trips.populate_with_dataframe(df, field_dict)

find_stops(trips)

# Loading the NETWORK
net = Network(out_folder)
par = load_parameters("geoprocessing parameters")
net.set_geometry_parameters(par)

p = load_parameters("network file fields")
net.load_network("example_data/FAF_Network.shp", p)

net.load_nodes("example_data/FAF_Nodes.shp", "ID")

map_match(trips, net)
print(trips.path.to_csv(os.path.join(out_folder, "result_atri_like_data.csv"), index=False))
