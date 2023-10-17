import os, sys
import pandas as pd
import numpy as np
from map_matching import *

out_folder = load_parameters("output_folder")

single_trip = Trip()

# data quality parameters
p = load_parameters("data quality")
single_trip.set_data_quality_parameters(p)
single_trip.set_stop_algorithm("Exogenous")

# There is no need to setup parameters for stops, since we are not finding them
# p = load_parameters('stops parameters')[single_trip.stops_algorithm]
# single_trip.set_stops_parameters(p)


# Loading the GPS DATA
df = pd.read_csv("example_data/5000770301_2016-10-21_Pings.csv")
df["ping_id"] = df.index  # We create ping ids, because the database doesn't have it
df["timestamp"] = 0
df.timestamp[:] = pd.to_datetime(df.LocalTime)[:]

# Data source specific
field_dict = {"latitude": "Latitude", "longitude": "Longitude", "timestamp": "timestamp"}

single_trip.populate_with_dataframe(df, field_dict)

# Loading the NETWORK
net = Network(out_folder)
par = load_parameters("geoprocessing parameters")
net.set_geometry_parameters(par)

p = load_parameters("network file fields")
net.load_network("example_data/Links_2017_wgs_main.shp", p)

net.load_nodes("example_data/Nodes_2017_wgs_main.shp", "ID")


# Here is the difference from the other datasets, you need to build the stops table manually (because you already have
#   the stops from a different source)

# the stops table has a simple format. It is a list of stops, where each stop is a list itself
# the fields are: ['latitude', 'longitude', 'stop_time', 'duration', 'coverage']

# So let's read from the csv of stops

# trip.stops = pd.DataFrame(trip.stops, )
stops = pd.read_csv("example_data/5000770301_2016-10-21_Stops.csv")

# Your data structure results in these columns:
# ['HHID', 'HHPERSONID', 'StopID', 'StartTime', 'EndTime', 'mode_id', 'latitude', 'longitude']

# So we will rename them and create the columns we need

# We rename to have the columns we need
stops.columns = ["HHID", "HHPERSONID", "StopID", "stop_time", "EndTime", "mode_id", "latitude", "longitude"]
stops.stop_time[:] = pd.to_datetime(stops.stop_time)[:]
stops.EndTime[:] = pd.to_datetime(stops.EndTime)[:]

# create what the new columns we need
stops["duration"] = (stops.EndTime - stops.stop_time).dt.total_seconds().astype("int32")
stops["coverage"] = 0

# The duration for the first stop needs to be -99999999 and the last stop 99999999
stops["duration"].iloc[0] = -99999999
stops["duration"].iloc[-1] = 99999999

# Drop what we don't need
stops.drop(["HHID", "HHPERSONID", "StopID", "mode_id"], axis=1, inplace=True)

find_stops(single_trip, stops_table=stops)
find_network_links(single_trip, net)
find_route(single_trip, net)


print(single_trip.path.to_csv(os.path.join(out_folder, "result.csv"), index=False))
