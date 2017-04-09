import numpy as np
import pandas as pd
from aequilibrae.paths import Graph

class Network:
    def __init__(self):

        # Creates the properties for the outputs
        self.links = None
        self.nodes = None
        self.graph = Graph

        # Fields necessary for running the algorithm
        self.mandatory_fields = ["trip_id", "ping_id", "latitude", "longitude", "timestamp"]
        self.optional_fields = ["azimuth", "speed"]
        self.all_fields = self.mandatory_fields + self.optional_fields

        # Creates the dataframe for the GPS trace
        data = {x:[] for x in self.all_fields}
        self.gps_trace = pd.DataFrame(data)




        # Indicators to show if we have the optional fields in the data
        self.has_speed = False
        self.has_azimuth = False

    # Loading the data from a csv file
    def populate_from_csv(self, csv_file, fields_dictionary = False):
        gps_data = pd.read_csv(csv_file)

        if fields_dictionary:
            # The fields dictionary only needs to exist for the  fields that do not match what we are expecting
            for f in self.mandatory_fields:
                if f in fields_dictionary:
                    self.gps_trace[f] = gps_data[fields_dictionary[f]][:]
                else:
                    self.gps_trace[f] = gps_data[f][:]

            f = "azimuth"
            if f in fields_dictionary:
                self.gps_trace[f] = gps_data[fields_dictionary[f]][:]
                self.has_azimuth = True
            else:
                if f in list(gps_data.columns):
                    self.gps_trace[f] = gps_data[f][:]
                    self.has_azimuth = True

            f = "speed"
            if f in fields_dictionary:
                self.gps_trace[f] = gps_data[fields_dictionary[f]][:]
                self.has_speed = True
            else:
                if f in list(gps_data.columns):
                    self.gps_trace[f] = gps_data[f][:]
                    self.has_speed = True
        else:

            for f in self.mandatory_fields:
                self.gps_trace[f] = gps_data[f][:]

            f = "azimuth"
            if f in list(gps_data.columns):
                self.gps_trace[f] = gps_data[f][:]
                self.has_azimuth = True

            f = "speed"
            if f in list(gps_data.columns):
                self.gps_trace[f] = gps_data[f][:]
                self.has_speed = True