import pandas as pd

class Trip:
    def __init__(self):
        # Fields necessary for running the algorithm
        self.mandatory_fields = ["ping_id", "latitude", "longitude", "timestamp"]
        self.optional_fields = ["azimuth", "speed"]
        self.all_fields = self.mandatory_fields + self.optional_fields

        # Creates the properties for the outputs
        self.gps_trace = None
        self.used_links = None
        self.graph_links = None
        self.stops = None
        self.error = None
        self.path = None

        self.reset()

        # Indicators to show if we have the optional fields in the data
        self.has_speed = False
        self.has_azimuth = False

        # related to stop algorithms
        self.stops_algorithms_available = ["Maximum space", "Delivery stop"]
        self.stops_algorithm = None
        self.stops_parameters = None
        self.set_stop_algorithm()

        # related to data quality parameters
        self.data_quality_parameters = None

    def set_data_quality_parameters(self, parameters):
        self.data_quality_parameters = parameters

    def set_stop_algorithm(self, algo=False):
        if not algo:
            algo = self.stops_algorithms_available[0]

        if algo in self.stops_algorithms_available:
            self.stops_algorithm = algo
        else:
            t = ''
            for i in self.stops_algorithms_available:
                t += i + ','
            raise Exception('Bad algorithm for finding stops. Available algorithms are ' + t[0:-1])

    # The stop criteria needs to be a dictionary
    def set_stops_parameters(self, criteria):
        self.stops_parameters = criteria

    def set_parameters(self):
        pass

    # Loading the data from a csv file
    def populate_from_csv(self, csv_file, fields_dictionary = False):
        gps_data = pd.read_csv(csv_file)
        self.populate_with_dataframe(gps_data, fields_dictionary)

    def populate_with_dataframe(self, gps_data, fields_dictionary = False):
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

        self.gps_trace.sort_values(["timestamp"], ascending=True, inplace=True, kind='quicksort', na_position='last')
        self.gps_trace.reset_index(drop=True)
        self.pre_process()

    def pre_process(self):
        self.gps_trace['prev_lat'] = self.gps_trace["latitude"].shift(1)
        self.gps_trace.prev_lat.iloc[0] = self.gps_trace.latitude.iloc[0]
        self.gps_trace['prev_long'] = self.gps_trace["longitude"].shift(1)
        self.gps_trace.prev_long.iloc[0] = self.gps_trace.longitude.iloc[0]
        self.gps_trace['prev_timestamp'] = self.gps_trace["timestamp"].shift(1)
        self.gps_trace.prev_timestamp.iloc[0] = self.gps_trace.timestamp.iloc[0]


    def reset(self):
        # Creates the dataframe for the GPS trace
        data = {x: [] for x in self.all_fields}
        self.gps_trace = pd.DataFrame(data)

        # Creates the properties for the outputs
        self.used_links = None
        self.graph_links = None
        self.stops = None
        self.error = None
        self.path = None