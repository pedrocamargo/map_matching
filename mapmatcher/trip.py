from typing import Optional
from .parameters import Parameters
import geopandas as gpd
import numpy as np
import pandas as pd
from geopy.distance import great_circle


class Trip:
    def __init__(self, parameters: Parameters, gps_trace: gpd.GeoDataFrame, stops: Optional[gpd.GeoDataFrame] = None):
        # Fields necessary for running the algorithm
        self.parameters = parameters
        self.stops = stops

        # Creates the properties for the outputs
        self.trace = gps_trace.reset_index(drop=True)
        self.__pre_process

        self.reset()

        # Indicators to show if we have the optional fields in the data
        self.has_speed = "speed" in gps_trace
        self.has_azimuth = "azimuth" in gps_trace

        # related to stop algorithms
        self.set_stop_algorithm()

        # related to data quality parameters
        self.data_quality_parameters = None

    def map_match(self):
        pass
    
    def __pre_process(self):
        # Check number of pings
        if self.trace.shape[0] < dqp.minimum_pings:
            self.error = f"Vehicle with only {self.trace.shape[0]} pings"

        # Test ping coverage
        p1 = [self.trace.latitude.max(), self.trace.longitude.max()]
        p2 = [self.trace.latitude.min(), self.trace.longitude.min()]
        coverage = self.gc(p1[1], p1[0], p2[1], p2[0])
        if coverage < dqp.minimum_coverage:
            self.error = f"Vehicle covers only {coverage:,2} km"
            return

        self.trace["prev_lat"] = self.trace["latitude"].shift(1)
        self.trace.prev_lat.iloc[0] = self.trace.latitude.iloc[0]
        self.trace["prev_long"] = self.trace["longitude"].shift(1)
        self.trace.prev_long.iloc[0] = self.trace.longitude.iloc[0]
        self.trace["prev_timestamp"] = self.trace["timestamp"].shift(1)
        self.trace.prev_timestamp.iloc[0] = self.trace.timestamp.iloc[0]

        # Create data quality fields
        dist = self.trace.apply(lambda x: self.gc(x["longitude"], x["latitude"], x["prev_long"], x["prev_lat"]), axis=1)
        ttime = (self.trace["timestamp"] - self.trace["prev_timestamp"]).dt.seconds.astype(float)
        speed = dist / (ttime / 3600)

        self.trace["distance"] = dist
        self.trace["traveled_time"] = ttime
        self.trace["speed"] = speed
        self.trace["speed"].fillna(-1)

        # Verify data quality
        dqp = self.parameters.data_quality
        w = int(self.trace.traveled_time[self.trace.speed > dqp.max_speed].sum())
        if w > dqp.max_speed_time:
            # If there is evidence of speeding for longer than tolerated, we will see if it happens continuously
            too_fast = self.trace.groupby(self.trace.speed > dqp.max_speed)["traveled_time"].cumsum()
            if too_fast.max() > dqp.max_speed_time:
                self.error = f"Max speed surpassed for {w:,1} seconds"
                return


    def __compute_stops(self):
        if self.stops is not None:
            return
        

    # Great circle distance function
    @staticmethod
    def gc(a, b, c, d):
        p1 = (b, a)
        p2 = (d, c)
        return great_circle(p1, p2).kilometers
