from math import sqrt
from typing import Optional

import geopandas as gpd
import numpy as np

from .parameters import Parameters


class Trip:
    def __init__(self, gps_trace: gpd.GeoDataFrame, parameters: Parameters, stops: Optional[gpd.GeoDataFrame] = None):
        # Fields necessary for running the algorithm

        self.__coverage = -1

        self.parameters = parameters
        self.stops = stops

        # Creates the properties for the outputs
        self.trace = gps_trace.to_crs(3857)
        self.__pre_process()

        # Indicators to show if we have the optional fields in the data
        self.has_heading = "azimuth" in gps_trace


    def map_match(self):
        pass

    @property
    def coverage(self)->float:
        if coverage is None:
            x1, y1, x2, y2 = self.trace.total_bounds
            coverage = sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2) / 1000
        return self.__coverage
    
    def __pre_process(self):
        dqp = self.parameters.data_quality

        # Check number of pings
        if self.trace.shape[0] < dqp.minimum_pings:
            self.error = f"Vehicle with only {self.trace.shape[0]} pings"

        if self.coverage < dqp.minimum_coverage:
            self.error = f"Vehicle covers only {self.coverage:,2} km"
            return

        # Create data quality fields
        dist = self.trace.distance(self.trace.shift(1))
        ttime = (self.trace["timestamp"] - self.trace["timestamp"].shift(1)).dt.seconds.astype(float)
        speed = dist / (ttime / 3600)
        speed[0] = 0
        self.trace["dist"] = dist.fillna(0)
        self.trace["traveled_time"] = ttime.fillna(0)
        self.trace["speed"] = speed
        self.trace.speed.fillna(-1)

        # Verify data quality
        w = int(self.trace.traveled_time[self.trace.speed > dqp.max_speed].sum())
        if w > dqp.max_speed_time:
            # If there is evidence of speeding for longer than tolerated, we will see if it happens continuously
            too_fast = self.trace.groupby(self.trace.speed > dqp.max_speed)["traveled_time"].cumsum()
            if too_fast.max() > dqp.max_speed_time:
                self.error = f"Max speed surpassed for {w} seconds"
                return

    def __compute_stops(self):
        if self.stops is not None:
            return
