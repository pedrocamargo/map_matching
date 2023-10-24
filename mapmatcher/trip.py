import logging
from math import sqrt
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd

from .parameters import Parameters


class Trip:
    def __init__(self, gps_trace: gpd.GeoDataFrame, parameters: Parameters, stops: Optional[gpd.GeoDataFrame] = None):
        # Fields necessary for running the algorithm

        self.__coverage = -1

        self.parameters = parameters
        self.stops = stops
        self.__error = True
        self._error_type= "Data not loaded yet"

        # Creates the properties for the outputs
        self.trace = gps_trace.to_crs(parameters.geoprocessing.projected_crs)
        self.__pre_process()

        # Indicators to show if we have the optional fields in the data
        self.has_heading = "heading" in gps_trace

    def map_match(self):
        pass

    @property
    def coverage(self) -> float:
        if coverage is None:
            x1, y1, x2, y2 = self.trace.total_bounds
            coverage = sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2) / 1000
        return self.__coverage

    def __pre_process(self):
        dqp = self.parameters.data_quality

        if "trace_id" not in self.trace:
            self._error_type = "Trace does not have field trace_id"
            return
        
        if len(self.trace_id.unique()) > 1:
            self._error_type = "trace_id is not unique"
            return
        
        self.id = self.trace.trace_id.values[0]

        # Check number of pings
        if self.trace.shape[0] < dqp.minimum_pings:
            self._error_type =  f"Vehicle with only {self.trace.shape[0]} pings"
            return

        if self.coverage < dqp.minimum_coverage:
            self._error_type =  f"Vehicle covers only {self.coverage:,2} km"
            return

        # removes pings on the same spot
        self.trace["posix_time"] = (self.trace.timestamp - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

        diff_pings = self.trace.shape[0] - len(self.trace.posix_time.unique())
        if diff_pings:
            logging.warning(f"There are {diff_pings:,} with the same timestamp")

            df = pd.DataFrame(
                {"posix_time": self.trace.ping_posix_time, "x": self.trace.geometry.x, "y": self.trace.geometry.y}
            )
            agg = df.groupby(["posix_time"]).agg(["min", "max", "count"])
            jitter = np.sqrt((agg.x["min"] - agg.x["max"]) ** 2 + (agg.y["min"] - agg.y["max"]) ** 2) / 1000
            if np.max(jitter) > (1000 * dqp.maximum_jittery):
                self._error_type = f"Data is jittery. Same timestamp with max difference of {np.max(jitter):,.2} m"
                return

            self.trace.drop_duplicates(subset=["posix_time"], inplace=True,keep="first")

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
                self._error_type =  f"Max speed surpassed for {w} seconds"
                return

        self._error_type = ""
        self.__error = False

    def __compute_stops(self):
        if self.stops is not None:
            return
