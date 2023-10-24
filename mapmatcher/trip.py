import logging
from math import sqrt
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd

from mapmatcher.network import Network

from .parameters import Parameters


class Trip:
    def __init__(
        self, gps_trace: gpd.GeoDataFrame, parameters: Parameters, network: Network, stops=gpd.GeoDataFrame([])):
        # Fields necessary for running the algorithm

        self.__coverage = -1.1
        self.__links_used = []
        self.id = -1

        self.parameters = parameters
        self.stops = stops.to_crs(parameters.geoprocessing.projected_crs)
        self._stop_nodes = []
        self.network = network
        self._error_type = "Data not loaded yet"

        # Creates the properties for the outputs
        self.trace = gps_trace.to_crs(parameters.geoprocessing.projected_crs)
        self.__pre_process()

        # Indicators to show if we have the optional fields in the data
        self.has_heading = "heading" in gps_trace

    def map_match(self, ignore_errors=False):
        if self.has_error:
            if not ignore_errors:
                logging.critical(f"Cannot map-match due to : {self._error_type}. You can also ignore errors")
        self.__compute_stops()

        pass

    @property
    def coverage(self) -> float:
        if self.__coverage < 0:
            x1, y1, x2, y2 = self.trace.total_bounds
            self.__coverage = sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
        return self.__coverage

    @property
    def has_error(self) -> bool:
        return len(self._error_type) > 0

    def __pre_process(self):
        dqp = self.parameters.data_quality

        if "trace_id" not in self.trace:
            self._error_type = "Trace does not have field trace_id"
            return

        if len(self.trace.trace_id.unique()) > 1:
            self._error_type = "trace_id is not unique"
            return

        self.id = self.trace.trace_id.values[0]

        # Check number of pings
        if self.trace.shape[0] < dqp.minimum_pings:
            self._error_type = f"Vehicle with only {self.trace.shape[0]} pings. Minimum is {dqp.minimum_pings}"
            return

        if self.coverage < dqp.minimum_coverage:
            self._error_type = f"Vehicle covers only {self.coverage:,.2} m. Minimum is {dqp.minimum_coverage}"
            return

        # removes pings on the same spot
        self.trace["ping_posix_time"] = (self.trace.timestamp - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

        diff_pings = self.trace.shape[0] - len(self.trace.ping_posix_time.unique())
        if diff_pings:
            logging.warning(f"There are {diff_pings:,} with the same timestamp")

            df = pd.DataFrame(
                {"ping_posix_time": self.trace.ping_posix_time, "x": self.trace.geometry.x, "y": self.trace.geometry.y}
            )
            agg = df.groupby(["ping_posix_time"]).agg(["min", "max", "count"])
            jitter = np.sqrt((agg.x["min"] - agg.x["max"]) ** 2 + (agg.y["min"] - agg.y["max"]) ** 2)
            if np.max(jitter) > (dqp.maximum_jittery):
                self._error_type = f"Data is jittery. Same timestamp {np.max(jitter):,.2} m apart."
                return

            self.trace.drop_duplicates(subset=["ping_posix_time"], inplace=True, keep="first")

        # Create data quality fields
        dist = self.trace.distance(self.trace.shift(1))
        ttime = (self.trace["timestamp"] - self.trace["timestamp"].shift(1)).dt.seconds.astype(float)
        speed = dist / ttime
        speed[0] = 0
        self.trace["trace_segment_dist"] = dist.fillna(0)
        self.trace["trace_segment_traveled_time"] = ttime.fillna(0)
        self.trace["trace_segment_speed"] = speed
        self.trace.trace_segment_speed.fillna(-1)

        # Verify data quality
        w = int(self.trace.trace_segment_traveled_time[self.trace.trace_segment_speed > dqp.max_speed].sum())
        if w > dqp.max_speed_time:
            # If there is evidence of speeding for longer than tolerated, we will see if it happens continuously
            too_fast = self.trace.groupby(self.trace.trace_segment_speed > dqp.max_speed)[
                "trace_segment_traveled_time"
            ].cumsum()
            if too_fast.max() > dqp.max_speed_time:
                self._error_type = f"Max speed surpassed for {w} seconds"
                return

        self._error_type = ""

    def __compute_stops(self):
        if len(self._stop_nodes):
            return

        if self.stops.shape[0]:
            # TODO: Add capability of computing stops
            pass

        self._stop_nodes = self.stops.sjoin_nearest(self.network.nodes, distance_col="ping_dist").node_id.tolist()

    def __network_links(self):
        cand = self.network.links.sjoin_nearest(
            trace=self.trace, distance_col="ping_dist", max_distance=self.parameters.map_matching.buffer_size
        )

        if self.network.has_speed:
            cand = cand[cand[self.network._speed_field] <= cand.trace_segment_speed]

        if not self.has_heading:
            self.__links_used = cand.link_id.tolist()
            return

        # TODO: Add consideration of heading
        # TODO: Add heuristic to give bigger discounts for dasd
        self.__links_used = cand.link_id.tolist()
