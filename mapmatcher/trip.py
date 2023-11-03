import logging
from math import sqrt
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from aequilibrae.paths.results import PathResults
from shapely.geometry import LineString
from shapely.ops import linemerge

from mapmatcher.network import Network

from .parameters import Parameters
from .stop_finder import stops_maximum_space


class Trip:
    def __init__(
        self, gps_trace: gpd.GeoDataFrame, parameters: Parameters, network: Network, stops=gpd.GeoDataFrame([])
    ):
        # Fields necessary for running the algorithm

        self.__coverage = -1.1
        self.__candidate_links = np.array([])
        self.id = -1

        self.parameters = parameters
        self.stops = stops
        if self.stops.shape[0]:
            self.stops.to_crs(parameters.geoprocessing.projected_crs, inplace=True)
        self._stop_nodes = []
        self.__geo_path = LineString([])
        self.__mm_results = pd.DataFrame([], columns=["links", "direction", "milepost"])
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
        self.compute_stops()

        self.network.reset_graph()
        self.network.discount_graph(self.candidate_links)
        res = PathResults()
        res.prepare(self.network.graph)

        links = []
        directions = []
        mileposts = []
        position = 0
        for stop1, stop2 in zip(self._stop_nodes[:-1], self._stop_nodes[1:]):
            res.compute_path(stop1, stop2)
            if res.path.shape[0]:
                links.extend(res.path)
                directions.extend(res.path_link_directions)
                mileposts.extend(res.milepost[1:] + position)
                position += res.milepost[-1]

        self.__mm_results = pd.DataFrame({"links": links, "direction": directions, "milepost": mileposts})

    @property
    def path_shape(self) -> LineString:
        if not self.__geo_path.length:
            links = self.network.links.loc[self.__mm_results.links.to_numpy(), :]
            geo_data = []
            for (link_id, rec), direction in zip(links.iterrows(), self.__mm_results.direction.to_numpy()):
                geo = rec.geometry if direction > 0 else LineString(rec.geometry.coords[::-1])
                geo_data.append(geo)
            self.__geo_path = linemerge(geo_data)
        return self.__geo_path

    @property
    def result(self):
        links = self.network.links.loc[self.__mm_results.links.to_numpy(), :]
        # return links
        return gpd.GeoDataFrame(self.__mm_results, geometry=links.geometry.values, crs=links.crs).to_crs(
            self.network._orig_crs
        )

    @property
    def coverage(self) -> float:
        if self.__coverage < 0:
            x1, y1, x2, y2 = self.trace.total_bounds
            self.__coverage = sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
        return self.__coverage

    @property
    def has_error(self) -> bool:
        return len(self._error_type) > 0

    @property
    def candidate_links(self) -> np.ndarray:
        if self.__candidate_links.shape[0] > 0:
            self.__network_links()
        return self.__candidate_links

    def __pre_process(self):
        dqp = self.parameters.data_quality
        self._error_type = ""

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

        if self.coverage < dqp.minimum_coverage:
            self._error_type += f"  Vehicle covers only {self.coverage:,.2} m. Minimum is {dqp.minimum_coverage}"

        # removes pings on the same spot
        self.trace["ping_posix_time"] = (self.trace.timestamp - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

        diff_pings = self.trace.shape[0] - len(self.trace.ping_posix_time.unique())
        if diff_pings:
            logging.warning(f"There are {diff_pings:,} pings with the same timestamp")

            df = pd.DataFrame(
                {"ping_posix_time": self.trace.ping_posix_time, "x": self.trace.geometry.x, "y": self.trace.geometry.y}
            )
            agg = df.groupby(["ping_posix_time"]).agg(["min", "max", "count"])
            jitter = np.sqrt((agg.x["min"] - agg.x["max"]) ** 2 + (agg.y["min"] - agg.y["max"]) ** 2)
            if np.max(jitter) > (dqp.maximum_jittery):
                self._error_type += f"  Data is jittery. Same timestamp {np.max(jitter):,.2} m apart."
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
                self._error_type += f"  Max speed surpassed for {w} seconds"
                return

    def compute_stops(self):
        if len(self._stop_nodes):
            return

        if self.stops.shape[0]:
            if self.parameters.stop_algorithm == "maximum_space":
                algo_parameters = self.parameters.algorithm_parameters[self.parameters.stop_algorithm]
                self.stops = stops_maximum_space(self.trace, algo_parameters)
            else:
                raise NotImplementedError("Not implemented yet")

        self._stop_nodes = self.stops.sjoin_nearest(self.network.nodes, distance_col="ping_dist").node_id.tolist()

    def __network_links(self):
        if self.__candidate_links.shape[0]:
            return
        cand = self.network.links.sjoin_nearest(
            self.trace, distance_col="ping_dist", max_distance=self.parameters.map_matching.buffer_size
        )

        if self.network.has_speed:
            cand = cand[cand[self.network._speed_field] <= cand.trace_segment_speed]

        if not self.has_heading:
            self.__candidate_links = cand.link_id.to_numpy()
            return

        # TODO: Add consideration of heading
        # TODO: Add heuristic to give bigger discounts for dasd
        self.__candidate_links = cand.link_id.to_numpy()
