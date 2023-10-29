import dataclasses


@dataclasses.dataclass
class geoprocessing:
    projected_crs: int = 3857  # We require a projected CRS to make sure all distance computations are correct


@dataclasses.dataclass
class data_quality:
    max_speed: float = 36.1  # in m/s
    max_speed_time: float = 120  # in seconds   --> time that the truck needs to be above the speed limit to be scraped
    minimum_pings: int = 20  # Minimum number of pings that the vehicle needs to have to be considered valid
    minimum_coverage: float = 2000  # Minimum diagonal of the Bounding box (m) defined by the GPS pings in the trace
    maximum_jittery: float = 1  # Maximum distance for which a vehicle can move within the same timestamp (m)


@dataclasses.dataclass
class map_matching:
    # map matching related parameters
    cost_discount: float = 0.1  # possibly used link cost reduction ratio
    buffer_size: float = 50  # Buffer around the links to capture links likely used. Unit is meters
    heading_tolerance: float = 22.5  # In case the network and the GPS data have headings, this is the tolerance to
    # be used to define if a GPS ping could have used a certain link


@dataclasses.dataclass
class maximum_space:  # Time in seconds
    max_time: float = 10800
    min_distance: float = 5000  # Distance in m. Measured as Great circle distance between each consecutive ping
    max_distance: float = 20000  # Distance in m. Measured as Great circle distance between each consecutive ping


# This is the algorithm commonly used for ATRI truck GPS data. Initially developed by Pinjari et. Al and improved by
# Camargo, Hong and Livshits (2017)
@dataclasses.dataclass
class delivery_stop:  # Time in seconds
    stopped_speed: float = 2.22  # in m/s
    min_time_stopped: float = 300  # 5*60 in seconds   --> minimum stopped time to be considered
    max_time_stopped: float = 14400  # 4*60*60 in seconds   --> maximum stopped time to be considered
    max_stop_coverage: float = 800  # in m


@dataclasses.dataclass
class Parameters:
    geoprocessing = geoprocessing()
    data_quality = data_quality()
    stop_algorithm = "delivery_stop"
    algorithm_parameters = {"delivery_stop": delivery_stop(), "maximum_space": maximum_space(), "exogenous": None}
    map_matching = map_matching()


# network file fields:
#   link_id: id
#   direction: dir
#   cost: length
#   interpolation: length

#   # Optional fields. Fill with 0 if not applicable
#   heading: AUTO  #For on-the-fly computation, use AUTO
#   speed_AB: 0
#   speed_BA: 0
