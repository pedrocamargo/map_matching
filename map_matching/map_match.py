

from .find_stops import find_stops
from .finding_network_links import find_network_links
from .finding_routes import find_route


def map_match(trip, network):
    find_stops(trip)
    find_network_links(trip, network)
    find_route(trip, network)