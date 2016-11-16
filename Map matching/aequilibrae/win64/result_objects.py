"""
Original Algorithm for Shortest path (Dijkstra with a Fibonacci heap) was written by Jake Vanderplas <vanderplas@astro.washington.edu> under license: BSD, (C) 2012

Codes for route ennumeration, DAG construction and Link nesting were written by Pedro Camargo (2013) and have all their rights reserved to the author
"""

# GRAPH STRUCTURE:  LINK ID, NODE A, NODE B, IMPEDANCE, SKIM1, SKIM2, ..., SKIM_N
                                                         #OPTIONAL FIELDS
import multiprocessing as M
import numpy as np

class PathResults:
    def __init__(self):
        """
        @type graph: Set of numpy arrays to store Computation results
        """
        self.predecessors = None
        self.connectors = None
        self.temporary_skims = None
        self.path = None
        self.pathnodes = None
        self.milepost = None

        self.links = -1
        self.nodes = -1
        self.zones = -1
        self.num_skims = -1

    def prepare(self, graph):

        self.nodes = graph.num_nodes + 1
        self.zones = graph.centroids + 1
        self.links= graph.num_links + 1
        self.num_skims = graph.skims.shape[1]

        self.predecessors = np.zeros(self.nodes, dtype=np.int64)
        self.connectors = np.zeros(self.nodes, dtype=np.int64)
        self.temporary_skims = np.zeros((self.nodes, self.num_skims), np.float64)

    def reset(self):
        if self.predecessors is not None:
            self.predecessors.fill(-1)
            self.connectors.fill(-1)
            self.temporary_skims.fill(0)
            self.path = None
            self.pathnodes = None
            self.milepost = None

            self.links = -1
            self.nodes = -1
            self.zones = -1
            self.num_skims = -1
        else:
            print 'Exception: Path results object was not yet prepared/initialized'

class AssignmentResults:
    def __init__(self):
        """
        @type graph: Set of numpy arrays to store Computation results
        """
        self.linkloads = None   # The actual results for assignment
        self.predecessors = None # The predecessors for each node in the graph
        self.connectors = None  # The previous link for each node in the tree
        self.skims = None # The array of skims
        self.no_path = None # The list os paths
        self.temporary_skims = None
        self.cores = M.cpu_count()

        self.nodes = -1
        self.zones = -1
        self.links= -1

    #In case we want to do by hand, we can prepare each method individually
    def prepare(self, graph):

        self.nodes = graph.num_nodes + 1
        self.zones = graph.centroids + 1
        self.links = graph.num_links + 1
        self.num_skims = graph.skims.shape[1]
        self.__redim()

    def reset(self):
        if self.predecessors is not None:
            self.predecessors.fill(-1)
            self.connectors.fill(-1)
            self.skims.fill(0)
            self.no_path.fill(-1)
        else:
            print 'Exception: Assignment results object was not yet prepared/initialized'

    def __redim(self):
        self.linkloads = np.zeros((self.links, self.cores), np.float64)
        self.predecessors = np.zeros((self.nodes, self.cores), dtype=np.int64)
        self.connectors = np.zeros((self.nodes, self.cores), dtype=np.int64)

        self.skims = np.zeros((self.zones, self.zones, self.num_skims), np.float64)
        self.no_path = np.zeros((self.zones, self.zones, self.cores), dtype=np.int64)
        self.temporary_skims = np.zeros((self.nodes, self.num_skims, self.cores), np.float64)

        self.reset()

    def set_cores(self, cores):
        if isinstance(cores, int):
            if cores > 0:
                if self.cores != cores:
                    self.cores = cores
                    if self.predecessors is not None:
                        self.__redim()

            else:
                raise ValueError("Number of cores needs to be equal or bigger than one")
        else:
            raise ValueError("Number of cores needs to be an integer")
    def results(self):
        return np.sum(self.linkloads, axis=1)