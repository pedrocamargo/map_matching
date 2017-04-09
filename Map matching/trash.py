import os
import pandas as pd
import numpy as np
from time import clock

os.chdir('D:\\ATRI_Processing\\CODE\\Map matching')
import parameters as P
from aequilibrae import Graph, PathResults, path_computation

print 'Reading graph'
graph = Graph()
graph.add_single_field('real_length')
graph.load_graph_from_disk(P.shape_folder + P.graph_file)
graph.set_graph(10, cost_field = 'length')
orig_cost=graph.cost.copy()

results= PathResults()
results.prepare(graph)
results.reset()

print graph.skims.shape[:]
origin = 567
destination = 177

path_computation(origin, destination, graph, results)
