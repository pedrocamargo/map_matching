import os

# MODEL PARAMETERS
tolerance = 22.5
buffer_size = 0.0006

# Penalties for using certain types of links
link_penalties = {0:1000,
                  1: 0.75,
                  2: 0.85,
                  3: 1.0,
                  4: 1.0,
                  5: 1000,
                  6: 1.0,
                  7: 1.0,
                  8: 1.0,
                  9: 1.0,
                  10: 1.0,
                  11: 2.0,
                  12: 1000,
                  13: 1000,
                  14: 1000,
                  15: 1000}



#inputs
graph_file = 'GRAPH.csv'


#  FOLDERS
main_folder = 'D:/Pedro/UCI Google Drive/MAG/ATRI_Processing/'
sample_folder = '2014_SAMPLE/'

shape_folder = main_folder + 'Network/'
data_folder = main_folder + 'DATA/' + sample_folder + 'hdf5/'
links_per_route_folder = main_folder + 'DATA/' + sample_folder + 'links_per_route/'
truck_stats_folder = main_folder + 'DATA/' + sample_folder + 'truck_stats/'
stops_per_route_folder = main_folder + 'DATA/' + sample_folder + 'stops_per_route/PARTIAL/'
matching_results_folder = main_folder + 'DATA/' + sample_folder + 'map matching/'
analysis_folder = main_folder + 'DATA/' + sample_folder + 'map matching/analysis/'
temp_folder = main_folder + 'DATA/' + sample_folder + 'temp/'
log_folder = main_folder + 'DATA/' + sample_folder + 'logs/'
folders = [shape_folder, data_folder, links_per_route_folder, truck_stats_folder, stops_per_route_folder,
           matching_results_folder, analysis_folder]

for f in folders:
    if not os.path.exists(f):
        os.makedirs(f)

#Outputs

atri_data = 'ATRI_Data.h5'
all_stops = 'DATA_stops.h5'
lu = 'ALL_STOPS_LU.h5'
taz = 'ALL_STOPS_TAZ.h5'
final_lu = lu
truck_final_stats = 'truck_final_stats.h5'
node_list = 'used_nodes.csv'
# PROCESSING PARAMETERS

cores = 22 # Number of independent processors to use with Multi2011
data_chunks = 22 # Number of slices to divide the data in for 2011 (to feed in multiprocessing)

multiple_spatial_indexes = True
lat_long_breakage = 0.25  # This is the parameter used to break the layer of land use in more than one piece.
                          # Although not necessary, it helps to do this because having a spatial index that is too
                          # large makes all searches much slower

#Files

skims = 'truck_skims.csv'
land_use_file = 'SINGLE_LAYER.shp'
taz_layer = 'ZoningSystem.shp'