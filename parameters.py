import os
import yaml
import multiprocessing as M


def load_parameter(group, parameter):
    with open('parameters.yaml', 'r') as yml:
        par_values = yaml.safe_load(yml)
    return par_values[group][parameter]


'''
# Field names
# These names are the mandatory fields that need to exist in the dataset. Additional fields will be ignored and NOT
# carried forward as the data is processed and new tables are generated
ping_id = 'PingID'
vehicle_id = 'VehicleID'
latitude = 'latitude'
longitude = 'longitude'
timestamp = 'timestamp'
azimuth = 'azimuth'
speed = 'speed'

# Path computation


#  FOLDERS
master_folder = 'D:/GOOGLE_DRIVES/MapMatchingVehicleGPSData'
csv_folder = master_folder + '/GPS DATA/'
hdf5_folder = master_folder + '/OUTPUTS/Data streams in HDF5/'
stops_folder = master_folder + '/OUTPUTS/Vehicle stops/'
map_matched_folder = master_folder +'/OUTPUTS/map_matched/'
temp_folder =  master_folder + '/_temp/'
log_folder = master_folder + '/_logs/'
vehicles_folder = master_folder + '/OUTPUTS/vehicle_stats/'
layer_folder = master_folder + '/NETWORK LAYER/'


# PROCESSING PARAMETERS
cores = M.cpu_count() #Number of independet processors to use with Multi2011
data_chunks = cores + 1 # Number of slices to divide the data in for 2011 (to feed in multiprocessing)

'''


'''
stats_folder = master_folder + '/Analytics/'
alldata_folder = master_folder + '/ALL_DATA/'
truck_classification_folder = master_folder + '/truck_classification/'
distance_folder = 'D:/ATRI_Processing/DATA/'





# MODEL PARAMETERS

closest = 5  #Closest land uses to find
stopped_speed = 5  # in mph
max_speed = 90    # in mph
max_speed_time = 120      # in seconds   --> time that the truck needs to be above the speed limit to be scraped
min_time_stopped = 5*60 # in seconds   --> minimum stopped time to be considered
max_time_stopped = 4*60*60 # in seconds   --> maximum stopped time to be considered
max_stop_coverage = 0.5 # in miles




folders = [csv_folder, hdf5_folder, stops_folder, temp_folder, log_folder, stats_folder, vehicles_folder,
           alldata_folder, truck_classification_folder]

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

# PROCESSING PARAMETERS

multiple_spatial_indexes = True
lat_long_breakage = 0.25  # This is the parameter used to break the layer of land use in more than one piece.
                          # Although not necessary, it helps to do this because having a spatial index that is too
                          # large makes all searches much slower

#Files

skims = 'truck_skims.csv'
land_use_file = 'SINGLE_LAYER.shp'
taz_layer = 'ZoningSystem.shp'

'''