#-------------------------------------------------------------------------------
# Name:       module1
# Purpose:    Importing Vehicle GPS data streams into HDF5 containers for future processing
#
# Author:      Pedro Camargo
#
# Created:     01/10/2015
# Copyright:   (c) pcamargo 2015
# Licence:     ALL RIGHTS RESERVED. NOT TO BE USED OR DISTRIBUTED WITHOUT THE AUTHOR'S PERMISSION
#-------------------------------------------------------------------------------

import os
import ntpath
import glob
import pandas as pd
from parameters import load_parameter
import multiprocessing as mp

def main():

    # Retrieves folder structure parameters
    data_folder = load_parameter('folders', 'csv folder')

    cores = load_parameter('processing', 'cores')
    pool = mp.Pool(cores)
    for ProcessingFile in glob.glob(data_folder + "*.csv"):
        pool.apply_async(import_job, args=(ProcessingFile,))
    pool.close()
    pool.join()


def import_job(ProcessingFile):
    try:
        out_data_folder = load_parameter('folders', 'hdf5 folder')

        ping_id = load_parameter('fields', 'ping id')
        timestamp = load_parameter('fields', 'timestamp')

        # Load the source data into a pandas table
        gps_data = pd.read_csv(ProcessingFile)
        head, ProcessingFile = ntpath.split(ProcessingFile)

        # Delete repeated records
        deleted = gps_data[ping_id].shape[0]
        gps_data.drop_duplicates(gps_data.columns, inplace=True)

        # makes sure the timestamp is correct
        gps_data[timestamp] = pd.to_datetime(gps_data[timestamp])

        store = pd.HDFStore(out_data_folder + ProcessingFile[0:-4] + '.h5')
        store['/gps_data'] = gps_data
        store.close()
        del gps_data
        print ProcessingFile, 'imported successfully'

    except:
        print ProcessingFile, 'failed'


if __name__ == '__main__':
    main()
