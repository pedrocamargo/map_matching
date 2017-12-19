# Map Matching
Map matching / route reconstruction algorithm

If you use this code, please reference it in the following way:
Camargo, P., Hong, S., Livshits, V., 'Expanding the Uses of Truck GPS Data in Freight Modeling and Planning Activities', Transportation Research Record, In Press

## Dependencies
The code depends on a number of specialized libraries

* PyYaml
##### Data frames and array computation
* Pandas
* NumPY

##### Geographic operations
* geopy
* rtree
* shapefile
* fiona
* shapely

##### Path Computation
* AequilibraE

Since AequilibraE is not yet an established library, binaries for Windows 32 and 64 bits, MAC and Linux are provided.
The source code for the relevant portions of AequilibraE at the time of compilation is also included in this project
For further references, https://github.com/AequilibraE/AequilibraE
 

## Overview

The Code is composed by a series of 5 steps:

1. Importing the data from CSV
2. Finding stops for each sequence
3. Finding the links likely used by the vehicles
4. re-constructing the routes
5. Exporting routes and sequence of nodes along routes to spatialite

The code is based on having two objects: A trip and a network. Both classes are implemented and are the basis for the code
I only implemented a method to import data from CSV. However, it is easy to add a method to import data from other data sources. A method to import data for sqlite will be developed in the future. Please fill in an issue to request its development if needed



##### The data is expected to have the following fields:
 
* **ping_id** (integer or long): Identifier for the ping ID (needs to be unique within a same vehicle)
* **latitude** (float): latitude
* **longitude** (float): longitude
* **timestamp** (data-time format): timestamp for the data file


##### The optional fields that can be also used in the algorithm are:
* **azimuth** (float): Direction (degrees [0,359]) the vehicle was heading when ping was registered
* **speed** (float): Speed the vehicle was travelling at when ping happened

##### Parallelization

Map-matching (for cold data) is an embarassingly parallel problem. However, no advanced parallelization was implemented so far. Contributions on this issue are welcome

# Using the code

## 1. The Trip class
  A trip is the path a vehicle did that needs to be map-matched.
  It has the following properties:
  1. **GPS trace**: The input data (**Mandatory fields**: trip_id, ping_id, latitude, longitude, timestamp **Optional fields**: azimuth, speed)
  2. **data_quality_parameters**: Dictionary with the following fields["max speed", "max speed time", "minimum_pings", "minimum_coverage"]. Maximum speed allowed for vehicle (km/h), time permited above the allowed time (seconds), minimum number of pings, and minimum coverage of the pings in km (diagonal of the bounding box)


  It has the following methods:
  1. **populate_from_csv**: Reads the data from CSV. Arguments are the path to the csv file. It takes an optional argument for the dictionary of fields (e.g. {'trip_id': 'my_arbitrary_name_for_trip_id')
  2. **set_stops_algorithm**: Set the algorithm for finding stops. Takes a string ("Maximum space" or "Delivery stop")
  3. **set_stops_parameters**: Set the criteria for the stop algorithm. Takes a dictionary. For "Maximum space", it needs ["time", "min distance", "max distance"]. For "Delivery stop" it needs ["stopped speed", "min time stopped", "max time stopped", "max stop coverage"]
  
  
## The network class



## Units 
 All units in the code are:
 * **Distance**: km
 * **Time**: h
 * **Coordinates**: Degrees (and its decimal parts)t  
  
