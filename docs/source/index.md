MapMatcher
############

The Python **MapMatcher** package is yet another Python package designed to 
map-match GPS traces onto a network.

The main difference between this package and other existing solutions is that it 
is built around (AequilibraE)[https://www.aequilibrae.com], adding a powerful resource
to its ecosystem.

Following on the AequilibraE ethos, however, one does not need an AequilibraE model 
to use **MapMatcher** to map-match GPS traces to a bespoke link network.

To cite **MapMatcher**

::

  P. Camargo, S. Hong, and V. Livshits, ‘Expanding the Uses of Truck GPS Data in Freight Modeling and Planning Activities’, Transportation Research Record, vol. 2646, no. 1, pp. 68–76, Jan. 2017, doi: 10.3141/2646-08.


(network_data)=
Network data
============

Three pieces of network data are required by **MapMatcher**

1. An AequilibraE Graph
2. A GeoPandas GeoDataFrame with all **links** in the graph
3. A GeoPandas GeoDataFrame with all **nodes** in the graph


(gps_data)=
GPS data
========

(data_quality)=
Data Quality
------------

Before map-matching a GPS trace, a series of data quality assurances are performed


Data jitter
***********

MapMatcher is designed to work with time stamps at the 1s resolution, and it 
may happen that a single GPS trace have multiple records at the same instant
but at slightly different positions. Since a single GPS device cannot be 
in two places at the same time, there is data quality parameter to control for
the maximum *jitter* acceptable in the model, which defaults to zero.
The parameter can be changed before any data is loaded into the mapmatcher
instance (to 2.5 meters, for example)

::

    matcher = Mapmatcher()
    matcher.parameters.data_quality.maximum_jittery = 2.5


(algorithms)=
Algorithms
==========

(stop_finding)=
Stop Finding
------------


DeliveryStop
************


MaximumSpace
************


(path_reconstruction)=
Path Reconstruction
-------------------
