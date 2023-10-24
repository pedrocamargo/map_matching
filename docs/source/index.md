MapMatcher
############

The Python **MapMatcher** package is yet another Python package designed to 
map-match GPS traces onto a network.

There are a few key differences in mapmatcher that justify its existence:

* It is designed to map-match GPS traces into arbitrary networks (i.e. provided by the user)
* It is built on the (AequilibraE)[https://www.aequilibrae.com] data model (i.e. Incredibly 
  fast path computation and well-established data model)
* It is based on an heuristic that incorporates criteria developed in several papers 
  focusing in transport planning 
* Parallel processing through Python's multi-processing module