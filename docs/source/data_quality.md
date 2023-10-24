(data_quality)=
Data Quality
############

Before map-matching a GPS trace, a series of data quality assurances are performed


Data jitter
-----------

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
