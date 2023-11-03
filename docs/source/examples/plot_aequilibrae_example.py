"""
Matching with an AequilibraE Model
==================================


"""

# %%
import uuid
from os.path import join
from pathlib import Path
from tempfile import gettempdir

from aequilibrae.utils.create_example import create_example
from mapmatcher.examples import nauru_data
from mapmatcher import MapMatcher

# %%
nauru_gps = nauru_data()

# Let's see if the data has all the fields we need
nauru_gps.head()

# %%
# Since it does not, let's fix it
nauru_gps.rename(
    columns={"x": "longitude", "y": "latitude", "gps_fix_id": "ping_id", "vehicle_unique_id": "trace_id"}, inplace=True
)

# %%
# We get our AequilibraE model for Nauru and create the map-mather from this model
# We also need to provide the transportation mode we want to consider for the
# map-matching
project = create_example(join(gettempdir(), uuid.uuid4().hex), "nauru")
mmatcher = MapMatcher.from_aequilibrae(project, "c")

# %%
# let's add the GPS data to the map-matcher and run it!
mmatcher.load_gps_traces(nauru_gps)
mmatcher.execute()
