"""
/***************************************************************************
 AequilibraE - www.aequilibrae.com
 
    Name:        QGIS plugin initializer
                              -------------------
        begin                : 2014-03-19
        copyright            : AequilibraE developers 2014
        Original Author: Pedro Camargo pedro@xl-optim.com
        Contributors: 
        Licence: See LICENSE.TXT
 ***************************************************************************/

"""

# This portion of the script initializes the plugin, making it known to QGIS.
import sys
sys.dont_write_bytecode = True

from .graph import Graph        # We import the graph
from .assignment import *


# We import the algorithms
import platform
plat = platform.system()

if plat == 'Windows':
    import struct
    if (8 * struct.calcsize("P")) == 64:
        from win64 import *
        
    if (8 * struct.calcsize("P")) == 32:
        from win32 import *

if plat == 'Darwin':
    from mac import *