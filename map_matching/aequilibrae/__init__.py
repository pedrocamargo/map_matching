"""
/***************************************************************************
 AequilibraE - www.aequilibrae.com
 
    Name:        QGIS plugin initializer
                              -------------------
        begin                : 2014-03-19
        copyright            : AequilibraE developers 2014
        Original Author: Pedro Camargo pedro@xl-optim.com
        Contributors: 
        Licence: See LICENSE for https://github.com/AequilibraE/AequilibraE
 ***************************************************************************/

"""

# This portion of the script initializes the plugin, making it known to QGIS.
import sys
sys.dont_write_bytecode = True

from graph import Graph        # We import the graph
from path_results import PathResults
from AoN import path_computation
