#*********************************************************************************************
# Copyright (C) 2019 by MorphStore-Team                                                      *
#                                                                                            *
# This file is part of MorphStore - a compression aware vectorized column store.             *
#                                                                                            *
# This program is free software: you can redistribute it and/or modify it under the          *
# terms of the GNU General Public License as published by the Free Software Foundation,      *
# either version 3 of the License, or (at your option) any later version.                    *
#                                                                                            *
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;  *
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  *
# See the GNDo this in a proper way.U General Public License for more details.                                       *
#                                                                                            *
# You should have received a copy of the GNU General Public License along with this program. *
# If not, see <http://www.gnu.org/licenses/>.                                                *
#*********************************************************************************************

"""
This module contains everything related to compression.

The basic idea of employing compression in the translated queries is to select
one of the compression configurations defined in this module. A compression
configuration assigns the input and output formats of all operators in a
translated query program. At the moment, this is done using simple rule-based
strategies.

By convention, the name of each format attribute of an operator-object must end
with "F". See module mal2morphstore.operators.
"""


import mal2morphstore.operators as ops


# *****************************************************************************
# Names of the compression configurations.
# *****************************************************************************

# These names are used for the command line arguments.

CC_ALLUNCOMPR = "alluncompr"

# List of all compression configurations.
COMPR_CONFIGS = [
    CC_ALLUNCOMPR,
]


# *****************************************************************************
# Implementations of the compression configurations.
# *****************************************************************************

# Each of these functions takes an instance of
# mal2morphstore.translation.TranslationResult as input and must set all input
# and output formats of each operator call in the translated program.

# All base and intermediate columns are uncompressed.
def _configCompr_AllUncompr(tr):
    # TODO Remove the first header, but currently, uncompr_f is defined there.
    tr.headers.add("core/morphing/format.h")
    tr.headers.add("core/morphing/uncompr.h")
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    el.__dict__[key] = "uncompr_f"


# *****************************************************************************
# Functions to be used from outside.
# *****************************************************************************

# Sets all formats in the given translated program according to the specified
# compression configuration. This function takes an instance of
# mal2morphstore.translation.TranslationResult and the name of a compression
# configuration and delegates the control flow to the function of the selected
# compression configuration.
def configCompr(tr, comprConfig):
    if comprConfig == CC_ALLUNCOMPR:
        _configCompr_AllUncompr(tr)
    else:
        raise RuntimeError(
            "Unsupported compression configuration: '{}'".format(baseCompr)
        )

# Checks if all input and output formats of all operators in the given
# translated query have been set and raises an error otherwise.
def checkAllFormatsSet(tr):
    opIdx = 0
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    if el.__dict__[key] is None:
                        raise RuntimeError(
                            "The format '{}' of operator '{}' (number {} in "
                            "the translated query program) has not been "
                            "set".format(key, el.opName, opIdx)
                        )
            opIdx += 1