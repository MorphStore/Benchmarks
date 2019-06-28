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
import mal2morphstore.processingstyles as pss


# *****************************************************************************
# Names of the compression configurations.
# *****************************************************************************

# These names are used for the command line arguments.

CC_ALLUNCOMPR = "alluncompr"
CC_ALLDYNAMICVBP = "alldynamicvbp"

# List of all compression configurations.
COMPR_CONFIGS = [
    CC_ALLUNCOMPR,
    CC_ALLDYNAMICVBP,
]


# *****************************************************************************
# Format names and utilities.
# *****************************************************************************

FORMAT_UNCOMPR = "uncompr_f"

def makeDynamicVBP(ps):
    if ps == pss.PS_SCALAR:
        blockSizeLog = 64
        pageSizeBlocks = 8
        step = 1
    elif ps == pss.PS_VEC128:
        blockSizeLog = 128
        pageSizeBlocks = 16
        step = 2
    elif ps == pss.PS_VEC256:
        blockSizeLog = 256
        pageSizeBlocks = 32
        step = 4
    elif ps == pss.PS_VEC512:
        blockSizeLog = 512
        pageSizeBlocks = 32
        step = 8
    return "dynamic_vbp_f<{}, {}, {}>".format(
        blockSizeLog, pageSizeBlocks, step
    )

shortNames = {
    FORMAT_UNCOMPR: "u",
    # We do not intend to use different vector extensions in one query at the
    # moment, so its ok if the short names are the same.
    makeDynamicVBP(pss.PS_SCALAR): "d",
    makeDynamicVBP(pss.PS_VEC128): "d",
    makeDynamicVBP(pss.PS_VEC256): "d",
    makeDynamicVBP(pss.PS_VEC512): "d",
}


# *****************************************************************************
# Implementations of the compression configurations.
# *****************************************************************************

# Each of these functions takes an instance of
# mal2morphstore.translation.TranslationResult as input and must set all input
# and output formats of each operator call in the translated program.

# All base and intermediate columns are uncompressed.
def _configCompr_AllUncompr(tr, ps):
    # TODO Remove the first header, but currently, uncompr_f is defined there.
    tr.headers.add("core/morphing/format.h")
    tr.headers.add("core/morphing/uncompr.h")
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    el.__dict__[key] = FORMAT_UNCOMPR
                    
# All base and intermediate columns are represented in the format
# dynamic_vbp_f.
# TODO Since not all of MorphStore's query operators support this yet, we use
#      dynamic_vbp_f where it is supported and otherwise use uncompr_f
def _configCompr_AllDynamicVBP(tr, ps):
    # Set all formats to uncompr_f
    # TODO This should not be required.
    _configCompr_AllUncompr(tr, ps)
    
    tr.headers.add("core/morphing/dynamic_vbp.h")
    
    formatName = makeDynamicVBP(ps)
        
    # Set the formats to dynamic_vbp_f for all operators that support it.
    for el in tr.prog:
        if isinstance(el, ops.Select):
            for key in el.__dict__:
                if key.endswith("F"):
                    el.__dict__[key] = formatName


# *****************************************************************************
# Functions to be used from outside.
# *****************************************************************************

# Sets all formats in the given translated program according to the specified
# compression configuration. This function takes an instance of
# mal2morphstore.translation.TranslationResult and the name of a compression
# configuration and delegates the control flow to the function of the selected
# compression configuration.
def configCompr(translationResult, comprConfig, processingStyle):
    if comprConfig == CC_ALLUNCOMPR:
        _configCompr_AllUncompr(translationResult, processingStyle)
    elif comprConfig == CC_ALLDYNAMICVBP:
        _configCompr_AllDynamicVBP(translationResult, processingStyle)
    else:
        raise RuntimeError(
            "Unsupported compression configuration: '{}'".format(comprConfig)
        )

# Checks if all input and output formats of all operators in the given
# translated query have been set and raises an error otherwise.
def checkAllFormatsSet(translationResult):
    opIdx = 0
    for el in translationResult.prog:
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
            
# Inserts morph-operators before each operator to ensure that its input columns
# are available in the expected format.
# TODO Do not insert morphs with the same source and destination format to make
#      the C++ source code easier to read (it has no impact on the
#      performance, since such morphs are zero-cost).
def insertMorphs(translationResult):
    def makeNewVarName(varName, formatName):
        return "{}__{}".format(
            varName.replace(".", "_"), shortNames[formatName]
        )
    
    # The new query program and result column names.
    newProg = []
    newResultCols = []
    
    # Names of column variables newly introduced by morphs.
    newVarNames = []
    
    # Morphs for the operators' input columns.
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.startswith("in") and key.endswith("Col"):
                    if isinstance(el, ops.SumGrBased) and key == "inExtCol":
                        # This special case must be skipped, since the
                        # group-based agg_sum-operator does not access the data
                        # of its parameter inExtCol, but only needs its number
                        # of data elements. Thus, no special format is required
                        # for this input column.
                        continue
                    varName = getattr(el, key)
                    requiredFormat = getattr(
                        el, "{}F".format(key[:-len("Col")])
                    )
                    newVarName = makeNewVarName(varName, requiredFormat)
                    if newVarName not in newVarNames:
                        newProg.append(
                            ops.Morph(newVarName, varName, requiredFormat)
                        )
                        newVarNames.append(newVarName)
                    setattr(el, key, newVarName)
        newProg.append(el)
    
    
    # Morphs for the query's result columns.
    newProg.append("// Decompress the output columns, if necessary.")
    for varName in translationResult.resultCols:
        newVarName = makeNewVarName(varName, FORMAT_UNCOMPR)
        newProg.append(ops.Morph(newVarName, varName, FORMAT_UNCOMPR))
        newResultCols.append(newVarName)
        
    # Overwriting the old program and result column names with the new ones.
    translationResult.prog = newProg
    translationResult.resultCols = newResultCols