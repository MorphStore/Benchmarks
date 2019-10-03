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
one of the compression strategies defined in this module. A compression
strategy assigns the input and output formats of all operators in a translated
query program. At the moment, this is done using simple rule-based strategies.

By convention, the name of each format attribute of an operator-object must end
with "F". See module mal2morphstore.operators.
"""


import mal2morphstore.analysis as analysis
import mal2morphstore.operators as ops
import mal2morphstore.processingstyles as pss


# *****************************************************************************
# Constants
# *****************************************************************************

# -----------------------------------------------------------------------------
# Compression strategies
# -----------------------------------------------------------------------------

CS_UNCOMPR = "uncompr"
CS_RULEBASED = "rulebased"

COMPR_STRATEGIES = [
    CS_UNCOMPR,
    CS_RULEBASED,
]

# -----------------------------------------------------------------------------
# Format names
# -----------------------------------------------------------------------------
# We need to distinguish between the C++ identifiers (including template
# parameters) in the MorphStore code and the simpler format names in this
# script.

_MS_UNCOMPR = "uncompr_f"

FN_UNCOMPR = "uncompr"
FN_STATICVBP = "static_vbp"
FN_DYNAMICVBP = "dynamic_vbp"
FN_DELTA = "delta"
FN_FOR = "for"

_CASC_SEP = "/"
def _makeCascName(logName, phyName):
    return "{}/{}".format(logName, phyName)

COMPR_FORMATS = [
    FN_UNCOMPR,
    FN_STATICVBP,
    FN_DYNAMICVBP,
    _makeCascName(FN_DELTA, FN_DYNAMICVBP),
    _makeCascName(FN_FOR, FN_DYNAMICVBP),
]

def _getMorphStoreFormatByName(fn, ps, maxBw):
    pos = fn.find(_CASC_SEP)
    if pos == -1:
        if fn == FN_UNCOMPR:
            return _MS_UNCOMPR
        elif fn == FN_STATICVBP:
            if maxBw is None:
                raise RuntimeError(
                    "static bit-packing needs to know the maximum bit width"
                )
            step = pss.PS_INFOS[ps].vectorElementCount
            return "static_vbp_f<vbp_l<{}, {}> >".format(maxBw, step)
        elif fn == FN_DYNAMICVBP:
            blockSizeLog = pss.PS_INFOS[ps].vectorSizeBit
            pageSizeBlocks = pss.PS_INFOS[ps].vectorSizeByte
            step = pss.PS_INFOS[ps].vectorElementCount
            return "dynamic_vbp_f<{}, {}, {}>".format(
                blockSizeLog, pageSizeBlocks, step
            )
        else:
            raise RuntimeError("unsupported format: '{}'".format(fn))
    else:
        outerName = fn[:pos]
        innerName = fn[pos+1:]
        # TODO reduce the code duplication here
        if outerName == FN_DELTA:
            blockSizeLog = 1024 # TODO dont hardcode
            step = pss.PS_INFOS[ps].vectorElementCount
            inner = _getMorphStoreFormatByName(innerName, ps, None)
            return "delta_f<{}, {}, {} >".format(blockSizeLog, step, inner)
        elif outerName == FN_FOR:
            blockSizeLog = 1024 # TODO dont hardcode
            step = pss.PS_INFOS[ps].vectorElementCount
            inner = _getMorphStoreFormatByName(innerName, ps, None)
            return "for_f<{}, {}, {} >".format(blockSizeLog, step, inner)
        else:
            raise RuntimeError(
                "unsupported outer format of a cascade: '{}'".format(outerName)
            )
        
# -----------------------------------------------------------------------------
# Short names of the formats
# -----------------------------------------------------------------------------

_SHORT_NAMES = {
    _MS_UNCOMPR: "u",
}
# We do not intend to use different vector extensions in one query at the
# moment, so its ok if the short names are the same.
for ps in [
    pss.PS_SCALAR,
    pss.PS_VEC128,
    pss.PS_VEC128_NEON,
    pss.PS_VEC256,
    pss.PS_VEC512,
]:
    _SHORT_NAMES[_getMorphStoreFormatByName(FN_DYNAMICVBP, ps, None)] = "d"
    _SHORT_NAMES[_getMorphStoreFormatByName(
        _makeCascName(FN_DELTA, FN_DYNAMICVBP), ps, None
    )] = "dd"
    _SHORT_NAMES[_getMorphStoreFormatByName(
        _makeCascName(FN_FOR, FN_DYNAMICVBP), ps, None
    )] = "fd"
    for bw in range(1, 64 + 1):
        _SHORT_NAMES[_getMorphStoreFormatByName(
            FN_STATICVBP, ps, bw
        )] = "s{}".format(bw)

# -----------------------------------------------------------------------------
# C++ headers required for the formats
# -----------------------------------------------------------------------------

_HEADERS_BY_FN = {
    FN_UNCOMPR: [
        # TODO format.h is only required, sinec uncompr_f is still defined there.
        "core/morphing/format.h",
        "core/morphing/uncompr.h"
    ],
    FN_STATICVBP: [
        "core/morphing/static_vbp.h",
        "core/morphing/vbp.h"
    ],
    FN_DYNAMICVBP: ["core/morphing/dynamic_vbp.h"],
    FN_DELTA: ["core/morphing/delta.h"],
    FN_FOR: ["core/morphing/for.h"],
}

def _addHeaders(tr, fn):
    pos = fn.find(_CASC_SEP)
    if pos == -1:
        for header in _HEADERS_BY_FN[fn]:
            tr.headers.add(header)
    else:
        _addHeaders(tr, fn[:pos])
        _addHeaders(tr, fn[pos+1:])
        
        
# *****************************************************************************
# Compression strategies
# *****************************************************************************

# All base and intermediate columns are uncompressed.
def configureUncompr(tr):
    _addHeaders(tr, FN_UNCOMPR)
    
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    el.__dict__[key] = _MS_UNCOMPR
                    
# Simple rule-based strategy.
def configureRuleBased(tr, ps, fnRndAcc, fnSeqAccUnsorted, fnSeqAccSorted):
    _addHeaders(tr, FN_UNCOMPR)
    _addHeaders(tr, fnRndAcc)
    _addHeaders(tr, fnSeqAccUnsorted)
    _addHeaders(tr, fnSeqAccSorted)
    
    ar = analysis.analyze(tr, analyzeCardsAndBws=True)
    
    formatByCol = {}
    
    def _decideFormat(varName):
        if varName in ar.varsForcedUncompr:
            return _MS_UNCOMPR
        elif varName in ar.varsRndAccess:
            return _getMorphStoreFormatByName(
                    fnRndAcc, ps, ar.maxBwByCol[varName]
            )
        elif varName in ar.varsSorted:
            return _getMorphStoreFormatByName(
                    fnSeqAccSorted, ps, ar.maxBwByCol[varName]
            )
        else:
            return _getMorphStoreFormatByName(
                    fnSeqAccUnsorted, ps, ar.maxBwByCol[varName]
            )
    
    for tblName in tr.colNamesByTblName:
        for colName in tr.colNamesByTblName[tblName]:
            varName = "{}.{}".format(tblName, colName)
            formatByCol[varName] = _decideFormat(varName)
    
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    varName = getattr(el, key[:-1] + "Col")
                    if key.startswith("out"):
                        formatByCol[varName] = _decideFormat(varName)
                    el.__dict__[key] = formatByCol[varName]
                    
                    
# *****************************************************************************
# Utilities after the compression configuration
# *****************************************************************************

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
            varName.replace(".", "_"), _SHORT_NAMES[formatName]
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
        newVarName = makeNewVarName(varName, _MS_UNCOMPR)
        newProg.append(ops.Morph(newVarName, varName, _MS_UNCOMPR))
        newResultCols.append(newVarName)
        
    # Overwriting the old program and result column names with the new ones.
    translationResult.prog = newProg
    translationResult.resultCols = newResultCols