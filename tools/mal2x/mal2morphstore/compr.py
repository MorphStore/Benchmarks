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
# See the GNU General Public License for more details.                                       *
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

import sys
# TODO This is relative to ssb.sh.
sys.path.append("../../LC-BaSe/cm")
import algo
import data
import est
sys.path.append(".")
import csvutils

import pandas as pd

import math
import os
from functools import partial


# *****************************************************************************
# Constants
# *****************************************************************************

# -----------------------------------------------------------------------------
# Compression strategies
# -----------------------------------------------------------------------------

CS_UNCOMPR = "uncompr"
CS_RULEBASED = "rulebased"
CS_COSTBASED = "costbased"

COMPR_STRATEGIES = [
    CS_UNCOMPR,
    CS_RULEBASED,
    CS_COSTBASED,
]

# -----------------------------------------------------------------------------
# Block size of cascades
# -----------------------------------------------------------------------------
# There is only one global block size for the entire program, since we do not
# consider using different cascade block sizes for different columns. Note that
# this default can be set by a command line argument.

CASC_BLOCKSIZE_LOG = 1024

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
FN_KWISENS = "k_wise_ns"
FN_DELTA = "delta"
FN_FOR = "for"

_CASC_SEP = "+"
def _makeCascName(logName, phyName):
    return "{}{}{}".format(logName, _CASC_SEP, phyName)

COMPR_FORMATS = [
    FN_UNCOMPR,
    FN_STATICVBP,
    FN_DYNAMICVBP,
    FN_KWISENS,
    _makeCascName(FN_DELTA, FN_DYNAMICVBP),
    _makeCascName(FN_FOR, FN_DYNAMICVBP),
    _makeCascName(FN_DELTA, FN_KWISENS),
    _makeCascName(FN_FOR, FN_KWISENS),
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
        elif fn == FN_KWISENS:
            blockSizeLog = pss.PS_INFOS[ps].vectorElementCount
            return "k_wise_ns_f<{}>".format(blockSizeLog)
        else:
            raise RuntimeError("unsupported format: '{}'".format(fn))
    else:
        outerName = fn[:pos]
        innerName = fn[pos+1:]
        # TODO reduce the code duplication here
        if outerName == FN_DELTA:
            step = pss.PS_INFOS[ps].vectorElementCount
            inner = _getMorphStoreFormatByName(innerName, ps, None)
            return "delta_f<{}, {}, {} >".format(CASC_BLOCKSIZE_LOG, step, inner)
        elif outerName == FN_FOR:
            step = pss.PS_INFOS[ps].vectorElementCount
            inner = _getMorphStoreFormatByName(innerName, ps, None)
            return "for_f<{}, {}, {} >".format(CASC_BLOCKSIZE_LOG, step, inner)
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
# TODO This only need to be a function since the cascade block size can be
# changed by a command line argument.
def initShortNames():
    for ps in [
        pss.PS_SCALAR,
        pss.PS_VEC128,
        pss.PS_VEC128_NEON,
        pss.PS_VEC256,
        pss.PS_VEC512,
    ]:
        _SHORT_NAMES[_getMorphStoreFormatByName(FN_DYNAMICVBP, ps, None)] = "d"
        _SHORT_NAMES[_getMorphStoreFormatByName(FN_KWISENS, ps, None)] = "k"
        for logName, logShort in [
            (FN_DELTA, "d"),
            (FN_FOR, "f"),
        ]:
            for phyName, phyShort in [
                (FN_DYNAMICVBP, "d"),
                (FN_KWISENS, "k"),
            ]:
                _SHORT_NAMES[_getMorphStoreFormatByName(
                    _makeCascName(logName, phyName), ps, None
                )] = logShort + phyShort
        for bw in range(1, 64 + 1):
            _SHORT_NAMES[_getMorphStoreFormatByName(
                FN_STATICVBP, ps, bw
            )] = "s{}".format(bw)
initShortNames()

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
    FN_KWISENS: ["core/morphing/k_wise_ns.h"],
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
def chooseUncompr(varNames):
    return pd.Series([_MS_UNCOMPR] * len(varNames), index=varNames)
                    
# Simple rule-based strategy.
def chooseRuleBased(
    dfColInfos, processingStyle, fnRndAcc, fnSeqAccUnsorted, fnSeqAccSorted
):
    def _decideFormat(rColInfo):
        if rColInfo[csvutils.ColInfoCols.hasRndAcc]:
            return _getMorphStoreFormatByName(
                    fnRndAcc,
                    processingStyle,
                    rColInfo[csvutils.ColInfoCols.maxBw]
            )
        elif rColInfo[data.COL_DA_SORTEDASC]:
            return _getMorphStoreFormatByName(
                    fnSeqAccSorted,
                    processingStyle,
                    rColInfo[csvutils.ColInfoCols.maxBw]
            )
        else:
            return _getMorphStoreFormatByName(
                    fnSeqAccUnsorted,
                    processingStyle,
                    rColInfo[csvutils.ColInfoCols.maxBw]
            )
            
    return dfColInfos.apply(_decideFormat, axis=1)

def _configureCostModel(processingStyle, profileDirPath):
    algo.CascadeAlgo._fsInternalNameFormat = "{log}+{phy}"
    
    data.MAX_BW = 64 # MorphStore is 64-bit.
    
    class BwProfCols:
        ve = "vector_extension"
        fmt = "format"
        bw = "bitwidth"
        count = "countValues"
        rtc = "runtime compr [µs]"
        rtd = "runtime decompr [µs]"
        sizeUsed = "size used [byte]"
        sizeCompr = "size compr [byte]"
        check = "check"
    dfBwProfs = pd.read_csv(
            os.path.join(profileDirPath, "bwprof.csv"),
            delimiter="\t",
            skiprows=2
    )
    
    #TODO This does not belong here.
    BITS_PER_BYTE = 8
    
    for cmInternal, msInternal in [
        ("dynamic_vbp", "dynamic_vbp_f<>"),
        ("static_vbp", "static_vbp_f<vbp_l<>>"),
        ("k_wise_ns", "k_wise_ns_f<>"), # Gets omitted if not SSE.
    ][:(None if processingStyle == "sse<v128<uint64_t>>" else -1)]:
        df = dfBwProfs[
            (dfBwProfs[BwProfCols.ve] == processingStyle) &
            (dfBwProfs[BwProfCols.fmt] == msInternal)
        ]
        df.index = range(1, data.MAX_BW + 1)
        sBwProf = df[BwProfCols.sizeUsed] / \
                (df[BwProfCols.count] * BITS_PER_BYTE) * data.MAX_BW
        est._bwProfs_Alone[algo.StandAloneAlgo(cmInternal, algo.MODE_FORMAT)] = \
                sBwProf
        
    est._constProfs_Alone[algo.StandAloneAlgo("delta", algo.MODE_FORMAT)] = \
            data.MAX_BW
    # TODO Take the reference values (meta data) into account. The precise
    # value depends on the cascade's block size.
    est._constProfs_Alone[algo.StandAloneAlgo("for", algo.MODE_FORMAT)] = \
            data.MAX_BW
    
    est._costFuncs[algo.StandAloneAlgo("dynamic_vbp")] = est._costPhy
    est._adaptFuncs[algo.StandAloneAlgo("dynamic_vbp")] = partial(
            est._adaptFixed,
            blockSize=pss.PS_INFOS[processingStyle].vectorSizeBit
    )
    est._costFuncs[algo.StandAloneAlgo("k_wise_ns")] = est._costPhy
    est._adaptFuncs[algo.StandAloneAlgo("k_wise_ns")] = est._adaptId
    est._costFuncs[algo.StandAloneAlgo("static_vbp")] = est._costPhy
    est._adaptFuncs[algo.StandAloneAlgo("static_vbp")] = est._adaptMax

    est._costFuncs[algo.StandAloneAlgo("delta")] = est._costLogConst
    est._changeFuncs[algo.StandAloneAlgo("delta")] = est._changeDC_Delta
    est._costFuncs[algo.StandAloneAlgo("for")] = est._costLogConst
    est._changeFuncs[algo.StandAloneAlgo("for")] = est._changeDC_For

# Our cost-based strategy.
def chooseCostBased(
    dfColInfos, processingStyle, choice, profileDirPath
):
    # TODO Don't reconfigure every time.
    _configureCostModel(processingStyle, profileDirPath)
    res = est.select(
            choice,
            partial(est.estimate, dfColInfos),
            algo.MODE_FORMAT,
            data.COL_F_COMPRRATE_BITSPERINT,
            True,
            math.inf,
            None
    )
    # Map algo.Algo objects to MorphStore C++ format names.
    return pd.DataFrame(
            {
                "algo": res[0],
                csvutils.ColInfoCols.maxBw: dfColInfos[csvutils.ColInfoCols.maxBw]
            }
    ).apply(
            lambda row: _getMorphStoreFormatByName(
                    row["algo"].getInternalName(),
                    processingStyle,
                    row[csvutils.ColInfoCols.maxBw]
            ),
            axis=1
    )
    
                    
# *****************************************************************************
# Utilities after the formats were chosen
# *****************************************************************************

def _insertFormats(tr, sFormats):
    for el in tr.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("F"):
                    varName = getattr(el, key[:-1] + "Col")
                    if varName not in sFormats:
                        raise RuntimeError(
                                "no format provided for column '{}'".format(
                                        varName
                                )
                        )
                    formatName = sFormats[varName]
                    el.__dict__[key] = formatName
                    #TODO This is ugly, maybe the strategies should return both
                    # the nice name and the MorphStore C++ identifier of the
                    # format.
                    _addHeaders(tr, formatName[:formatName.index("_f")])

# Checks if all input and output formats of all operators in the given
# translated query have been set and raises an error otherwise.
def _checkAllFormatsSet(translationResult):
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
            
# Inserts morph-operators before each operator, if necessary to ensure that its
# input columns are available in the expected format.
def _insertMorphs(translationResult):
    def _ensureAvailable(varName, formatName):
        # If there is no column variable for this column in the given format
        # yet, then we add a full-column morph.
        if formatName not in actualNames[varName]:
            newVarName = "{}__{}".format(
                varName.replace(".", "_"), _SHORT_NAMES[formatName]
            )
            newProg.append(ops.Morph(newVarName, varName, formatName))
            actualNames[varName][formatName] = newVarName
    
    # The new query program and result column names.
    newProg = []
    newResultCols = []

    # A mapping from original column variable names to a mapping from
    # MorphStore format names to original or newly introduced column variable
    # names.
    actualNames = {}
    for tblName in translationResult.colNamesByTblName:
        for colName in translationResult.colNamesByTblName[tblName]:
            varName = "{}.{}".format(tblName, colName)
            actualNames[varName] = {_MS_UNCOMPR: varName}
    
    # Add full-column morphs for the operators' input columns, if necessary.
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            for key in el.__dict__:
                if key.endswith("Col"):
                    if isinstance(el, ops.SumGrBased) and key == "inExtCol":
                        # This special case must be skipped, since the
                        # group-based agg_sum-operator does not access the data
                        # of its parameter inExtCol, but only needs its number
                        # of data elements. Thus, no special format is required
                        # for this input column.
                        continue
                    varName = getattr(el, key)
                    configuredFormat = getattr(
                        el, "{}F".format(key[:-len("Col")])
                    )
                    if key.startswith("in"): # Input columns
                        _ensureAvailable(varName, configuredFormat)
                        setattr(el, key, actualNames[varName][configuredFormat])
                    else: # Output columns
                        # Remember in which format this column was created.
                        actualNames[varName] = {configuredFormat: varName}
        newProg.append(el)
    
    
    # Morphs for the query's result columns.
    for varName in translationResult.resultCols:
        _ensureAvailable(varName, _MS_UNCOMPR)
        newResultCols.append(actualNames[varName][_MS_UNCOMPR])
        
    # Overwriting the old program and result column names with the new ones.
    translationResult.prog = newProg
    translationResult.resultCols = newResultCols
    
def _reorderMorphs(translationResult):
    """
    Separates the morphs of the base and result columns (which might have been
    inserted by insertMorphs()) from the rest of the query program.
    """
    
    newProg = []
    baseMorphs = []
    resultMorphs = []
    
    for el in translationResult.prog:
        if isinstance(el, ops.Morph):
            if "." in el.inCol:
                # The input is a base column.
                baseMorphs.append(el)
            elif el.outCol in translationResult.resultCols:
                # The output is a result column.
                resultMorphs.append(el)
            else:
                # Input and output are intermediate results.
                newProg.append(el)
        else:
            newProg.append(el)
            
    translationResult.prog = newProg
    translationResult.baseMorphs = baseMorphs
    translationResult.resultMorphs = resultMorphs
    
    
# *****************************************************************************
# Top-level functions
# *****************************************************************************

def choose(
    dfColInfos, processingStyle,
    strategy, uncomprBase, uncomprInterm,
    fnRndAcc, fnSeqAccUnsorted, fnSeqAccSorted,
    profileDirPath,
):
    """
    Chooses a (un)compressed format for each base column or intermediate result
    represented by a row in the given `pandas.DataFrame`.
    
    Note that this function can be used completely independent from the query
    translation.
    """
    
    if strategy == CS_UNCOMPR:
        sFormats = chooseUncompr(dfColInfos.index)
    else:
        sMustBeUncompr = \
            dfColInfos[csvutils.ColInfoCols.isForcedUncompr] | \
            dfColInfos[csvutils.ColInfoCols.isResult] | \
            (
                dfColInfos[csvutils.ColInfoCols.isBaseCol] &
                pd.Series(uncomprBase, index=dfColInfos.index)
            ) | \
            (
                ~dfColInfos[csvutils.ColInfoCols.isBaseCol] &
                pd.Series(uncomprInterm, index=dfColInfos.index)
            )
        dfColInfosCompr = dfColInfos[~sMustBeUncompr]
        if len(dfColInfosCompr):
            if strategy == CS_RULEBASED:
                sFormats = chooseRuleBased(
                    dfColInfosCompr,
                    processingStyle,
                    fnRndAcc,
                    fnSeqAccUnsorted,
                    fnSeqAccSorted,
                )
            elif strategy == CS_COSTBASED:
                sHasRndAcc = dfColInfosCompr[csvutils.ColInfoCols.hasRndAcc]
                dfColInfosComprHasRndAcc = dfColInfosCompr[sHasRndAcc]
                dfColInfosComprHasNoRndAcc = dfColInfosCompr[~sHasRndAcc]
                sFormats = pd.Series()
                if len(dfColInfosComprHasRndAcc):
                    sFormats = sFormats.append(chooseCostBased(
                            dfColInfosComprHasRndAcc,
                            processingStyle,
                            [
                                # TODO Uncompressed should be an option, too.
                                algo.StandAloneAlgo(FN_STATICVBP),
                            ],
                            profileDirPath
                    ))
                if len(dfColInfosComprHasNoRndAcc):
                    sFormats = sFormats.append(chooseCostBased(
                            dfColInfosComprHasNoRndAcc,
                            processingStyle,
                            [
                                # TODO Uncompressed should be an option, too.
                                algo.StandAloneAlgo(FN_DYNAMICVBP),
                                algo.StandAloneAlgo(FN_STATICVBP),
                                algo.CascadeAlgo(FN_DELTA, FN_DYNAMICVBP, 123),
                                algo.CascadeAlgo(FN_FOR, FN_DYNAMICVBP, 123),
                                # The following are ignored if the processing
                                # style is not SSE.
                                algo.StandAloneAlgo(FN_KWISENS),
                                algo.CascadeAlgo(FN_DELTA, FN_KWISENS, 123),
                                algo.CascadeAlgo(FN_FOR, FN_KWISENS, 123),
                            ][:(
                                None
                                if processingStyle == "sse<v128<uint64_t>>"
                                else -3
                            )],
                            profileDirPath
                    ))
            else:
                raise RuntimeError(
                    "Unsupported compression strategy: '{}'".format(strategy)
                )
            sFormats = sFormats.append(
                chooseUncompr(sMustBeUncompr[sMustBeUncompr].index),
                verify_integrity=True
            )
        else:
            sFormats = chooseUncompr(dfColInfos.index)
    
    return sFormats
            
def configureProgram(
    translationResult, colInfosFilePath, processingStyle,
    strategy, uncomprBase, uncomprInterm,
    fnRndAcc, fnSeqAccUnsorted, fnSeqAccSorted,
    profileDirPath,
):
    """
    Modifies the given translated query program to use compression in the way
    specified by the other parameters.
    """
    
    # Choose the (un)compressed format for each base column and intermediate.
    if strategy == CS_UNCOMPR:
        # Uncompressed processing is possible without the CSV file containing
        # information on the columns. This is important since that CSV file is
        # created by an uncompressed execution.
        varNames = []
        for el in translationResult.prog:
            if isinstance(el, ops.Op):
                for key in el.__dict__:
                    if key.endswith("Col"):
                        varName = getattr(el, key)
                        if varName not in varNames:
                            varNames.append(varName)
        sFormats = chooseUncompr(varNames)
    else:
        sFormats = choose(
            csvutils.getColInfos(colInfosFilePath),
            processingStyle,
            strategy, uncomprBase, uncomprInterm,
            fnRndAcc, fnSeqAccUnsorted, fnSeqAccSorted,
            profileDirPath,
        )
        
    # Insert the formats into the query program.
    _insertFormats(translationResult, sFormats)
    _checkAllFormatsSet(translationResult)
    
    # Insert full-column morph-operators if and where necessary.
    if strategy != CS_UNCOMPR:
        _insertMorphs(translationResult)
        
    # Move full-column morph-operators of base and result columns out of the
    # query program.
    _reorderMorphs(translationResult)