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
This module contains everything related to assigning (un)compressed formats to
columns.

The basic idea of employing compression in the translated queries is to select
one of the compression strategies defined in this module. A compression
strategy assigns the input and output formats of all operators in a translated
query program. At the moment, this is done using simple rule-based strategies.

By convention, the name of each format attribute of an operator-object must end
with "F". See module mal2morphstore.operators.
"""


import mal2morphstore.analysis as analysis
import mal2morphstore.formats as formats
import mal2morphstore.operators as ops
import mal2morphstore.processingstyles as pss

import sys

# TODO This is relative to ssb.sh.
sys.path.append("../../LC-BaSe")
import lcbase_py.algo as algo
import lcbase_py.costmodel as cm
import lcbase_py.whitebox as wb

# Configure the cost model to assume 64 bit for uncompressed data elements.
# This must be done before everything else, e.g., before importing csvutils.
cm.UNCOMPR_BW = 64

# TODO This is relative to ssb.sh.
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
CS_REALBEST = "realbest"
CS_REALWORST = "realworst"
CS_MANUAL = "manual"

COMPR_STRATEGIES = [
    CS_UNCOMPR,
    CS_RULEBASED,
    CS_COSTBASED,
    CS_REALBEST,
    CS_REALWORST,
    CS_MANUAL,
]

# -----------------------------------------------------------------------------
# Optimization objectives
# -----------------------------------------------------------------------------

OBJ_MEM = "mem"
OBJ_PERF = "perf"

OBJECTIVES = [OBJ_MEM, OBJ_PERF]


# *****************************************************************************
# Compression strategies
# *****************************************************************************

def _setStaticBitwidth(sFormat, dfColInfos):
    def setBw(fmt, bw):
        """
        Explicitly sets the bit width of the given format, if necessary.
        
        If the given format is not Static Bit-Packing, it is returned as it is.
        If the given format is Static Bit-Packing, then the bit width is set
        only if the bit width has not been explicitly set to a number before.
        Otherwise, the bit width is set depending on the placeholder.
        """
        if isinstance(fmt, formats.StaticVBPFormat):
            if fmt._bw is None or fmt._bw == formats.StaticVBPFormat.SUFFIX_BIT:
                return fmt.changeBw(bw)
            if fmt._bw == formats.StaticVBPFormat.SUFFIX_EVEN:
                return fmt.changeBw(bw + bw % 2)
            if fmt._bw == formats.StaticVBPFormat.SUFFIX_BYTE:
                return fmt.changeBw(bw + 8 - bw % 8)
            if fmt._bw == formats.StaticVBPFormat.SUFFIX_POWEROFTWO:
                return fmt.changeBw(2 ** int(bw - 1).bit_length())
        return fmt
    return sFormat.combine(dfColInfos[csvutils.ColInfoCols.maxBw], setBw)

# All base and intermediate columns are uncompressed.
def chooseUncompr(varNames):
    return pd.Series([formats.UncomprFormat()] * len(varNames), index=varNames)
                    
# Simple rule-based strategy.
def chooseRuleBased(dfColInfos, fRndAccUnsorted, fRndAccSorted, fSeqAccUnsorted, fSeqAccSorted):
    def _decideFormat(rColInfo):
        # TODO Think about which case should determine the format if the column
        # requires random access with both sorted and unsorted positions.
        # However, in SSB this situation does not occur.
        if rColInfo[csvutils.ColInfoCols.hasRndAccUnsorted]:
            return fRndAccUnsorted
        elif rColInfo[csvutils.ColInfoCols.hasRndAccSorted]:
            return fRndAccSorted
        elif rColInfo[cm.ColsDC.isSorted]:
            return fSeqAccSorted
        else:
            return fSeqAccUnsorted
            
    return _setStaticBitwidth(
            dfColInfos.apply(_decideFormat, axis=1),
            dfColInfos
    )

def _configureCostModel(ps, profileDirPath):
    #TODO This does not belong here.
    BITS_PER_BYTE = 8
    
    # -------------------------------------------------------------------------
    # Column names in the CSV files containing the profiles
    # -------------------------------------------------------------------------
    
    class GeneralCols:
        ve = "vector_extension"
        fmt = "format"
        rtc = "runtime compr [µs]"
        rtd = "runtime decompr [µs]"
        rta = "runtime agg [µs]"
        check = "check"
        rep = "repetition"
    class BwProfCols:
        bw = "bitwidth"
    class BwProfAloneCols:
        sizeUsed = "size used [byte]"
        sizeCompr = "size compr [byte]"
        count = "countValues"
    
    # -------------------------------------------------------------------------
    # Loading the CSV files containing the profiles
    # -------------------------------------------------------------------------

    # Has GeneralCols + BwProfCols + BwProfColsAlone
    readCsvParams = dict(sep="\t", skiprows=2)
    dfBwProfsAlone = pd.read_csv(
            os.path.join(profileDirPath, "bw_prof_alone.csv"),
            **readCsvParams
    )
    # Has GeneralCols + BwProfCols
    dfBwProfsCasc = pd.read_csv(
            os.path.join(profileDirPath, "bw_prof_casc.csv"),
            **readCsvParams
    )
    # Has GeneralCols
    dfConstProfsCasc = pd.read_csv(
            os.path.join(profileDirPath, "const_prof_casc.csv"),
            **readCsvParams
    )
    # Has GeneralCols
    dfUncomprProfs = pd.read_csv(
            os.path.join(profileDirPath, "uncompr.csv"),
            **readCsvParams
    )
    
    if any(dfBwProfsAlone[GeneralCols.check] == 0):
        raise RuntimeError(
                "some check in the calibration of stand-alone bit width "
                "profiles failed"
        )
    if any(dfBwProfsCasc[GeneralCols.check] == 0):
        raise RuntimeError(
                "some check in the calibration of cascade bit width "
                "profiles failed"
        )
    # The checks for cascade constant profiles cannot be checked, because they
    # are all false-positives by construction.
    
    # -------------------------------------------------------------------------
    # Averaging of repeated calibration measurements
    # -------------------------------------------------------------------------
    
    # Drop unnecessary columns.
    for df in [dfBwProfsAlone, dfBwProfsCasc, dfConstProfsCasc, dfUncomprProfs]:
        df.drop(columns=[GeneralCols.rep, GeneralCols.check], inplace=True)
    
    # Calculate the mean of repeated measurements.
    dfBwProfsAlone = dfBwProfsAlone.groupby(
            [GeneralCols.ve, GeneralCols.fmt, BwProfCols.bw], as_index=False
    ).mean()
    dfBwProfsCasc = dfBwProfsCasc.groupby(
            [GeneralCols.ve, GeneralCols.fmt, BwProfCols.bw], as_index=False
    ).mean()
    dfConstProfsCasc = dfConstProfsCasc.groupby(
            [GeneralCols.ve, GeneralCols.fmt], as_index=False
    ).mean()
    dfUncomprProfs = dfUncomprProfs.groupby(
            GeneralCols.ve, as_index=False
    ).mean()
    
    # -------------------------------------------------------------------------
    # Creation of a new cost model
    # -------------------------------------------------------------------------
    
    costModel = cm.CostModel()
    # TODO Don't hardcode this. Obtain it from the calibration CSV files and
    # make sure that it was the same for all calibration benchmarks.
    costModel.countValuesCalib = 128 * 1024 * 1024
    
    # -------------------------------------------------------------------------
    # Cost model configuration for the uncompressed format
    # -------------------------------------------------------------------------
    
    fmt = formats.UncomprFormat()
    
    # --------------
    # White-box part
    # --------------
 
    costModel.costTypes[fmt] = cm.COSTTYPE_LOG_DI
    
    # --------------
    # Black-box part
    # --------------
    
    # Compression rate.
    costModel.diProfs[cm.CONTEXT_STAND_ALONE][fmt.changeMode(algo.MODE_FORMAT)] = cm.UNCOMPR_BW
    
    # Runtimes.
    dfUncomprProfs_Ps = dfUncomprProfs[dfUncomprProfs[GeneralCols.ve] == ps]
    if not len(dfUncomprProfs_Ps):
        raise RuntimeError(
                "there seem to be no calibration measurements for "
                "uncompressed data for processing style '{}'".format(ps)
        )
    costModel.diProfs[cm.CONTEXT_STAND_ALONE][fmt.changeMode(algo.MODE_COMPR)] = \
            dfUncomprProfs_Ps["runtime reg2ram [µs]"].values[0]
    costModel.diProfs[cm.CONTEXT_STAND_ALONE][fmt.changeMode(algo.MODE_DECOMPR)] = \
            dfUncomprProfs_Ps["runtime agg [µs]"].values[0]
    
    # -------------------------------------------------------------------------
    # Cost model configuration for physical-level algorithms
    # -------------------------------------------------------------------------
    
    # TODO Always check if the obtained profile has the expected size (e.g., is
    # not empty).
    
    phyInfo = [
        (
            formats.StaticVBPFormat(ps, granularity),
            partial(wb.adaptMax, granularity=granularity),
            pss.PS_INFOS[ps].vectorSizeBit,
            False,
        )
        for granularity in ["bit", "even", "byte", "pot"]
    ] + [
        (
            formats.StaticVBPFormat(ps, None),
            partial(wb.adaptMax, granularity="bit"),
            pss.PS_INFOS[ps].vectorSizeBit,
            False,
        ),
        (
            formats.DynamicVBPFormat(ps),
            partial(
                    wb.adaptFixed,
                    blockSize=pss.PS_INFOS[ps].vectorSizeBit
            ),
            pss.PS_INFOS[ps].vectorSizeBit,
            True,
        ),
    ]
    if ps == pss.PS_VEC128:
        phyInfo.append((
            formats.KWiseNSFormat(ps),
            wb.adaptId,
            pss.PS_INFOS[ps].vectorElementCount,
            True,
        ))
        
    for fmt, adaptFunc, comprGran, supportCasc in phyInfo:
        # --------------
        # White-box part
        # --------------
        
        costModel.costTypes[fmt] = cm.COSTTYPE_PHY
        costModel.adaptFuncs[fmt] = adaptFunc
        # The mixture/penalty is zero for all Null Suppression algorithms we
        # have so far in MorphStore.
        costModel.mixtureFuncs[fmt] = wb.mixtureZero
        
        # --------------
        # Black-box part
        # --------------
        
        if isinstance(fmt, formats.StaticVBPFormat):
            internalName = fmt.changeBw(None).getInternalName()
        else:
            internalName = fmt.getInternalName()
        dfBwProfsAlone_PsFmt = dfBwProfsAlone[
            (dfBwProfsAlone[GeneralCols.ve] == ps) &
            (dfBwProfsAlone[GeneralCols.fmt] == internalName)
        ]
        dfBwProfsAlone_PsFmt.index = dfBwProfsAlone_PsFmt[BwProfCols.bw]
        dfBwProfsCasc_PsFmt = dfBwProfsCasc[
            (dfBwProfsCasc[GeneralCols.ve] == ps) &
            (dfBwProfsCasc[GeneralCols.fmt] == internalName)
        ]
        dfBwProfsCasc_PsFmt.index = dfBwProfsCasc_PsFmt[BwProfCols.bw]
        
        # Compression rate.
        
        costModel.bwProfs[cm.CONTEXT_STAND_ALONE][fmt.changeMode(algo.MODE_FORMAT)] = \
                dfBwProfsAlone_PsFmt[BwProfAloneCols.sizeUsed] / \
                dfBwProfsAlone_PsFmt[BwProfAloneCols.count] * BITS_PER_BYTE
        
        # Runtimes and penalty factors.
        # The penalty factors are 0 for all Null Suppression algorithms we
        # have so far in MorphStore.

        fmtCompr = fmt.changeMode(algo.MODE_COMPR)
        costModel.bwProfs[cm.CONTEXT_STAND_ALONE][fmtCompr] = dfBwProfsCasc_PsFmt[GeneralCols.rtc] # cache -> RAM
        costModel.penaltyFactors[cm.CONTEXT_STAND_ALONE][fmtCompr] = 0
        if supportCasc:
            costModel.bwProfs[cm.CONTEXT_IN_CASC][fmtCompr] = dfBwProfsCasc_PsFmt[GeneralCols.rtc] # cache -> RAM
            costModel.penaltyFactors[cm.CONTEXT_IN_CASC][fmtCompr] = 0

        fmtDecompr = fmt.changeMode(algo.MODE_DECOMPR)
        costModel.bwProfs[cm.CONTEXT_STAND_ALONE][fmtDecompr] = dfBwProfsAlone_PsFmt[GeneralCols.rta] # RAM -> reg
        costModel.penaltyFactors[cm.CONTEXT_STAND_ALONE][fmtDecompr] = 0
        if supportCasc:
            costModel.bwProfs[cm.CONTEXT_IN_CASC][fmtDecompr] = dfBwProfsCasc_PsFmt[GeneralCols.rtd] # RAM -> cache
            costModel.penaltyFactors[cm.CONTEXT_IN_CASC][fmtDecompr] = 0

    # -------------------------------------------------------------------------
    # Cost model configuration for logical-level algorithms
    # -------------------------------------------------------------------------
    
    logInfos = [
        (
            formats._DeltaFormat(ps),
            wb.changeDelta,
            None, # TODO
        ),
        (
            formats._ForFormat(ps),
            wb.changeFor,
            None, # TODO
        ),
    ]
    
    for fmt, changeFunc, comprGran in logInfos:
        # --------------
        # White-box part
        # --------------
        
        costModel.costTypes[fmt] = cm.COSTTYPE_LOG_DI
        costModel.changeFuncs[fmt] = changeFunc
        
        # --------------
        # Black-box part
        # --------------
        
        dfConstProfsCasc_PsFmt = dfConstProfsCasc[
            (dfConstProfsCasc[GeneralCols.ve] == ps) &
            (dfConstProfsCasc[GeneralCols.fmt] == fmt.getInternalName())
        ]
        
        # Compression rate.
        
        # TODO This is inaccurate for FOR.
        costModel.diProfs[cm.CONTEXT_STAND_ALONE][fmt.changeMode(algo.MODE_FORMAT)] = cm.UNCOMPR_BW
        
        # Runtimes.
        
        costModel.diProfs[cm.CONTEXT_IN_CASC][fmt.changeMode(algo.MODE_COMPR)] = \
                dfConstProfsCasc_PsFmt["runtime compr cache2cache [µs]"].values[0] # cache -> cache
        costModel.diProfs[cm.CONTEXT_IN_CASC][fmt.changeMode(algo.MODE_DECOMPR)] \
                = dfConstProfsCasc_PsFmt[GeneralCols.rta].values[0] # cache -> reg
                    
    return costModel

def _chooseFuncBased(costFunc, minimize, dfColInfos, choice):
    res = cm.select(
            choice,
            costFunc,
            minimize,
            math.inf,
            None
    )["algo"]
    res = _setStaticBitwidth(res, dfColInfos)
    return res

# Our cost-based strategy.
def chooseCostBased(
    objective, dfColInfos, choice, processingStyle, profileDirPath
):
    # TODO Don't reconfigure every time.
    algoCostModel = _configureCostModel(processingStyle, profileDirPath)
    if objective == OBJ_MEM:
        func = partial(algoCostModel.cost, dfDC=dfColInfos)
    elif objective == OBJ_PERF:
        costModel = CostModel(algoCostModel)
        func = partial(costModel.cost, dfColInfos=dfColInfos)
    else:
        raise RuntimeError(
                "unsupported objective for cost-based format selection: "
                "'{}'".format(objective)
        )
    return _chooseFuncBased(func, True, dfColInfos, choice)

def _measure(dfMea, al):
    if al._mode == algo.MODE_FORMAT:
        haystackFormatCol = csvutils.SizesCols.formatWithoutBw
        sMeaForAlgo = dfMea[
                dfMea[haystackFormatCol] == al.getInternalName()
        ][csvutils.SizesCols.sizeUsedByte]
        if len(sMeaForAlgo) == 0:
            raise RuntimeError(
                    "could not retrieve measured column sizes for format "
                    "'{}', provided measurements contain only the following "
                    "formats: [{}]".format(
                            al.getInternalName(),
                            ", ".join(map(
                                    lambda x: "'{}'".format(x),
                                    dfMea[haystackFormatCol].unique()
                            ))
                    )
            )
        return sMeaForAlgo
    elif al._mode is None:
        raise RuntimeError("the algorithm's mode is None")
    else:
        raise NotImplemented()

def chooseRealBased(
    objective, dfColInfos, choice, sizesFilePath, minimize=True
):
    if objective == OBJ_MEM:
        func = partial(_measure, csvutils.getSizes(sizesFilePath, dfColInfos.index))
    else:
        raise RuntimeError(
                "unsupported objective for real best/worst format selection: "
                "'{}'".format(objective)
        )
    return _chooseFuncBased(func, minimize, dfColInfos, choice).reindex(
            dfColInfos.index
    )

def chooseManual(processingStyle, configFilePath, dfColInfos):
    return _setStaticBitwidth(
            pd.read_csv(
                    configFilePath,
                    sep="\t",
                    index_col=0,
                    squeeze=True,
                    converters={
                        "format": partial(formats.byName, ps=processingStyle)
                    }
            ).reindex(dfColInfos.index),
            dfColInfos
    )
    
                    
# *****************************************************************************
# Utilities after the formats were chosen
# *****************************************************************************

def _addHeaders(tr, fmt):
    if isinstance(fmt, algo.StandAloneAlgo):
        for header in fmt.headers:
            tr.headers.add(header)
    elif isinstance(fmt, algo.CascadeAlgo):
        _addHeaders(tr, fmt.getLogAlgo())
        _addHeaders(tr, fmt.getPhyAlgo())
    else:
        raise RuntimeError("unsupported format: '{}'".format(fmt))

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
                    fmt = sFormats[varName]
                    el.__dict__[key] = fmt.getInternalName()
                    _addHeaders(tr, fmt)

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
            newVarName = "{}_m{}".format(
                varName.replace(".", "_"), len(actualNames[varName])
            )
            newProg.append(ops.Morph(newVarName, varName, formatName))
            actualNames[varName][formatName] = newVarName
    
    # The new query program and result column names.
    newProg = []
    newResultCols = []

    # A mapping from original column variable names to a mapping from
    # MorphStore format names to original or newly introduced column variable
    # names.
    nameUncompr = formats.UncomprFormat().getInternalName()
    actualNames = {}
    for tblName in translationResult.colNamesByTblName:
        for colName in translationResult.colNamesByTblName[tblName]:
            varName = "{}.{}".format(tblName, colName)
            actualNames[varName] = {nameUncompr: varName}
    
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
        _ensureAvailable(varName, nameUncompr)
        newResultCols.append(actualNames[varName][nameUncompr])
        
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
    # general compression parameters
    strategy, objective, uncomprBase=False, uncomprInterm=False,
    # parameter for rule-based strategy
    fnRndAccUnsorted=None, fnRndAccSorted=None, fnSeqAccUnsorted=None, fnSeqAccSorted=None,
    # parameters for cost-based strategy
    profileDirPath=None,
    # parameters for real best/worst w.r.t. memory footprint
    sizesFilePath=None,
    # parameters for the manual strategy
    configFilePath=None,
    # other
    allowHigherStaticBitWidth=False,
):
    """
    Chooses a (un)compressed format for each base column or intermediate result
    represented by a row in the given `pandas.DataFrame`.
    
    Note that this function can be used completely independent from the query
    translation, e.g., from a jupyter notebook. Therefore, it should support
    all compression strategies.
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
                    fnRndAccUnsorted,
                    fnRndAccSorted,
                    fnSeqAccUnsorted,
                    fnSeqAccSorted,
                )
            elif strategy == CS_MANUAL:
                sFormats = chooseManual(
                        processingStyle, configFilePath, dfColInfosCompr
                )
            elif strategy in [CS_COSTBASED, CS_REALBEST, CS_REALWORST]:
                if strategy == CS_COSTBASED:
                    chooseFunc = partial(
                            chooseCostBased,
                            processingStyle=processingStyle,
                            profileDirPath=profileDirPath
                    )
                else:
                    chooseFunc = partial(
                            chooseRealBased,
                            sizesFilePath=sizesFilePath,
                            minimize=strategy == CS_REALBEST
                    )
                
                sHasRndAcc = dfColInfosCompr[csvutils.ColInfoCols.hasRndAccUnsorted] | dfColInfosCompr[csvutils.ColInfoCols.hasRndAccSorted]
                dfColInfosComprHasRndAcc = dfColInfosCompr[sHasRndAcc]
                dfColInfosComprHasNoRndAcc = dfColInfosCompr[~sHasRndAcc]
                sFormats = pd.Series()
                if len(dfColInfosComprHasRndAcc):
                    sFormats = chooseRuleBased(
                            dfColInfosComprHasRndAcc,
                            fnRndAccUnsorted, fnRndAccSorted,
                            None, None
                    )
                if len(dfColInfosComprHasNoRndAcc):
                    choice = [
                        formats.UncomprFormat(),
                    ]
                    if allowHigherStaticBitWidth:
                        choice.extend([
                            formats.StaticVBPFormat(processingStyle, "bit"),
                            formats.StaticVBPFormat(processingStyle, "even"),
                            formats.StaticVBPFormat(processingStyle, "byte"),
                            formats.StaticVBPFormat(processingStyle, "pot"),
                        ])
                    else:
                        choice.append(formats.StaticVBPFormat(processingStyle))
                    choice.extend([
                        formats.DynamicVBPFormat(processingStyle),
                        formats.DeltaCascFormat(
                                formats.CASC_BLOCKSIZE_LOG,
                                processingStyle,
                                formats.DynamicVBPFormat(processingStyle)
                        ),
                        formats.ForCascFormat(
                                formats.CASC_BLOCKSIZE_LOG,
                                processingStyle,
                                formats.DynamicVBPFormat(processingStyle)
                        ),
                    ])
                    if processingStyle == pss.PS_VEC128:
                        choice.extend([
                            formats.KWiseNSFormat(processingStyle),
                            formats.DeltaCascFormat(
                                    formats.CASC_BLOCKSIZE_LOG,
                                    processingStyle,
                                    formats.KWiseNSFormat(processingStyle)
                            ),
                            formats.ForCascFormat(
                                    formats.CASC_BLOCKSIZE_LOG,
                                    processingStyle,
                                    formats.KWiseNSFormat(processingStyle)
                            ),
                        ])
                    choice = [al.changeMode(algo.MODE_FORMAT) for al in choice]
                    sFormats = sFormats.append(chooseFunc(
                            objective, dfColInfosComprHasNoRndAcc, choice
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
    # general compression parameters
    strategy, objective, uncomprBase, uncomprInterm,
    # parameter for rule-based strategy
    fnRndAccUnsorted, fnRndAccSorted, fnSeqAccUnsorted, fnSeqAccSorted,
    # parameters for cost-based strategy
    profileDirPath,
    # parameters for real best/worst w.r.t. memory footprint
    sizesFilePath,
    # parameters for the manual strategy
    configFilePath,
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
            csvutils.getColInfos(colInfosFilePath), processingStyle,
            # general compression parameters
            strategy, objective, uncomprBase, uncomprInterm,
            # parameter for rule-based strategy
            fnRndAccUnsorted, fnRndAccSorted, fnSeqAccUnsorted, fnSeqAccSorted,
            # parameters for cost-based strategy
            profileDirPath,
            # parameters for real best/worst w.r.t. memory footprint
            sizesFilePath,
            # parameters for the manual strategy
            configFilePath,
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
    
class CostModel:
    def __init__(self, algoCostModel):
        self.algoCostModel = algoCostModel
        
    def cost(self, fmt, dfColInfos):
        fmtCompr   = fmt.changeMode(algo.MODE_COMPR)
        fmtDecompr = fmt.changeMode(algo.MODE_DECOMPR)
        sCostCompr   = self.algoCostModel.cost(fmtCompr  , dfColInfos)
        sCostDecompr = self.algoCostModel.cost(fmtDecompr, dfColInfos)
        # Base columns are never written (during the query execution).
        sCountCompr = (~dfColInfos[csvutils.ColInfoCols.isBaseCol]).astype(int)
        sCountDecompr = dfColInfos[csvutils.ColInfoCols.countSeqAcc]
        return sCountCompr * sCostCompr + sCountDecompr * sCostDecompr