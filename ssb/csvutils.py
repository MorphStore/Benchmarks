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
# See the GNDo this in a proper way.U General Public License for more details.               *
#                                                                                            *
# You should have received a copy of the GNU General Public License along with this program. *
# If not, see <http://www.gnu.org/licenses/>.                                                *
#*********************************************************************************************

"""
Some utilities for working with the CSV files produced by SSB queries in
MorphStore.
"""


import sys
# TODO This is relative to ssb.sh.
sys.path.append("../../LC-BaSe/cm")
import data

import pandas as pd


# *****************************************************************************
# Reading CSV files
# *****************************************************************************

def _getSkip(filePath):
    with open(filePath, "r") as f:
        for lineIdx, line in enumerate(f):
            line = line.strip()
            if line == "[MEA]":
                lineIdxStart = lineIdx
            elif line == "[RES]":
                return lambda x: not (lineIdxStart < x < lineIdx)

def readMorphStoreCsv(filePath):
    return pd.read_csv(filePath, delimiter="\t", skiprows=_getSkip(filePath))


# *****************************************************************************
# Extracting column information
# *****************************************************************************
# In the format required by our cost model code.

# TODO The handling of the column name constants would be easier if all column
# names in the entire code were harmonized.
class ColInfoCols:
    """
    A collection of the column names used in the `pandas.DataFrame` returned by
    `getColInfos()`.
    """
    
    # Columns needed by our cost model for compression algorithms.
    bwHist          = data.COLS_DA_EFFBITSHIST
    countValues     = data.COL_DA_COUNTVALUES
    countDistinct   = data.COL_DA_COUNTDISTINCT
    min             = data.COL_DA_MIN
    max             = data.COL_DA_MAX
    isSorted        = data.COL_DA_SORTEDASC
    # More columns.
    maxBw           = "maxBw"
    isBaseCol       = "isBaseCol"
    isResult        = "isResult"
    hasRndAcc       = "hasRndAcc"
    isForcedUncompr = "isForcedUncompr"

def getColInfos(colInfosFilePath):
    dfIn = readMorphStoreCsv(colInfosFilePath)
    dfInDedup = dfIn.drop_duplicates("colName")
    
    df = dfInDedup[
        ["bwHist_{}".format(bw) for bw in range(1, data.MAX_BW + 1)] +
        ["valueCount", "Min", "Max", "DistinctCount"]
    ].copy()
    df.columns = ColInfoCols.bwHist + [
        ColInfoCols.countValues,
        ColInfoCols.min,
        ColInfoCols.max,
        ColInfoCols.countDistinct,
    ]
    df[ColInfoCols.isSorted]        = dfInDedup["Sorted"]          == 1
    df[ColInfoCols.isResult]        = dfInDedup["isResult"]        == 1
    df[ColInfoCols.hasRndAcc]       = dfInDedup["hasRndAccess"]    == 1
    df[ColInfoCols.isForcedUncompr] = dfInDedup["isForcedUncompr"] == 1
    
    df[ColInfoCols.maxBw] = df.apply(
        lambda row: data.MAX_BW - list(row[ColInfoCols.bwHist] > 0)[::-1].index(True),
        axis=1
    )
    df[ColInfoCols.isBaseCol] = dfInDedup["colName"].apply(
        lambda varName : "." in varName,
    )
    
    df.index = dfInDedup["colName"]
    return df


# *****************************************************************************
# Extracting size measurements
# *****************************************************************************

class SizesCols:
    fmt = "format"
    sizeUsedByte = data.COL_F_WRITTEN_BYTE
    comprRateBitsPerInt = data.COL_F_COMPRRATE_BITSPERINT

def getSizes(sizesFilePath):
    dfIn = readMorphStoreCsv(sizesFilePath)

    bitsPerByte = 8 # TODO This does not belong here.
    df = pd.DataFrame({
        SizesCols.fmt: dfIn["format"],
        SizesCols.sizeUsedByte: dfIn["sizeUsedByte"],
        SizesCols.comprRateBitsPerInt: dfIn["sizeUsedByte"] / (
                dfIn["valueCount"] * (data.MAX_BW / bitsPerByte)
        ),
    })
    df.index=dfIn["colName"]
    return df