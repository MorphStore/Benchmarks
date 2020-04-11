#!/usr/bin/env python3

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
A greedy algorithm trying to find the best or worst combination of the formats
of base and intermediate columns in SSB queries.
"""

import argparse
import math
import os
import subprocess
import sys

# TODO This is relative to ssb.sh.
sys.path.append("../tools/mal2x")
import mal2morphstore.compr as compr
import mal2morphstore.formats as formats
import mal2morphstore.processingstyles as pss

import csvutils


# *****************************************************************************
# Helper functions
# *****************************************************************************

def getAltSimpleName(fmt):
    """
    Returns the simple name of the given format, whereby the bit width is
    appended to the name for StaticVBP.
    """
    fmtName = fmt.getSimpleName();
    if isinstance(fmt, formats.StaticVBPFormat):
        fmtName += "_{}".format(fmt._bw)
    return fmtName

def saveFmtComb(fmtComb):
    """
    Stores a format combination to a CSV file.
    """
    with open(os.path.join(combDir, "q{}.csv".format(query)), "w") as f:
        f.write("colName\tformat\n")
        for colName, fmt in fmtComb.items():
            f.write("{}\t{}\n".format(colName, getAltSimpleName(fmt)))
            
def loadFmtComb(ps):
    """
    Loads a format combination from a CSV file.
    """
    fmtComb = {}
    with open(os.path.join(combDir, "q{}.csv".format(query)), "r") as f:
        f.readline() # skip header
        for line in f:
            colName, fmtName = line.rstrip().split("\t")
            fmtComb[colName] = formats.byName(fmtName, ps)
    return fmtComb
            
def saveRuntimes(runtimes):
    """
    Stores runtime measurements to a CSV file.
    """
    with open(os.path.join(combDir, "q{}_runtimes.csv".format(query)), "w") as f:
        f.write("colName\tformat\trepetition\truntime\n")
        for colName, fmt, repetition, runtime in runtimes:
            f.write("{}\t{}\t{}\t{}\n".format(
                    colName, getAltSimpleName(fmt), repIdx, runtime)
            )
            
def getBwChoice(minBw):
    """
    Returns the bit widths we want to consider given the smallest bit width
    required for a lossless representation.
    """
    bws = {
        minBw, # the bit width itself
        minBw + minBw % 2, # the next even bit width
        minBw + (8 - minBw % 8) # the next multiple of a byte
    }
    # The next power of two.
    for potBw in [2, 4, 8, 16, 32]:
        if minBw <= potBw:
            bws.add(potBw)
            break
    # Return only bit widths less than 64 bits.
    return [bw for bw in sorted(bws) if bw < 64]

def getFmtChoice(hasRndAcc, maxBw):
    """
    Returns the formats to choose from for a column with the specified
    properties.
    """
    choice = []
    for bw in getBwChoice(maxBw):
        choice.append(formats.StaticVBPFormat(ps, bw))
    if not hasRndAcc:
        choice.extend([
            formats.DynamicVBPFormat(ps),
            formats.DeltaCascFormat(
                    formats.CASC_BLOCKSIZE_LOG,
                    ps,
                    formats.DynamicVBPFormat(ps)
            ),
            formats.ForCascFormat(
                    formats.CASC_BLOCKSIZE_LOG,
                    ps,
                    formats.DynamicVBPFormat(ps)
            ),
        ])
    return choice

def evaluateFmtComb(fmtComb):
    """
    Determines the runtime yielded by the given format combination by executing
    the query.
    """
    saveFmtComb(fmtComb)
    args = "./ssb.sh -mem n -um s -sf {} -ps {} -p t -c manual -cconfig {} -q {}".format(
            scaleFactor, ps, combDir, query
    ).split(" ")
    # Build the executable.
    ret = subprocess.call(
            args + "-s t -e b".split(" "),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if ret:
        raise RuntimeError("query build failed")
    # Execute the query multiple times.
    runtimes = []
    for repIdx in range(countReps):
        # Execute the query once.
        ret = subprocess.call(
                args + "-s r".split(" "),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if ret:
            raise RuntimeError("query execution failed")
        # Extract the time measurement from the results file.
        dfTime = csvutils.readMorphStoreCsv(os.path.join(
                "time_sf{}".format(scaleFactor), "q{}.csv".format(query)
        ))
        runtimes.append(dfTime[dfTime["opIdx"] == 0]["runtime"].values[0])
    return runtimes


# *****************************************************************************
# Main program
# *****************************************************************************

if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Argument parsing and configuration
    # -------------------------------------------------------------------------
    
    parser = argparse.ArgumentParser(description=__doc__)
    # TODO Add descriptions.
    parser.add_argument(
            "-q", "--query", metavar="N.N",
            required=True,
    )
    parser.add_argument(
            "-sf", "--scaleFactor", metavar="N", type=int,
            required=True,
    )
    parser.add_argument(
            "-ps", "--processingStyle", metavar="PROCESSING_STYLE",
            required=True,
    )
    parser.add_argument(
            "-d", "--distance", metavar="DISTANCE",
            default=csvutils.ColInfoCols.producingOpIdx,
    )
    parser.add_argument(
            "-o", "--outputDir", metavar="DIR",
            required=True,
    )
    parser.add_argument(
            "-r", "--repetitions", metavar="N", type=int,
            default=1,
    )
    gr = parser.add_mutually_exclusive_group(required=True)
    gr.add_argument(
            "--findBest", action="store_true",
    )
    gr.add_argument(
            "--findWorst", action="store_true",
    )
    args = parser.parse_args()
    
    query = args.query
    scaleFactor = args.scaleFactor
    ps = args.processingStyle
    distance = args.distance
    combDir = args.outputDir
    countReps = args.repetitions
    optimizeFactor = 1 if args.findBest else -1
    
    # -------------------------------------------------------------------------
    # Preparation
    # -------------------------------------------------------------------------
    
    os.makedirs(combDir, exist_ok=True)
    
    # Load the data characteristics.
    dfColInfos = csvutils.getColInfos(os.path.join(
            "dc_sf{}".format(scaleFactor), "q{}.csv".format(query)
    ));
    dfColInfos = dfColInfos.sort_values(distance)

    # Extract some relevant data characteristics.
    sIsResult = dfColInfos[csvutils.ColInfoCols.isResult]
    sIsForcedUncompr = dfColInfos[csvutils.ColInfoCols.isForcedUncompr]
    sHasRndAcc = dfColInfos[csvutils.ColInfoCols.hasRndAccUnsorted] | \
        dfColInfos[csvutils.ColInfoCols.hasRndAccSorted]
    sMaxBw = dfColInfos[csvutils.ColInfoCols.maxBw]

    # In the initial format combination, all columns are uncompressed.
    fmtComb = {colName: formats.UncomprFormat() for colName in dfColInfos.index}

    # Determine the number of possible format combinations.
    countFmtCombs = 1 # Leaving all columns uncompressed is always possible.
    for colName in dfColInfos.index:
        if sIsResult[colName] or sIsForcedUncompr[colName]:
            # These columns must remain uncompressed. Thus, they do not
            # contribute to the number of combinations.
            continue
        countFmtCombs += len(getFmtChoice(sHasRndAcc[colName], sMaxBw[colName]))

    # Print some stats.
    countCols = len(dfColInfos)
    sIsUncompr = sIsResult | sIsForcedUncompr
    countColsUncompr = sIsUncompr.sum()
    countColsCompr = countCols - countColsUncompr
    countColsRes = sIsResult.sum()
    print("Query {}".format(query))
    print("\t{} columns".format(countCols))
    print("\t\t{} can be compressed".format(countColsCompr))
    print("\t\t\t{} have rnd access".format((~sIsUncompr & sHasRndAcc).sum()))
    print("\t\t\t{} have only sequential access".format((~sIsUncompr & ~sHasRndAcc).sum()))
    print("\t\t{} must be uncompressed".format(countColsUncompr))
    print("\t\t\t{} are results".format(countColsRes))
    print("\t\t\t{} are uncompressed intermediates".format(countColsUncompr - countColsRes))
    print("\t{} format combinations".format(countFmtCombs))
    
    # -------------------------------------------------------------------------
    # Greedy search.
    # -------------------------------------------------------------------------
    
    measurements = []
    first = True
    bestRuntimeOverall = math.inf
    colIdx = 0
    for colName in dfColInfos.index:
        if sIsUncompr[colName]:
            continue
        colIdx += 1
        print("Trying formats for '{}' ({}/{})".format(
                colName, colIdx, countColsCompr)
        )
        hasRndAcc = sHasRndAcc[colName]
        if hasRndAcc:
            print("\thas random access")
        else:
            print("\thas only sequential access")

        bestRuntime = math.inf
        bestFormat = None
        choice = getFmtChoice(hasRndAcc, sMaxBw[colName])
        if first:
            choice = [formats.UncomprFormat()] + choice
            first = False
        for fmt in choice:
            print("\t\ttrying '{}'".format(getAltSimpleName(fmt)))
            fmtComb[colName] = fmt
            runtimes = evaluateFmtComb(fmtComb)
            for repIdx, runtime in enumerate(runtimes, start=1):
                measurements.append([colName, fmt, repIdx, runtime])
            runtime = sum(runtimes) / len(runtimes) * optimizeFactor
            if runtime < bestRuntime:
                bestRuntime = runtime
                bestFormat = fmt
        if bestRuntime < bestRuntimeOverall:
            bestRuntimeOverall = bestRuntime
        else:
            bestFormat = formats.UncomprFormat()
        print("\tselected '{}'".format(getAltSimpleName(bestFormat)))
        fmtComb[colName] = bestFormat

    # -------------------------------------------------------------------------
    # Result output
    # -------------------------------------------------------------------------
    
    # Save resulting format combination and all measurements to files.
    saveFmtComb(fmtComb)
    saveRuntimes(measurements)