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
Facilities for analyzing the abstract representation of a translated program.

Such analyses can be used to find out interesting facts about the translated
program. A translated program can be analyzed by passing it to function
analyze(), which returns an instance of class AnalysisResult containing the
relevant information.
"""

# TODO Documentation for parameters and return values.


import mal2morphstore.operators as ops

import json
import os


class AnalysisResult:
    """
    Encapsulates all information analyzed about a translated program. This is
    used as the result type of the function analyze().
    """
    
    def __init__(
        self,
        varsUsedBeforeAssigned,
        varsNeverUsed,
        varsUnique,
        maxCardByCol,
        maxBwByCol,
        varsRndAccessUnsorted,
        varsRndAccessSorted,
        countSeqAccessByCol,
        varsSorted,
        varsForcedUncompr,
    ):
        # A list of the names of column-variables in the translated program
        # which are used before they are assigned.
        # This should never happen, since the translated C++ programm will not
        # be compilable, thus we want to know if it happened.
        self.varsUsedBeforeAssigned = varsUsedBeforeAssigned
        
        # A list of the names of column-variables in the translated program
        # which are assigned, but never used.
        # This should better not happen, since it might indicate a bug in the
        # translation or an inefficiency in the translated program.
        self.varsNeverUsed = varsNeverUsed
        
        # A list of the names of column-variables in the translated program
        # whose contents is unique, i.e., columns that are known at query
        # translation-time to contain only unique data elements.
        self.varsUnique = varsUnique
        
        # A dictionary mapping a column name to a pessimistic estimate of that
        # column's cardinality.
        self.maxCardByCol = maxCardByCol
        
        # A dictionary mapping a column name to the effective bit width of the 
        # pessimisticly estimated maximum value in that column.
        self.maxBwByCol = maxBwByCol
        
        # A list of the names of column-variables in the translated program
        # which require unsorted random access, i.e., which are the input data
        # column of some project-operator with an unsorted input positions
        # column.
        self.varsRndAccessUnsorted = varsRndAccessUnsorted
        
        # A list of the names of column-variables in the translated program
        # which require sorted random access (skip-sequential access), i.e.,
        # which are the input data column of some project-operator with a
        # sorted input positions column.
        self.varsRndAccessSorted = varsRndAccessSorted
        
        # A dictionary mapping a column name to the number of sequential read
        # accesses to that column in the translated query program.
        self.countSeqAccessByCol = countSeqAccessByCol
        
        # A list of the names of column-variables in the translated program
        # whose contents is known to be sorted at query translation-time.
        self.varsSorted = varsSorted
        
        # A list of the names of column-variables in the translated program
        # which must be uncompressed, since they are produced or consumed by an
        # operator which does not support compressed data.
        self.varsForcedUncompr = varsForcedUncompr

def analyze(translationResult, analyzeCardsAndBws=False, statDirPath=None):
    """
    Analyzes the given abstract representation of a translated program to find
    out some interesting things about it. The result is an instance of class
    AnalysisResult.
    """
    
    # In the beginning, only the base columns are "assigned".
    varsAssigned = [
        "{}.{}".format(tblName, colName)
        for tblName in translationResult.colNamesByTblName
        for colName in translationResult.colNamesByTblName[tblName]
    ]
    
    varsUsedBeforeAssigned = []
    
    varsNeverUsed = []
    
    # TODO This is specific to the Star Schema Benchmark (SSB) and covers only
    #      what we currently need for translating the queries in this benchmark
    #      correctly.
    # All base-columns known to be unique.
    varsUnique = [
        # These are the primary key columns of the dimension tables of the SSB
        # schema. We know that each of these is unique.
        "customer.c_custkey",
        "date.d_datekey",
        "part.p_partkey",
        "supplier.s_suppkey",
    ]
    
    varsRndAccessUnsorted = []
    varsRndAccessSorted = []
    
    # In the beginning, we know only the base columns, and, so far, each of
    # them has been read sequentially zero times.
    countSeqAccessByCol = {
        "{}.{}".format(tblName, colName): 0
        for tblName in translationResult.colNamesByTblName
        for colName in translationResult.colNamesByTblName[tblName]
    }
    
    # TODO This is specific to the Star Schema Benchmark (SSB). Do not hardcode
    #      this.
    # All base-columns known to be sorted.
    varsSorted = [
        "customer.c_custkey",
        "customer.c_name",
        "date.d_datekey",
        "lineorder.lo_shippriority",
        "part.p_partkey",
        "supplier.s_name",
        "supplier.s_suppkey",
    ]
    
    varsForcedUncompr = []
    
    # TODO Use the implementation in lcbase_py.whitebox.
    def effective_bitwidth(val):
        if val == 0:
            return 1
        else:
            return 64 - "{:0>64b}".format(val).find("1")
    
    if analyzeCardsAndBws:
        if statDirPath is None:
            raise RuntimeError(
                "a directory containing statistics on the base data must be "
                "specified to analyze cardinalities and bit widths"
            )
        stats = {}
        for tblName in translationResult.colNamesByTblName:
            with open(os.path.join(statDirPath, "{}.json".format(tblName)), "r") as inFile:
                stats[tblName] = json.load(inFile)

        maxCardByCol = {
            "{}.{}".format(tblName, colName): stats[tblName]["_cardinality"]
            for tblName in translationResult.colNamesByTblName
            for colName in translationResult.colNamesByTblName[tblName]
        }
        maxBwByCol = {
            "{}.{}".format(tblName, colName): effective_bitwidth(
                stats[tblName][colName]
            )
            for tblName in translationResult.colNamesByTblName
            for colName in translationResult.colNamesByTblName[tblName]
        }
    else:
        maxCardByCol = None
        maxBwByCol = None
    
    def foundUsage(var):
        if var not in varsAssigned and var not in varsUsedBeforeAssigned:
            varsUsedBeforeAssigned.append(var)
        elif var in varsNeverUsed:
            varsNeverUsed.remove(var)
            
    def raiseNonUnique(op, param):
        raise RuntimeError(
                "input column '{}' of operator '{}' must be unique".format(
                        param, op.__class__.__name__
                )
        )
            
    def raiseUnsorted(op, param):
        raise RuntimeError(
                "input column '{}' of operator '{}' must be sorted".format(
                        param, op.__class__.__name__
                )
        )
    
    for el in translationResult.baseMorphs + translationResult.prog + translationResult.resultMorphs:
        if isinstance(el, ops.Op):
            # Tracking the usage of column-variables.
            for key in el.__dict__:
                if key.startswith("out") and key.endswith("Col"):
                    varName = getattr(el, key)
                    varsAssigned.append(varName)
                    varsNeverUsed.append(varName)
                    countSeqAccessByCol[varName] = 0
                elif key.startswith("in") and key.endswith("Col"):
                    varName = getattr(el, key)
                    foundUsage(varName)
                    
            # Tracking the uniqueness of column-variables.
            if isinstance(el, ops.Project):
                if el.inDataCol in varsUnique and el.inPosCol in varsUnique:
                    varsUnique.append(el.outDataCol)
            elif isinstance(el, ops.Select) or isinstance(el, ops.Between):
                varsUnique.append(el.outPosCol)
            elif (
                isinstance(el, ops.Intersect) or
                isinstance(el, ops.IntersectKAry) or
                isinstance(el, ops.Merge)
            ):
                if el.inPosLCol not in varsUnique:
                    raiseNonUnique(el, "inPosLCol")
                if el.inPosRCol not in varsUnique:
                    raiseNonUnique(el, "inPosRCol")
                varsUnique.append(el.outPosCol)
            elif isinstance(el, ops.Join):
                # TODO The following assumes an equi-join.
                # Uniqueness of one side's input(data) implies uniqueness of
                # the other side's output(positions).
                if el.inDataLCol in varsUnique:
                    varsUnique.append(el.outPosRCol)
                if el.inDataRCol in varsUnique:
                    varsUnique.append(el.outPosLCol)
            elif isinstance(el, ops.Nto1Join):
                if el.inDataLCol not in varsUnique:
                    raiseNonUnique(el, "inDataLCol")
                varsUnique.append(el.outPosRCol)
                if el.inDataRCol in varsUnique:
                    varsUnique.append(el.outPosLCol)
            elif isinstance(el, ops.LeftSemiNto1Join):
                if el.inDataLCol not in varsUnique:
                    raiseNonUnique(el, "inDataLCol")
                varsUnique.append(el.outPosRCol)
            elif isinstance(el, ops.CalcBinary):
                # We do not know whether the output is unique.
                pass
            elif isinstance(el, ops.SumWholeCol):
                # Unique since it contains only one data element.
                varsUnique.append(el.outDataCol)
            elif isinstance(el, ops.SumGrBased):
                # We do not know whether the output is unique.
                pass
            elif isinstance(el, ops.GroupUnary):
                if el.inDataCol in varsUnique:
                    varsUnique.append(el.outGrCol)
                varsUnique.append(el.outExtCol)
            elif isinstance(el, ops.GroupBinary):
                if el.inDataCol in varsUnique and el.inGrCol in varsUnique:
                    varsUnique.append(el.outGrCol)
                varsUnique.append(el.outExtCol)
            elif isinstance(el, ops.Morph):
                if el.inCol in varsUnique:
                    varsUnique.append(el.outCol)
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking the uniqueness of columns".format(
                                el.__class__.__name__
                        )
                )
                
            if analyzeCardsAndBws:
                # Tracking the maximum cardinalities of columns.
                if isinstance(el, ops.Project):
                    maxCardByCol[el.outDataCol] = maxCardByCol[el.inPosCol]
                elif isinstance(el, ops.Select) or isinstance(el, ops.Between):
                    maxCardByCol[el.outPosCol] = maxCardByCol[el.inDataCol]
                elif (
                    isinstance(el, ops.Intersect) or
                    isinstance(el, ops.IntersectKAry)
                ):
                    maxCardByCol[el.outPosCol] = min(
                        maxCardByCol[el.inPosLCol], maxCardByCol[el.inPosRCol]
                    )
                elif isinstance(el, ops.Merge):
                    maxCardByCol[el.outPosCol] = \
                        maxCardByCol[el.inPosLCol] + maxCardByCol[el.inPosRCol]
                elif isinstance(el, ops.Join):
                    # We do not use this variant of the join-operator any more.
                    pass
                elif isinstance(el, ops.Nto1Join):
                    #
                    maxCardByCol[el.outPosLCol] = maxCardByCol[el.inDataRCol]
                    maxCardByCol[el.outPosRCol] = maxCardByCol[el.inDataRCol]
                elif isinstance(el, ops.LeftSemiNto1Join):
                    #
                    maxCardByCol[el.outPosRCol] = maxCardByCol[el.inDataRCol]
                elif isinstance(el, ops.CalcBinary):
                    maxCardByCol[el.outDataCol] = maxCardByCol[el.inDataLCol]
                elif isinstance(el, ops.SumWholeCol):
                    maxCardByCol[el.outDataCol] = 1
                elif isinstance(el, ops.SumGrBased):
                    maxCardByCol[el.outDataCol] = maxCardByCol[el.inExtCol]
                elif isinstance(el, ops.GroupUnary):
                    maxCardByCol[el.outGrCol] = maxCardByCol[el.inDataCol]
                    maxCardByCol[el.outExtCol] = maxCardByCol[el.inDataCol]
                elif isinstance(el, ops.GroupBinary):
                    maxCardByCol[el.outGrCol] = maxCardByCol[el.inDataCol]
                    maxCardByCol[el.outExtCol] = maxCardByCol[el.inDataCol]
                elif isinstance(el, ops.Morph):
                    maxCardByCol[el.outCol] = maxCardByCol[el.inCol]
                else:
                    raise RuntimeError(
                            "the operator {} is not taken into account in "
                            "tracking the maximum cardinalities of columns".format(
                                    el.__class__.__name__
                            )
                    )

                # Tracking the maximum bit width of columns.
                if isinstance(el, ops.Project):
                    maxBwByCol[el.outDataCol] = maxBwByCol[el.inDataCol]
                elif isinstance(el, ops.Select) or isinstance(el, ops.Between):
                    maxBwByCol[el.outPosCol] = effective_bitwidth(
                        maxCardByCol[el.inDataCol] - 1
                    )
                elif (
                    isinstance(el, ops.Intersect) or
                    isinstance(el, ops.IntersectKAry)
                ):
                    maxBwByCol[el.outPosCol] = min(
                        maxBwByCol[el.inPosLCol], maxBwByCol[el.inPosRCol]
                    )
                elif isinstance(el, ops.Merge):
                    maxBwByCol[el.outPosCol] = max(
                        maxBwByCol[el.inPosLCol], maxBwByCol[el.inPosRCol]
                    )
                elif isinstance(el, ops.Join):
                    # We do not use this variant of the join-operator any more.
                    pass
                elif isinstance(el, ops.Nto1Join):
                    maxBwByCol[el.outPosLCol] = effective_bitwidth(
                        maxCardByCol[el.inDataLCol] - 1
                    )
                    maxBwByCol[el.outPosRCol] = effective_bitwidth(
                        maxCardByCol[el.inDataRCol] - 1
                    )
                elif isinstance(el, ops.LeftSemiNto1Join):
                    maxBwByCol[el.outPosRCol] = effective_bitwidth(
                        maxCardByCol[el.inDataRCol] - 1
                    )
                elif isinstance(el, ops.CalcBinary):
                    # TODO Remove the std-functors once we only use the TVL
                    # primitives. Currently they must be included to avoid
                    # query translation problems with the hand-vectorized
                    # operators.
                    if el.op in ["add", "std::plus"]:
                        maxBwByCol[el.outDataCol] = min(
                            64,
                            1 + max(
                                maxBwByCol[el.inDataLCol], maxBwByCol[el.inDataRCol]
                            )
                        )
                    elif el.op in ["sub", "std::minus"]:
                        maxBwByCol[el.outDataCol] = max(
                            maxBwByCol[el.inDataLCol],
                            maxBwByCol[el.inDataRCol]
                        )
                    elif el.op in ["mul", "std::multiplies"]:
                        maxBwByCol[el.outDataCol] = min(
                            64,
                            maxBwByCol[el.inDataLCol] + \
                                maxBwByCol[el.inDataRCol]
                        )
                    else:
                        raise RuntimeError(
                            "binary calc with the operation '{}' is not taken "
                            "into account".format(el.op)
                        )
                elif (
                    isinstance(el, ops.SumWholeCol) or
                    isinstance(el, ops.SumGrBased)
                ):
                    maxBwByCol[el.outDataCol] = 64
                elif isinstance(el, ops.GroupUnary):
                    maxBwByCol[el.outGrCol] = effective_bitwidth(
                        maxCardByCol[el.inDataCol] - 1
                    )
                    maxBwByCol[el.outExtCol] = effective_bitwidth(
                        maxCardByCol[el.inDataCol] - 1
                    )
                elif isinstance(el, ops.GroupBinary):
                    maxBwByCol[el.outGrCol] = effective_bitwidth(
                        maxCardByCol[el.inDataCol] - 1
                    )
                    maxBwByCol[el.outExtCol] = effective_bitwidth(
                        maxCardByCol[el.inDataCol] - 1
                    )
                elif isinstance(el, ops.Morph):
                    maxBwByCol[el.outCol] = maxBwByCol[el.inCol]
                else:
                    raise RuntimeError(
                            "the operator {} is not taken into account in "
                            "tracking the maximum bit width of columns".format(
                                    el.__class__.__name__
                            )
                    )
                    
            # Tracking which columns require random access.
            if isinstance(el, ops.Project):
                if el.inPosCol in varsSorted:
                    if el.inDataCol not in varsRndAccessSorted:
                        varsRndAccessSorted.append(el.inDataCol)
                else:
                    if el.inDataCol not in varsRndAccessUnsorted:
                        varsRndAccessUnsorted.append(el.inDataCol)
            elif isinstance(el, ops.IntersectKAry):
                # The accessed positions are always unsorted due to the k-ary
                # search.
                varsRndAccessUnsorted.append(el.inPosRCol)
            elif (
                isinstance(el, ops.Select) or
                isinstance(el, ops.Between) or
                isinstance(el, ops.Intersect) or
                isinstance(el, ops.Merge) or
                isinstance(el, ops.Join) or
                isinstance(el, ops.Nto1Join) or
                isinstance(el, ops.LeftSemiNto1Join) or
                isinstance(el, ops.CalcBinary) or
                isinstance(el, ops.SumWholeCol) or
                isinstance(el, ops.SumGrBased) or
                isinstance(el, ops.GroupUnary) or
                isinstance(el, ops.GroupBinary) or
                isinstance(el, ops.Morph)
            ):
                pass
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking which columns require random access.".format(
                                el.__class__.__name__
                        )
                )
                
            # Tracking the number of sequential read accesses to each column.
            if isinstance(el, ops.Project):
                countSeqAccessByCol[el.inPosCol] += 1
            elif (
                isinstance(el, ops.Select) or
                isinstance(el, ops.Between) or
                isinstance(el, ops.GroupUnary) or
                isinstance(el, ops.SumWholeCol)
            ):
                countSeqAccessByCol[el.inDataCol] += 1
            elif (
                isinstance(el, ops.Intersect) or
                isinstance(el, ops.Merge)
            ):
                # TODO Update this once we use other implementations of these
                # operators.
                # In the current implementation, the input columns are not
                # accessed sequentially.
                pass
            elif isinstance(el, ops.IntersectKAry):
                countSeqAccessByCol[el.inPosLCol] += 1
            elif isinstance(el, ops.Join):
                # We do not use this variant of the join-operator any more.
                pass
            elif (
                isinstance(el, ops.Nto1Join) or
                isinstance(el, ops.LeftSemiNto1Join) or
                isinstance(el, ops.CalcBinary)
            ):
                # TODO This holds for a hash join with separate build and probe
                # phase, but might not hold for other join implementations.
                countSeqAccessByCol[el.inDataLCol] += 1
                countSeqAccessByCol[el.inDataRCol] += 1
            elif (
                isinstance(el, ops.SumGrBased) or
                isinstance(el, ops.GroupBinary)
            ):
                countSeqAccessByCol[el.inDataCol] += 1
                countSeqAccessByCol[el.inGrCol] += 1
            elif isinstance(el, ops.Morph):
                countSeqAccessByCol[el.inCol] += 1
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking the number of sequential read accesses to "
                        "columns".format(
                                el.__class__.__name__
                        )
                )
                    
            # Tracking the sortedness of column-variables.
            if isinstance(el, ops.Project):
                if el.inDataCol in varsSorted and el.inPosCol in varsSorted:
                    varsSorted.append(el.outDataCol)
            elif isinstance(el, ops.Select) or isinstance(el, ops.Between):
                varsSorted.append(el.outPosCol)
            elif (
                isinstance(el, ops.Intersect) or
                isinstance(el, ops.IntersectKAry) or
                isinstance(el, ops.Merge)
            ):
                # TODO IntersectKAry might be able to handle an unsorted left
                #      input column. This is not reflected here, yet.
                if el.inPosLCol not in varsSorted:
                    raiseUnsorted(el, "inPosLCol")
                if el.inPosRCol not in varsSorted:
                    raiseUnsorted(el, "inPosRCol")
                varsSorted.append(el.outPosCol)
            elif isinstance(el, ops.Join):
                # We do not know which of the two input data columns is the
                # inner and which is the outer, so we do not know which output
                # column is sorted.
                pass
            elif isinstance(el, ops.Nto1Join) or isinstance(el, ops.LeftSemiNto1Join):
                # The output positions corresponding to the right input, which
                # is the probe-side and, thus, the outer, is always sorted.
                varsSorted.append(el.outPosRCol)
            elif isinstance(el, ops.CalcBinary):
                # We do not know whether the output is sorted.
                pass
            elif isinstance(el, ops.SumWholeCol):
                # Sorted since it contains only one data element.
                varsSorted.append(el.outDataCol)
            elif isinstance(el, ops.SumGrBased):
                # We do not know whether the output is sorted.
                pass
            elif isinstance(el, ops.GroupUnary):
                # Generelly, outGrCol is sorted if a value in inDataCol cannot
                # re-occur after another value occurred.
                # There are more cases when this is fulfilled, however, this is
                # the case we can detect with the information we have.
                if el.inDataCol in varsUnique or el.inDataCol in varsSorted:
                    varsSorted.append(el.outGrCol)
                varsSorted.append(el.outExtCol)
            elif isinstance(el, ops.GroupBinary):
                # Analogous to the unary case, but for combinations.
                # There are more cases when this is fulfilled, however, these
                # are the cases we can detect with the information we have.
                if (el.inDataCol in varsUnique or el.inDataCol in varsSorted) and (el.inGrCol in varsUnique or el.inGrCol in varsSorted):
                    varsSorted.append(el.outGrCol)
                varsSorted.append(el.outExtCol)
            elif isinstance(el, ops.Morph):
                if el.inCol in varsSorted:
                    varsSorted.append(el.outCol)
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking the sortedness of columns".format(
                                el.__class__.__name__
                        )
                )
                
            # Tracking which columns must be uncompressed.
            if (
                    isinstance(el, ops.Intersect) or
                    isinstance(el, ops.Merge) or
                    isinstance(el, ops.CalcBinary) or
                    isinstance(el, ops.GroupBinary) or
                    isinstance(el, ops.SumGrBased)
            ):
                # These operators do not support compressed inputs and outputs.
                for key in el.__dict__:
                    if key.endswith("Col"):
                        # The group-based sum-operator does not access the data
                        # of its input extents column, but only needs to know
                        # the number of data elements.
                        if isinstance(el, ops.SumGrBased) and key == "inExtCol":
                            continue
                        varName = getattr(el, key)
                        if varName not in varsForcedUncompr:
                            varsForcedUncompr.append(varName)
            elif isinstance(el, ops.SumWholeCol):
                # This operators does not support compressed output (because it
                # would not make sense).
                varsForcedUncompr.append(el.outDataCol)
            elif (
                isinstance(el, ops.Project) or
                isinstance(el, ops.Select) or
                isinstance(el, ops.Between) or
                isinstance(el, ops.IntersectKAry) or
                isinstance(el, ops.Join) or
                isinstance(el, ops.Nto1Join) or
                isinstance(el, ops.LeftSemiNto1Join) or
                isinstance(el, ops.GroupUnary) or
                isinstance(el, ops.Morph)
            ):
                # These operators support compressed inputs and outputs.
                pass
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking which columns must be uncompressed".format(
                                el.__class__.__name__
                        )
                )
    
    for varName in translationResult.resultCols:
        foundUsage(varName)
    
    return AnalysisResult(
        varsUsedBeforeAssigned,
        varsNeverUsed,
        varsUnique,
        maxCardByCol,
        maxBwByCol,
        varsRndAccessUnsorted,
        varsRndAccessSorted,
        countSeqAccessByCol,
        varsSorted,
        varsForcedUncompr,
    )