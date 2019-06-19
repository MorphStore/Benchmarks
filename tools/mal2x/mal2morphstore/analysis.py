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


class AnalysisResult:
    """
    Encapsulates all information analyzed about a translated program. This is
    used as the result type of the function analyze().
    """
    
    def __init__(self, varsUsedBeforeAssigned, varsNeverUsed, varsUnique):
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

def analyze(translationResult):
    """
    Analyzes the given abstratc representation of a translated program to find
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
    
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            # Tracking the usage of column-variables.
            for key in el.__dict__:
                if key.startswith("out"):
                    varName = getattr(el, key)
                    varsAssigned.append(varName)
                    varsNeverUsed.append(varName)
                elif key.startswith("in"):
                    varName = getattr(el, key)
                    foundUsage(varName)
                    
            # Tracking the uniqueness of column-variables.
            if isinstance(el, ops.Project):
                if el.inDataCol in varsUnique and el.inPosCol in varsUnique:
                    varsUnique.append(el.outDataCol)
            elif isinstance(el, ops.Select):
                varsUnique.append(el.outPosCol)
            elif isinstance(el, ops.Intersect) or isinstance(el, ops.Merge):
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
            else:
                raise RuntimeError(
                        "the operator {} is not taken into account in "
                        "tracking the uniqueness of columns".format(
                                el.__class__.__name__
                        )
                )
    
    for varName in translationResult.resultCols:
        foundUsage(varName)
    
    return AnalysisResult(varsUsedBeforeAssigned, varsNeverUsed, varsUnique)