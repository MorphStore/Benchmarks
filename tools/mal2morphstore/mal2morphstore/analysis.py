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


import mal2morphstore.operators


class AnalysisResult:
    """
    Encapsulates all information analyzed about a translated program. This is
    used as the result type of the function analyze().
    """
    
    def __init__(self, varsUsedBeforeAssigned, varsNeverUsed):
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
    
    def foundUsage(var):
        if var not in varsAssigned and var not in varsUsedBeforeAssigned:
            varsUsedBeforeAssigned.append(var)
        elif var in varsNeverUsed:
            varsNeverUsed.remove(var)
    
    for el in translationResult.prog:
        if isinstance(el, mal2morphstore.operators.Op):
            for key in el.__dict__:
                if key.startswith("out"):
                    varName = getattr(el, key)
                    varsAssigned.append(varName)
                    varsNeverUsed.append(varName)
                elif key.startswith("in"):
                    varName = getattr(el, key)
                    foundUsage(varName)
    
    for varName in translationResult.resultCols:
        foundUsage(varName)
    
    return AnalysisResult(varsUsedBeforeAssigned, varsNeverUsed)