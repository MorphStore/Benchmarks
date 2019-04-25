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
Generation of the MorphStore C++ source code for an entire query.

This module is about the generation the of C++ source code representation of
the abstract representation of the translated program as a whole. That is, for
creating a complete CPP file compilable as a part of MorphStore without further
manual effort. The general frame for the CPP file is provided by a template CPP
file. That template file contains certain comment lines, which are replaced by
the translated program. Use function generate() to generate the C++ source code
of an abstract representation of the translated program.
"""

# TODO Document parameters and return values.
# TODO Support outputting the SQL-query again as a comment in the generated C++
#      code.
# TODO Use the result column names MonetDB uses for the output.
# TODO Output line numbers in error messages.
# TODO The format in which the C++ program outputs its result should be a
#      parameter -- either here or in the generated C++ program.
# TODO Automaticall break generated C++ source code lines in a nice way.
# TODO This module is not independent from the C++ template file. For instance,
#      it relies on the existence of certain header includes and variables in
#      the template. They should be completely independent.
# TODO Use constants for common literals ("binary_io", "load", "uncompr_f", the
#      monitoring macros, etc.) as in module operators.
# TODO Output the command line arguments passed to mal2morphstore.py in the
#      generated C++ files.


import mal2morphstore.analysis
from mal2morphstore.operators import Op
import mal2morphstore.processingstyles as ps

import os.path
import re
import sys


# *****************************************************************************
# * Functions producing the insertable code snippets
# *****************************************************************************

def _printDocu(indent, tr):
    """
    Prints a little "documentation" saying that the C++ code was automatially
    generated by this script.
    """
    
    print("{}/**".format(indent))
    # The filename is unknown to this script, since the output is printed to
    # stdout.
    #print("{} * @file ...".format(indent)
    print("{} * @brief This file was automatically generated by {}.".format(
        indent, os.path.basename(sys.argv[0]))
    )
    print("{} */".format(indent))

def _printHeaders(indent, tr, useMonitoring, processingStyle):
    """
    Prints preprocessor directives for the necessary header includes.
    """

    # Add the headers required for the query operators, tailored to the
    # selected processing style.
    for el in tr.prog:
        if isinstance(el, Op):
            for header in el.headers:
                tr.headers.add(header.format(
                        **{
                            ps.INCLUDE_DIR_KEY:
                            ps.INCLUDE_DIR_BY_PS[processingStyle]
                        }
                ))
    
    # Add monitoring header if required.
    if useMonitoring:
        tr.headers.add("core/utils/monitoring.h")
    
    # Print headers in lexicographical order.
    for header in sorted(tr.headers):
        print("{}#include <{}>".format(indent, header))
        
def _printSchema(indent, tr):
    """
    Prints the definition of a struct for each table used by the translated
    program. The struct of each table has one field for each column used from
    that table by the translated program.
    """
    
    for tblName in sorted(tr.colNamesByTblName):
        print("{}struct {}_t {{".format(indent, tblName))
        for colName in sorted(tr.colNamesByTblName[tblName]):
            print("{}    const column<uncompr_f> * {};".format(indent, colName))
        print("{}}} {};".format(indent, tblName))
        print()

def _printDataLoad(indent, tr):
    """
    Prints C++ statements for loading the data of each column used by the
    translated program from a file on disk.
    """
    
    maxLen = max([
        len(tblName) + 1 + len(colName)
        for tblName in tr.colNamesByTblName
        for colName in tr.colNamesByTblName[tblName]
    ])
    for tblName in sorted(tr.colNamesByTblName):
        for colName in sorted(tr.colNamesByTblName[tblName]):
            fullName = "{}.{}".format(tblName, colName)
            print(
                "{{}}{{: <{}}} = binary_io<uncompr_f>::load(dataPath + \"/{{}}.uncompr_f.bin\");".format(
                    maxLen
                ).format(
                    indent,
                    fullName,
                    fullName
                )
            )
        print()

def _printProg(indent, tr, useMonitoring, processingStyle):
    """
    Prints the core program, i.e., the sequence of operators.
    """
    
    # The constant representing the processing style to use for all operators.
    print("{}const processing_style_t {} = processing_style_t::{};".format(
            indent, ps.PS_VAR, processingStyle
    ))
    print()
    
    # The query program.
    if useMonitoring:
        monKeyQuery = "query"
        print('{}MONITOR_START_INTERVAL("{}")'.format(indent, monKeyQuery))
        print()
        for elIdx, el in enumerate(tr.prog):
            if isinstance(el, Op):
                monKeyOp = "{}_{}".format(el.opName, elIdx)
                print('{}MONITOR_START_INTERVAL("{}")'.format(
                        indent, monKeyOp)
                )
                print("{}{}".format(indent, el).replace("\n", "\n" + indent))
                print('{}MONITOR_END_INTERVAL  ("{}")'.format(
                        indent, monKeyOp)
                )
            else:
                print("{}{}".format(indent, el).replace("\n", "\n" + indent))
        print()
        print('{}MONITOR_END_INTERVAL  ("{}")'.format(indent, monKeyQuery))
    else:
        for el in tr.prog:
            print("{}{}".format(indent, el).replace("\n", "\n" + indent))

def _printResultOutput(indent, tr, useMonitoring):
    """
    Prints C++ statements for the output of the query's result columns.
    """

    if useMonitoring:
        print("{}MONITOR_PRINT_ALL(monitorShellLog, true)".format(indent))
    
    if True:
        # Output in the same CSV dialect MonetDB uses.
        print("{}print_columns_csv({{{}}});".format(
            indent,
            ", ".join(tr.resultCols)
        ))
    else:
        print("{}print_columns(print_buffer_base::decimal, {});".format(
            indent,
            ", ".join(tr.resultCols)
        ))

def _printAnalysis(indent, ar):
    """
    Prints C++ comments containing some interesting facts about the translated
    program.
    """
    
    print("{}//         Intermediates used before assigned".format(indent))
    if len(ar.varsUsedBeforeAssigned):
        print("{}// [WARN]: Found the following:".format(indent))
        for var in ar.varsUsedBeforeAssigned:
            print("{}//         - {}".format(indent, var))
    else:
        print("{}// [good]: Found none.".format(indent))
        
    print("{}//".format(indent))
        
    print("{}//         Intermediates never used".format(indent))
    if len(ar.varsNeverUsed):
        print("{}// [WARN]: Found the following:".format(indent))
        for var in ar.varsNeverUsed:
            print("{}//         - {}".format(indent, var))
    else:
        print("{}// [good]: Found none.".format(indent))
        
        
# *****************************************************************************
# * Function for generating the C++ source code as a whole
# *****************************************************************************

# Regular expression for recognizing comment lines in the template CPP file to
# replace by parts of the translated program.
_pPlaceholder = re.compile(r"(\s*)\/\/ ##### mal2morphstore (.+?) #####\s*")

def generate(
        translationResult, templateFilePath, useMonitoring, processingStyle
):
    """
    Generates the C++ source code for the given abstract representation of a
    translated program and prints it to stdout.
    
    This code generation is based on a template CPP file providing the general
    frame of the C++ program, i.e., the parts which are independent of the
    particular query. This template file is copied to the output line by line,
    whereby special comment lines are replaced by query-dependent C++ source
    snippets. These special comment lines have the following structure:
    "// ##### mal2morphstore ph #####". They can appear at any indentation
    level and the indentation of the comment is used for all lines which are
    inserted. ph is a placeholder for the kind of source snippet to be
    inserted. The supported placeholders are:
    - docu     : A small comment saying that the code was generated
    - headers  : C++ header includes required by the translated program
    - schema   : Definition of structs for the required tables
    - dataload : Loading the base data from files
    - prog     : The core of the query program, i.e., the sequence of operators
    - result   : The output of the query's result
    - analysis : Some interesting facts about the translated program
    """
    
    with open(templateFilePath, "r") as templateFile:
        for line in templateFile:
            line = line.rstrip()
            mPlaceholder = _pPlaceholder.fullmatch(line)
            if mPlaceholder is None:
                # Copy line from template file to output.
                print(line)
            else:
                # Insert C++ code snippet.
                indent = mPlaceholder.group(1)
                ph = mPlaceholder.group(2)
                if ph == "docu":
                    _printDocu(indent, translationResult)
                elif ph == "headers":
                    _printHeaders(
                            indent,
                            translationResult,
                            useMonitoring,
                            processingStyle
                    )
                elif ph == "schema":
                    _printSchema(indent, translationResult)
                elif ph == "dataload":
                    _printDataLoad(indent, translationResult)
                elif ph == "prog":
                    _printProg(
                            indent,
                            translationResult,
                            useMonitoring,
                            processingStyle
                    )
                elif ph == "result":
                    _printResultOutput(
                            indent, translationResult, useMonitoring
                    )
                elif ph == "analysis":
                    _printAnalysis(
                        indent,
                        mal2morphstore.analysis.analyze(translationResult)
                    )
                else:
                    raise RuntimeError(
                        "unknown placeholder in C++ template file: {}".format(
                            ph
                        )
                    )