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
Replaces string literals in a SQL query by their code in a dictionary.

The modified SQL query is printed to stdout and can be used on a
dictionary-coded dataset. Such a dataset can be obtained using MorphStore's
dbdict.py. Furthermore, the modified SQL query contains some diagnostic
comments.

In particular, comparisons of a column with a string literal are searched for.
The column name determines the dictionary file to use, while the string literal
is the value whose code is looked up in that file. The following forms of
comparisons are supported:
- [tbl.]col op 'foo'
- 'foo' op [tbl.]col
- [tbl.]col BETWEEN 'foo' AND 'bar'
Whereby op is a comparison operator in <, <=, =, >=, >, !=, <>. Note that the
LIKE operator is not supported, since it cannot always be applied to dictionary
codes.

A dictionary file must fulfil the following criteria:
- Its name must have the pattern "<tablename>.<columnname>.dict".
- It is a text file whose i-th line contains the value whose code is i, lines
  are counted starting at zero.
- Lines are separated by the newline character "\\n".
Such dictionary files can be obtained using MorphStore's dbdict.py.

Known limitations:
- The substitution is done using regular expressions and there are certainly
  ways to fool these.
- This script is not tailored for efficiency.
- Aliases as in "FROM table1 AS t1" are not supported yet.
"""

# TODO Make diagnostic comments optional.
# TODO Automatic tests.
# TODO Documentation for parameters and return values.


import argparse
import os
import re
import sys


# *****************************************************************************
# Some helper functions.
# *****************************************************************************

def _findTblName(colName):
    """
    Determines the name of the table which has a dictionary-coded column
    with the specified name. Raises an error if there is no such table or if
    there are multiple such tables.
    """

    resTblName = None
    for tblName, colNames in colNamesByTblNames.items():
        if colName in colNames:
            if resTblName is None:
                # Found the first table having the column.
                resTblName = tblName
            else:
                # Found another table having the column.
                raise RuntimeError(
                    "column name '{}' is ambiguous, could mean '{}.{}' or "
                    "'{}.{}'".format(
                        colName, resTblName, colName, tblName, colName
                    )
                )
    if resTblName is None:
        # Found no table having the column.
        raise RuntimeError(
            "found no table having a dictionary-coded column named "
            "'{}'".format(colName)
        )
    return resTblName

def _findValCode(tblName, colName, valStr):
    """
    Determines the code of the given string value in the dictionary of the
    given column from the given table.
    """
    
    valCode = None
    dictFilePath = os.path.join(args.dictDirPath, "{}.{}.dict".format(
        tblName, colName
    ))
    with open(dictFilePath, "r") as dictFile:
        for idx, line in enumerate(dictFile):
            if line[-1] == "\n":
                line = line[:-1]
            if line == valStr:
                valCode = idx
                break
    if valCode is None:
        raise RuntimeError(
            "could not find value '{}' in the dictionary of column "
            "'{}.{}'".format(valStr, tblName, colName)
        )
    return valCode


# *****************************************************************************
# Regular expressions for finding string literals in a SQL query
# *****************************************************************************

# Sub-patterns frequently needed in the actual full-patterns.

# Sub-pattern for a string literal. Starts with a quoatation mark in " ' ` and
# ends with the same quoatation mark. Between the quoation marks there is the
# actual value. Since a full-pattern can contain more than one string literal
# (see BETWEEN), this is a format string allowing the insertion of an index.
_fspStrLit = "(?P<quote{idx}>[\"'`])(?P<valStr{idx}>.+?)(?P=quote{idx})"

# Sub-pattern for a column name, optionally preceeded by a table name and a
# dot.
_pTblCol = r"(?:(?P<tblName>\w+)\s*\.\s*)?(?P<colName>\w+)"

# Sub-pattern for a comparison operator.
_pOp = r"(?P<op><|<=|=|>=|>|!=|<>|LIKE)"

# Full-patterns, composed of the sub-patterns above.

# Pattern for a comparison of a column (on the left hand side) with a single
# string literal (on the right hand side), e.g., "tbl.col = 'foo'".
_pColCmpLit = re.compile(
    r"(?P<prefix>{}\s*{}\s*){}".format(
        _pTblCol, _pOp, _fspStrLit.format(idx=""),
    ),
    re.IGNORECASE, # needed for "LIKE"
)

# Pattern for a comparison of a column (on the right hand side) with a single
# string literal (on the left hand side), e.g., "'foo' = tbl.col".
_pLitCmpCol = re.compile(
    r"{}(?P<suffix>\s*{}\s*{})".format(
        _fspStrLit.format(idx=""), _pOp, _pTblCol,
    ),
    re.IGNORECASE, # needed for "LIKE"
)

# Pattern for a comparison of a column with two string literals using a BETWEEN
# operator, e.g. "tbl.col BETWEEN 'foo' AND 'bar'".
_pColBetween = re.compile(
    r"(?P<prefix>{}\s+BETWEEN\s*){}(?P<infix>\s*AND\s*){}".format(
        _pTblCol, _fspStrLit.format(idx=0), _fspStrLit.format(idx=1)
    ),
    re.IGNORECASE, # needed for "BETWEEN" and "AND"
)


# *****************************************************************************
# Functions for replacing string literals by their dictionary codes.
# *****************************************************************************
# - Intended for usage with function sub() of match-objects.
# - While a match spans the entire comparison, we only want to replace the
#   contained string-literal(s).

def _getTblAndColName(match):
    """Extracts the table name and column name from a regex match."""
    tblName = match.group("tblName")
    colName = match.group("colName")
    if tblName is None:
        tblName = _findTblName(colName)
    return tblName, colName

def _checkNotLike(match):
    """
    Raises an error if the comparison operator captured by the given regex
    match is a "LIKE", because we do not support this.
    """
    if match.group("op").upper() == "LIKE":
        raise RuntimeError("comparisons using LIKE are not supported")

def _replaceColCmpLit(match):
    """
    Replacement function for comparisons of the form "tbl.col = 'foo'" (see
    _pColCmpLit).
    """
    _checkNotLike(match)
    tblName, colName = _getTblAndColName(match)
    valStr = match.group("valStr")
    valCode = _findValCode(tblName, colName, valStr)
    return "{}{} /*{}.{} '{}'*/".format(
        match.group("prefix"), valCode, tblName, colName, valStr
    )

def _replaceLitCmpCol(match):
    """
    Replacement function for comparisons of the form "'foo' = tbl.col" (see
    _pLitCmpCol).
    """
    _checkNotLike(match)
    tblName, colName = _getTblAndColName(match)
    valStr = match.group("valStr")
    valCode = _findValCode(tblName, colName, valStr)
    return "{} /*{}.{} '{}'*/{}".format(
        valCode, tblName, colName, valStr, match.group("suffix")
    )

def _replaceColBetween(match):
    """
    Replacement function for comparisons of the form "tbl.col BETWEEN 'foo' AND 
    'bar'" (see _pBetween).
    """
    tblName, colName = _getTblAndColName(match)
    valStr0 = match.group("valStr0")
    valStr1 = match.group("valStr1")
    valCode0 = _findValCode(tblName, colName, valStr0)
    valCode1 = _findValCode(tblName, colName, valStr1)
    return "{}{} /*{}.{} '{}'*/ {}{} /*{}.{} '{}'*/".format(
        match.group("prefix"),
        valCode0,
        tblName,
        colName,
        valStr0,
        match.group("infix"),
        valCode1,
        tblName,
        colName,
        valStr1,
    )


# *****************************************************************************
# Main program.
# *****************************************************************************

if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Argument parsing
    # -------------------------------------------------------------------------
    
    FROM_STDIN = "-"
    
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Required positional arguments.
    parser.add_argument(
        "dictDirPath", metavar="dictDir",
        help="The directory where the dictionary files are located."
        # TODO validate existence
    )
    # Optional arguments with defaults.
    parser.add_argument(
        "--sqlFile", dest="sqlFilePath",
        default=FROM_STDIN, metavar="FILE",
        help="The path of the SQL file. If this argument is omitted or if {} "
            "is specified, then the SQL query will be read from stdin.".format(
                FROM_STDIN
            )
        # TODO validate existence
    )
    args = parser.parse_args()

    if args.sqlFilePath == FROM_STDIN:
        # 0 is the file descriptor of stdin and can be used with open().
        args.sqlFilePath = 0

    # -------------------------------------------------------------------------
    # Actual work
    # -------------------------------------------------------------------------
    
    # Find out for which columns of which tables there are dictionaries.
    pDictFileName = re.compile(r"(\w+).(\w+).dict")
    colNamesByTblNames = {}
    for fileName in os.listdir(args.dictDirPath):
        m = pDictFileName.fullmatch(fileName)
        if m is not None:
            tblName = m.group(1)
            colName = m.group(2)
            if tblName not in colNamesByTblNames:
                colNamesByTblNames[tblName] = []
            colNamesByTblNames[tblName].append(colName)

    # Read the query from the SQL file and replace string literals by their
    # code in the respective column's dictionary. Print the result to stdout.
    with open(args.sqlFilePath, "r") as sqlFile:
        sql = sqlFile.read()
        if sql == "":
            sys.exit(1)
        sql = _pColCmpLit .sub(_replaceColCmpLit , sql)
        sql = _pLitCmpCol .sub(_replaceLitCmpCol , sql)
        sql = _pColBetween.sub(_replaceColBetween, sql)
        print(
            "-- String literals automatically substituted for dictionary "
            "codes by {}".format(os.path.basename(sys.argv[0]))
        )
        print(sql, end="")