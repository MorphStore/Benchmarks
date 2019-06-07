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
Translation of a MAL program to an abstract representation of a MorphStore C++
program.

This module provides the facilities for translating a MAL program produced by
MonetDB and available as a plain-text file to an abstract representation of a
MorphStore C++ program. This abstract representation is defined by class
TranslationResult in this module and can be further processed using the modules
mal2morphstore.output and mal2morphstore.analysis.

The function translate() is the entry-point for callers. The translation is
based on parsing the MAL program using regular expressions. During the
translation, several things must be tracked. These are representated by class
_TranslationState. The core of the translation are the individual translation
functions, one for each (family of) MAL operator(s). Each of these functions
changes the translation state according to its input, most importantly it
appends calls to MorphStore query operators in the form of instances of the
classes defined in module operators to the translated program.

Known limitations:
- Since MorphStore does not support sorting the result columns yet, we cannot
  translate MAL's sort operators. Instead, the translated program will output
  the unsorted result columns.
- Generally, we do not claim that this module, in its current state, is capable
  of translating every possible MAL program correctly. We merely tried to make
  it translate all cases currently relevant to us.

To add support for some additional MAL operator, the following will typically
have to be done:
- Add a new translation function for the new MAL operator. This may involve
  adding a new MorphStore operator in module mal2morphstore.operators if the
  new MAL operator cannot be expressed with existing MorphStore operators.
- Add a regular expression for parsing the MAL operator's parameters.
- In function translate() add a case calling the new translation function when
  encountering the new MAL operator in the MAL program.
"""

# TODO Support optionally outputting the original MAL assignments as comments.
# TODO Support optional status outputs so that during the execution of the
#      final C++ program the progress can be tracked.
# TODO The regular expressions in this module often treat the types of the
#      results or parameters of the MAL operators in a sloppy way. Maybe we
#      should at least check whether they are oids or actual data.
# TODO Translate MAL's sort operators as soon as MorphStore supports sorting.
# TODO Documentation for parameters and return values.
# TODO Systematically support all variants of the relevant MAL operators.
# TODO Error messages should mention the line number in the MAL program.


import mal2morphstore.analysis as analysis
import mal2morphstore.operators as ops
import mal2morphstore.processingstyles as ps

import re


_MAL_INT_TYPES = ["bit", "byte", "sht", "int", "lng", "hge"]


# *****************************************************************************
# Classes for the translation state and the translation result
# *****************************************************************************

class _TranslationState():
    """
    Encapsulates all information relevant *during* the translation of a MAL
    program and provides some utility functions on that state.
    """
    
    # The translation works as a state machine, whose states are called steps
    # here to avoid confusion with the overall translation state. These are the
    # possible steps. They also represent the common structure (from top to
    # bottom) of the MAL program as generated by MonetDB (see function
    # translate() for explanations of these steps):
    STEP_SKIP_PROLOGUE_COMMENTS    = 1
    STEP_SKIP_QUERY_FUNCTION_START = 2
    STEP_PROCESS_ASSIGNMENTS       = 3
    STEP_PROCESS_OUTPUT            = 4
    STEP_SKIP_QUERY_FUNCTION_END   = 5
    STEP_SKIP_EPILOGUE_COMMENTS    = 6
    
    def __init__(self):
        # The current step of the translation.
        self.step = _TranslationState.STEP_SKIP_PROLOGUE_COMMENTS
        
        # The current line in the MAL program.
        self.line = None
        
        # The C++ headers required in the translated program.
        self.headers = set()
        
        # A mapping from MAL variable names to other names. Usually, each
        # (BAT) variable in the MAL program corresponds to one (column)
        # variable with the same name in the translated C++ program. However,
        # there are two exceptions:
        # - In the C++ program we want to give the base columns speaking names,
        #   employing the table and column name (in MAL they have cryptic
        #   names). In these cases, a variable name is mapped to a tuple of a
        #   table name and a column name.
        # - Some MAL operators do not require action in MorphStore C++, e.g.,
        #   casting. The result variables of these MAL operators shall simply
        #   be replaced by their input variables in the translated program. In
        #   these cases, a variable name is mapped to another variable name.
        self.nameMap = dict()
        
        # The names of variables in the MAL program which are known to contain
        # all oids of a table from 0 to |table|-1.
        self.fullOidBats = []
        
        # A list of variable names in the MAL program which are outputs of
        # MAL's sort operators.
        self.sortResults = []
        
        # The translated program. This list can contain instances of ops.Op as
        # well as ordinary strings.
        self.prog = []
        
        # The MAL variable names of the result columns.
        self.resultCols = []
        
    def mapNameIf(self, malVarName):
        """
        Returns the name the given variable from the MAL program has in the
        translated C++ program.
        
        If the given name is contained in the nameMap, then the name it maps to
        is returned. Otherwise, the given name itself is returned.
        """

        if malVarName in self.nameMap:
            mappedName = self.nameMap[malVarName]
            if isinstance(mappedName, tuple):
                tblName = mappedName[0]
                colName = mappedName[1]
                return "{}.{}".format(tblName, colName)
            else:
                return mappedName
        else:
            return malVarName
        
    def _fullmatch(self, text, pattern, role):
        """
        Matches the given regular expression pattern against the given text.
        Returns the match object or raises an error if the pattern does not
        match.
        """

        match = pattern.fullmatch(text)
        if match is None:
            raise RuntimeError(
                "Could not parse the {} string '{}' in line\n{}".format(
                    role, text, self.line
                )
            )
        return match

    def fullmatchRes(self, text, pattern):
        return self._fullmatch(text, pattern, "result")

    def fullmatchPar(self, text, pattern):
        return self._fullmatch(text, pattern, "parameter")

    def error(self):
        """Raises an error based on the current translation state."""
        
        raise RuntimeError(
            "Error in step {} at line \n{}".format(self.step, self.line)
        )

class TranslationResult():
    """
    Encapsulates all information relevant *after* the translation, i.e. the 
    abstract representation of a MorphStore C++ program.
    """
    
    def __init__(self, ts):
        # The C++ headers required in the translated program as a set. Note
        # that this set does not account for the very operators themselves.
        # (These are handled in output.py, since they need to take the
        # processing style into account.)
        self.headers = ts.headers
                    
        # The translated program. This list can contain instances of ops.Op as
        # well as ordinary strings.
        self.prog = ts.prog
        
        # The variable names of the result columns.
        self.resultCols = ts.resultCols
        
        # The database schema seen by the translated program. That is, a
        # mapping from the names of all tables used in the translated program
        # to all columns (of the respective table) used in the translated
        # program.
        self.colNamesByTblName = {}
        # TODO Verify that all table and column names are valid C++ identifiers
        #      such that they can be used as variable names in the C++ code.
        #      Or should such checks be done in module output?
        baseCols = [x for x in ts.nameMap.values() if isinstance(x, tuple)]
        for tblName, colName in baseCols:
            if tblName not in self.colNamesByTblName:
                self.colNamesByTblName[tblName] = set()
            self.colNamesByTblName[tblName].add(colName)


# *****************************************************************************
# One translation function for each (family of) MAL operator(s)
# *****************************************************************************
# - The naming convention for these functions is "_translate" followed by the
#   MAL module name followed by the MAL function name (camel case).
# - Most of these translation functions are preceeded by a regular expression
#   pattern for parsing their parameters and some additional data structures,
#   if applicable.

# TODO Maybe all these translation function should rather be methods of class
#      _TranslationState, because they also access and modify the state.

# Regular expression patterns for parsing the left-hand sides of MAL
# assignments, i.e., the result variables of the MAL operators.
_pResScalar = re.compile(r"(X_\d+):(?:.+?)")
_str1Res = r"([XC]_\d+):bat\[:(?:.+?)\]"
_pRes1 = re.compile(_str1Res)
_pRes2 = re.compile(r"\({}, {}\)".format(_str1Res, _str1Res))
_pRes3 = re.compile(r"\({}, {}, {}\)".format(_str1Res, _str1Res, _str1Res))
    
_pParAggrSubsum = re.compile(
    r"(X_\d+):bat\[:(?:.+?)\], (X_\d+):bat\[:oid\], (C_\d+):bat\[:oid\], true:bit, true:bit"
)
def _translateAggrSubsum(ts, resStr, parStr):
    """Translation function for MAL's "aggr.subsum"."""
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    mPar = ts.fullmatchPar(parStr, _pParAggrSubsum)
    ts.prog.append(ops.SumGrBased(
        outDataCol = mRes.group(1),
        inGrCol    = ts.mapNameIf(mPar.group(2)),
        inDataCol  = ts.mapNameIf(mPar.group(1)),
        inExtCol   = ts.mapNameIf(mPar.group(3))
    ))
    
_pParAggrSum = re.compile(r"(X_\d+):bat\[:(?:.+?)\]")
def _translateAggrSum(ts, resStr, parStr):
    """Translation function for MAL's "aggr.sum"."""
    
    mRes = ts.fullmatchRes(resStr, _pResScalar)
    mPar = ts.fullmatchPar(parStr, _pParAggrSum)
    ts.prog.append(ops.SumWholeCol(
        outDataCol = mRes.group(1),
        inDataCol  = ts.mapNameIf(mPar.group(1))
    ))
    
_pParAlgebraJoin = re.compile(
    r"(X_\d+):bat\[:(?:.+?)\], (X_\d+):bat\[:(?:.+?)\], nil:BAT, nil:BAT, false:bit, nil:lng"
)
def _translateAlgebraJoin(ts, resStr, parStr):
    """Translation function for MAL's "algebra.join"."""
    
    mRes = ts.fullmatchRes(resStr, _pRes2)
    mPar = ts.fullmatchPar(parStr, _pParAlgebraJoin)
    ts.prog.append(ops.Join(
        outPosLCol = mRes.group(1),
        outPosRCol = mRes.group(2),
        inDataLCol = ts.mapNameIf(mPar.group(1)),
        inDataRCol = ts.mapNameIf(mPar.group(2))
    ))
            
_pParAlgebraProjectionpath = re.compile(
    r"((?:[XC]_\d+:bat\[:oid\], )+)(X_\d+):bat\[:(?:.+?)\]"
)
_pParAlgebraProjectionpathInner = re.compile(r"([XC]_\d+):bat\[:oid\], ")
def _translateAlgebraProjectionpath(ts, resStr, parStr):
    """
    Translation function for MAL's "algebra.projection" and
    "algebra.projectionpath".
    
    "projection" applies one positions list, while "projectionpath" applies at
    least two. However, we can translate them in a generalized way and in our
    translation both may result in applying just one or even no positions list.
    
    Since MorphStore supports only a single positions list per projection, we
    need to produce multiple operator calls in the translated program, if
    multiple positions lists shall be applied.
    """
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    mPar = ts.fullmatchPar(parStr, _pParAlgebraProjectionpath)
    outDataCol   = mRes.group(1)
    inDataCol    = ts.mapNameIf(mPar.group(2))
    inPosCols    = []
    inPosColsStr = mPar.group(1)
    
    # Find out the individual positions columns.
    mInner = _pParAlgebraProjectionpathInner.match(inPosColsStr)
    while mInner is not None:
        inPosCol = mInner.group(1)
        if inPosCol not in ts.fullOidBats and inPosCol not in ts.sortResults:
            inPosCols.append(ts.mapNameIf(inPosCol))
        inPosColsStr = inPosColsStr[mInner.end():]
        mInner = _pParAlgebraProjectionpathInner.match(inPosColsStr)
        
    pathLen = len(inPosCols)
    if pathLen == 0:
        # This projection is a no-op. No action is required in MorphStore C++,
        # we simply need to keep track of the names.
        ts.nameMap[mRes.group(1)] = inDataCol
    elif pathLen == 1:
        # Exactly one projection must be done in MorphStore.
        ts.prog.append(ops.Project(
            outDataCol = outDataCol,
            inDataCol  = inDataCol,
            inPosCol   = inPosCols[0]
        ))
    else:
        # Multiple projections must be done in MorphStore and we have to
        # introduce some additional intermediate results which are not present
        # in the MAL program.
        for idx, inPosCol in enumerate(reversed(inPosCols)):
            ts.prog.append(ops.Project(
                outDataCol =
                    "{}_{}".format(outDataCol, idx)
                    if idx < pathLen - 1
                    else outDataCol,
                inDataCol =
                    "{}_{}".format(outDataCol, idx - 1)
                    if idx > 0
                    else inDataCol,
                inPosCol = inPosCol
            ))
    
_pParAlgebraSelect = re.compile(
    r"([XC]_\d+):bat\[:(?:.+?)\], (?:(C_\d+):bat\[:oid\], )?(\d+):(?:.+?), (\d+):(?:.+?), true:bit, true:bit, false:bit"
)
def _translateAlgebraSelect(ts, resStr, parStr,vectorSelect, style):
    """
    Translation function for MAL's "algebra.select".
    
    This MAL operator representes a range selection with a lower and an upper
    bound, i.e., BETWEEN. Since MorphStore does not support such predicates
    directly, we have to translate this to two selections (with one constant
    each) whose results are intersected afterwards.
    
    Futhermore, a candidate list might be given to the MAL operator. If this is
    the case, we have to insert an additional intersect operator call in
    MorphStore.
    """
    
    # TODO Rethink the order in which we apply the generated MorphStore
    #      operators. Maybe it would be better to apply candidates first etc..
    # TODO Currently we support only >= and <=, we should also support > and <
    #      (see the second to last and third to last parameters).
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    mPar = ts.fullmatchPar(parStr, _pParAlgebraSelect)
    outPosCol = mRes.group(1)
    inDataCol = ts.mapNameIf(mPar.group(1))
    inCandCol = mPar.group(2)
    valLo = mPar.group(3)
    valHi = mPar.group(4)
    
    hasUsefulCands = \
        (inCandCol is not None) and (inCandCol not in ts.fullOidBats)
    
    ts.headers.add("functional")
    # Select for the lower bound.
    outPosColLo = "{}_lo".format(outPosCol)
    if (vectorSelect == 1):
        ts.prog.append(ops.Select(
            outPosCol = outPosColLo,
            inDataCol = inDataCol,
            op        = "std::greater_equal",
            val       = valLo
        ))
    else:
        ts.prog.append(ops.Select(
            outPosCol = outPosColLo,
            inDataCol = inDataCol,
            op        = "greaterequal",
            val       = valLo
        ))
    # Select for the upper bound.
    outPosColHi = "{}_hi".format(outPosCol)
    if (vectorSelect == 1):
        ts.prog.append(ops.Select(
            outPosCol = outPosColHi,
            inDataCol = inDataCol,
            op        = "std::less_equal",
            val       = valHi
        ))
    else:
        ts.prog.append(ops.Select(
            outPosCol = outPosColHi,
            inDataCol = inDataCol,
            op        = "lessequal",
            val       = valHi
        ))
    # Intersection of lower and upper bound.
    outPosColInterm = "{}_0".format(outPosCol)
    ts.prog.append(ops.Intersect(
        outPosCol = outPosColInterm if hasUsefulCands else outPosCol,
        inPosLCol = outPosColLo,
        inPosRCol = outPosColHi
    ))
    # Intersection with candidate list, if required.
    if hasUsefulCands:
        ts.prog.append(ops.Intersect(
            outPosCol = outPosCol,
            inPosLCol = outPosColInterm,
            inPosRCol = inCandCol
        ))
    
def _translateAlgebraSort(ts, resStr):
    """
    Translation function for MAL's "algebra.sort".
    
    Since MorphStore does not support sorting yet, we cannot translate this
    operator. Instead we only keep track of its output variables. Whenever one
    of these is used in a projection, we omit it. Thus, we will later output
    the unsorted columns.
    """
    
    # TODO Can it happen that one of the results is directly output, without
    #      going into a projection?

    m3Res = ts.fullmatchRes(resStr, _pRes3)
    ts.sortResults.append(m3Res.group(1))
    ts.sortResults.append(m3Res.group(2))
    ts.sortResults.append(m3Res.group(3))
    
_cmpOpMap = {
    "<" : "std::less",
    "<=": "std::less_equal",
    "==": "std::equal_to",
    ">=": "std::greater_equal",
    ">" : "std::equal",
}
_cmpOpMapVec = {
    "<" : "less",
    "<=": "lessequal",
    "==": "equal",
    ">=": "greaterequal",
    ">" : "equal",
}  
_pParThetaselect = re.compile(
    r'(X_\d+):bat\[:(?:.+?)\], (?:(C_\d+):bat\[:oid\], )?(\d+):(?:.+?), "({})":str'.format(
        "|".join(_cmpOpMap)
    )
)
_pParThetaselectVec = re.compile(
    r'(X_\d+):bat\[:(?:.+?)\], (?:(C_\d+):bat\[:oid\], )?(\d+):(?:.+?), "({})":str'.format(
        "|".join(_cmpOpMapVec)
    )
)
def _translateAlgebraThetaselect(ts, resStr, parStr, vectorSelect, style):
    """
    Translation function for MAL's "algebra.thetaselect".
    
    A candidate list might be given to the MAL operator. If this is the case,
    we have to insert an additional intersect operator call in sMorphStore.
    """
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    if (vectorSelect==1):
        mPar = ts.fullmatchPar(parStr, _pParThetaselect)
    else:
        mPar = ts.fullmatchPar(parStr, _pParThetaselectVec)
    outPosCol = mRes.group(1)
    inCandCol = ts.mapNameIf(mPar.group(2))
    
    hasUsefulCands = \
        (inCandCol is not None) and (inCandCol not in ts.fullOidBats)
        
    ts.headers.add("functional")
    # The actual selection.
    if (vectorSelect==1):
        outPosColInterm = "{}_0".format(outPosCol)
        ts.prog.append(ops.Select(
            outPosCol = outPosColInterm if hasUsefulCands else outPosCol,
            inDataCol = ts.mapNameIf(mPar.group(1)),
            op        = _cmpOpMap[mPar.group(4)],
            val       = mPar.group(3)
        ))
    else:
        outPosColInterm = "{}_0".format(outPosCol)
        ts.prog.append(ops.Select(
            outPosCol = outPosColInterm if hasUsefulCands else outPosCol,
            inDataCol = ts.mapNameIf(mPar.group(1)),
            op        = _cmpOpMapVec[mPar.group(4)],
            val       = mPar.group(3)
        ))
    # Intersection with candidate list, if required.
    if hasUsefulCands:
        ts.prog.append(ops.Intersect(
            outPosCol = outPosCol,
            inPosLCol = outPosColInterm,
            inPosRCol = inCandCol
        ))
    
_pParMerge = re.compile(r"(C_\d+):bat\[:oid\], (C_\d+):bat\[:oid\]")
def _translateBatMergecand(ts, resStr, parStr):
    """Translation function for MAL's "bat.mergecand"."""
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    mPar = ts.fullmatchPar(parStr, _pParMerge)
    ts.prog.append(ops.Merge(
        outPosCol = mRes.group(1),
        inPosLCol = ts.mapNameIf(mPar.group(1)),
        inPosRCol = ts.mapNameIf(mPar.group(2))
    ))
    
_pParBatCalcUnary = re.compile(r"(X_\d+):bat\[:(?:.+?)\]")
_pParBatCalcBinary = re.compile(
    r"(X_\d+):bat\[:(?:.+?)\], (X_\d+):bat\[:(?:.+?)\]"
)
_arithmOpMap = {
    # TODO Support more arithmetic operators.
    # TODO Support comparison operators here as well. (Required if two columns
    #      are compared to each other.)
    "+": "std::plus",
    "-": "std::minus",
    "*": "std::multiplies",
}
_arithmOpMapVec = {
    # TODO Support more arithmetic operators.
    # TODO Support comparison operators here as well. (Required if two columns
    #      are compared to each other.)
    "+": "add",
    "-": "sub",
    "*": "mul",
}
def _translateBatcalc(ts, resStr, funStr, parStr,vectorSelect, style):
    """Translation function for all MAL functions in MAL's "batcalc" module."""
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    if funStr in _MAL_INT_TYPES:
        # Conversion/cast MAL statement.
        mPar = ts.fullmatchPar(parStr, _pParBatCalcUnary)
        ts.nameMap[mRes.group(1)] = ts.mapNameIf(mPar.group(1))
    else:
        mPar = ts.fullmatchPar(parStr, _pParBatCalcBinary)
        ts.headers.add("functional")
        if (vectorSelect == 1):
            ts.prog.append(ops.CalcBinary(
            outDataCol = mRes.group(1),
            op         = _arithmOpMap[funStr],
            inDataLCol = ts.mapNameIf(mPar.group(1)),
            inDataRCol = ts.mapNameIf(mPar.group(2))
            ))
        else:
            ts.prog.append(ops.CalcBinary(
            outDataCol = mRes.group(1),
            op         = _arithmOpMapVec[funStr],
            inDataLCol = ts.mapNameIf(mPar.group(1)),
            inDataRCol = ts.mapNameIf(mPar.group(2))
            ))
        
    
_pParGroupGroup = re.compile(r"(X_\d+):bat\[:(?:.+?)\]")
def _translateGroupGroup(ts, resStr, parStr):
    """Translation function for MAL's unary grouping "group.group"."""
    
    mRes = ts.fullmatchRes(resStr, _pRes3)
    mPar = ts.fullmatchPar(parStr, _pParGroupGroup)
    ts.prog.append(ops.GroupUnary(
        outGrCol  = mRes.group(1),
        outExtCol = mRes.group(2),
        inDataCol = ts.mapNameIf(mPar.group(1))
    ))
    
_pParGroupSubgroup = re.compile(
    r"(X_\d+):bat\[:(?:.+?), (X_\d+):bat\[:(?:.+?)\]"
)
def _translateGroupSubgroup(ts, resStr, parStr):
    """Translation function for MAL's binary grouping "group.subgroupdone"."""
    
    mRes = ts.fullmatchRes(resStr, _pRes3)
    mPar = ts.fullmatchPar(parStr, _pParGroupSubgroup)
    ts.prog.append(ops.GroupBinary(
        outGrCol  = mRes.group(1),
        outExtCol = mRes.group(2),
        inGrCol   = ts.mapNameIf(mPar.group(2)),
        inDataCol = ts.mapNameIf(mPar.group(1))
    ))

_pParSqlBind = re.compile(
    r'X_\d+:int, ".+?":str, "(.+?)":str, "(.+?)":str, 0:int'
)
def _translateSqlBind(ts, resStr, parStr):
    """
    Translation function for MAL's "sql.bind".
    
    No action is required in MorphStore. We only need to remember that the
    output variable of this MAL operator denotes a base column.
    """
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    mPar = ts.fullmatchPar(parStr, _pParSqlBind)
    tabName = mPar.group(1)
    colName = mPar.group(2)
    ts.nameMap[mRes.group(1)] = (tabName, colName)
    
def _translateSqlTid(ts, resStr):
    """
    Translation function for MAL's "sql.tid".
    
    No action is required in MorphStore. We only need to remember that the
    output variable of this MAL operator is a full list of OIds for the
    respective table. We ignore such full lists if they are used in projections
    or as candidate lists.
    """
    
    mRes = ts.fullmatchRes(resStr, _pRes1)
    ts.fullOidBats.append(mRes.group(1))


# *****************************************************************************
# Program translation
# *****************************************************************************

_pAssignment = re.compile(r"(.+?) := (.+?)\.(.+?)\((.*?)\);")
_pResultSet = re.compile(
    r"sql.resultSet\(.+?:.+?, .+?:.+?, .+?:.+?, .+?:.+?, .+?:.+?(?:, \d+:int)?((?:, X_\d+:.+?)+)\);"
)
_pResultSetInner = re.compile(r", (X_\d+):(?:{}|bat\[.+?\])".format(
    "|".join(_MAL_INT_TYPES))
)

def translate(inMalFilePath,versionSelect,style):
    """
    Translates the MAL program in the specified file and returns an abstract
    representation of the translated C++ program as an instance of
    TranslationResult.
    """
    
    with open(inMalFilePath, "r") as inFile:
        # Initialize a new translation state.
        ts = _TranslationState()
        
        # Read the input MAL file line by line. The translation works like a
        # state machine, whose states are called steps here. See the STEP_...
        # fields in class _TranslationState.
        for line in inFile:
            # Get rid of leading and trailing whitespace.
            line = line.strip();
            ts.line = line
            
            # In some of the steps, exactly one line (matching a certain
            # format) is expected.
            # - If this line is found, the step changes and processing
            #   continues with the next line.
            # - If this line is not found, an error is raised.
            # In some of the steps, an a-priori unknown number of lines (each
            # matching a certain common format) can be processed.
            # - If the current line matches this format, it is consumed and the
            #   processing continues with the next line.
            # - Otherwise, the step is changed and the current line is tried
            #   according to the rules of the new step.

            # A MAL program starts with some comments beginning with "%". These
            # are irrelevant for us and must be skipped.
            # This step consumes an a-priori unknown number of lines.
            if ts.step == _TranslationState.STEP_SKIP_PROLOGUE_COMMENTS:
                if line[0] != "%":
                    ts.step = _TranslationState.STEP_SKIP_QUERY_FUNCTION_START

            # After that, the MAL query function is formally defined. This has
            # no relevant information for us, therefore we skip it.
            # This step expects exactly one line.
            if ts.step == _TranslationState.STEP_SKIP_QUERY_FUNCTION_START:
                if line.startswith("function user."):
                    ts.step = _TranslationState.STEP_PROCESS_ASSIGNMENTS
                    continue
                else:
                    _error(ts, line)

            # Within the MAL query function, there are assignments constituting
            # the core MAL program of the query. We translate these into
            # MorphStore query operators.
            # This step consumes an a-priori unknown number of lines.
            if ts.step == _TranslationState.STEP_PROCESS_ASSIGNMENTS:
                mAssignment = _pAssignment.fullmatch(line)
                if mAssignment is None:
                    ts.step = _TranslationState.STEP_PROCESS_OUTPUT
                else:
                    resStr = mAssignment.group(1) # the result list
                    modStr = mAssignment.group(2) # the MAL module
                    funStr = mAssignment.group(3) # the MAL function
                    parStr = mAssignment.group(4) # the parameter list
                    
                    progLenBefore = len(ts.prog)

                    # Sorted in alphabetical order w.r.t. (modStr, funStr)
                    if (modStr, funStr) == ("aggr", "subsum"):
                        _translateAggrSubsum(ts, resStr, parStr)
                    elif (modStr, funStr) == ("aggr", "sum"):
                        _translateAggrSum(ts, resStr, parStr)
                    elif (modStr, funStr) == ("algebra", "join"):
                        _translateAlgebraJoin(ts, resStr, parStr)
                    elif modStr == "algebra" and funStr in [
                        "projection",
                        "projectionpath"
                    ]:
                        _translateAlgebraProjectionpath(ts, resStr, parStr)
                    elif (modStr, funStr) == ("algebra", "select"):
                        _translateAlgebraSelect(ts, resStr, parStr, versionSelect, style)
                    elif (modStr, funStr) == ("algebra", "sort"):
                        _translateAlgebraSort(ts, resStr)
                    elif (modStr, funStr) == ("algebra", "thetaselect"):
                        _translateAlgebraThetaselect(ts, resStr, parStr, versionSelect, style)
                    elif (modStr, funStr) == ("bat", "append"):
                        pass
                    elif (modStr, funStr) == ("bat", "mergecand"):
                        _translateBatMergecand(ts, resStr, parStr)
                    elif (modStr, funStr) == ("bat", "new"):
                        pass
                    elif modStr == "batcalc":
                        _translateBatcalc(ts, resStr, funStr, parStr, versionSelect, style)
                    elif (modStr, funStr) == ("group", "group"):
                        _translateGroupGroup(ts, resStr, parStr)
                    elif modStr == "group" and funStr in [
                        "subgroup",
                        "subgroupdone"
                    ]:
                        _translateGroupSubgroup(ts, resStr, parStr)
                    elif (modStr, funStr) == ("querylog", "define"):
                        pass
                    elif (modStr, funStr) == ("sql", "bind"):
                        _translateSqlBind(ts, resStr, parStr)
                    elif (modStr, funStr) == ("sql", "mvc"):
                        pass
                    elif (modStr, funStr) == ("sql", "tid"):
                        _translateSqlTid(ts, resStr)
                    else:
                        raise RuntimeError(
                            "unknown MAL function: '{}.{}' in line\n{}".format(
                                modStr,
                                funStr,
                                line
                            )
                        )

                    if len(ts.prog) > progLenBefore:
                        # Produce a blank line after the translation of each
                        # MAL assignment to make it easier recognizable, which
                        # C++ statements were produced by the same MAL
                        # assignment (sometimes a single MAL assignment yields
                        # multiple C++ statements).
                        ts.prog.append("")

            # The MAL query function ends with the output of the result
            # columns. We need to know which these are.
            # This step expects exactly one line.
            if ts.step == _TranslationState.STEP_PROCESS_OUTPUT:
                mResultSet = _pResultSet.fullmatch(line)
                if mResultSet is not None:
                    inDataColsStr = mResultSet.group(1)
                    mInner = _pResultSetInner.match(inDataColsStr)
                    while mInner is not None:
                        inDataCol = mInner.group(1)
                        ts.resultCols.append(ts.mapNameIf(inDataCol))
                        inDataColsStr = inDataColsStr[mInner.end():]
                        mInner = _pResultSetInner.match(inDataColsStr)
                    ts.step = _TranslationState.STEP_SKIP_QUERY_FUNCTION_END
                    continue
                else:
                    _error(ts)

            # After that, the MAL query function formally ends. No relevant
            # information for us.
            # This step expects exactly one line.
            if ts.step == _TranslationState.STEP_SKIP_QUERY_FUNCTION_END:
                if line.startswith("end user."):
                    ts.step = _TranslationState.STEP_SKIP_EPILOGUE_COMMENTS
                    continue
                else:
                    _error(ts)

            # Finally, a MAL program ends with some comments beginning with
            # "#". These are irrelevant for us and must be skipped.
            # This step consumes an a-priori unknown number of lines.
            if ts.step == _TranslationState.STEP_SKIP_EPILOGUE_COMMENTS:
                if line[0] != "#":
                    _error(ts)
    
    # Replace inner joins by semi-joins where it is possible.
    ar = analysis.analyze(TranslationResult(ts))
    for idx, el in enumerate(ts.prog):
        if isinstance(el, ops.Join):
            # TODO From the structure of the program, we know that we can use
            #      a semi-join in the following two cases. However, using an
            #      N:1-join is not correct in all possible cases. While it is
            #      correct for SSB, we should find a generally sound solution.
            
            # TODO We do not consider 1:1-joins here (when both inputs are
            #      unique), but this case is not relevant for us at the moment.
            if el.inDataLCol in ar.varsUnique:
                # It is an 1:N-join (1 data element in the left input matches
                # N data elements in the right input).
                # The left input can be used as the build-side of a hash-join.
                #if el.outPosLCol in ar.varsNeverUsed:
                #    ts.prog[idx] = ops.LeftSemiNto1Join(el.outPosRCol, el.inDataLCol, el.inDataRCol)
                #else:
                    ts.prog[idx] = ops.Nto1Join(el.outPosLCol, el.outPosRCol, el.inDataLCol, el.inDataRCol)
            elif el.inDataRCol in ar.varsUnique:
                # It is an 1:N-join (1 data element in the right input matches
                # N data elements in the left input).
                # The right input can be used as the build-side of a hash-join.
                #if el.outPosRCol in ar.varsNeverUsed:
                #    ts.prog[idx] = ops.LeftSemiNto1Join(el.outPosLCol, el.inDataRCol, el.inDataLCol)
                #else:
                    ts.prog[idx] = ops.Nto1Join(el.outPosRCol, el.outPosLCol, el.inDataRCol, el.inDataLCol)
            else:
                # It is an M:N-join.
                # We do not want this at the moment.
                # TODO Support it in a nice way.
                raise RuntimeError("the query contains an M:N-join")

    return TranslationResult(ts)