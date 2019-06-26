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
# See the GNDo this in a proper way.U General Public License for more details.                                       *
#                                                                                            *
# You should have received a copy of the GNU General Public License along with this program. *
# If not, see <http://www.gnu.org/licenses/>.                                                *
#*********************************************************************************************

"""
Classes representing calls to MorphStore query operators.

There is one class for each MorphStore query opertor. Each of these classes
represents a call to the respective operator and has to fulfil the following
criteria:
- It must be a subclass of Op.
- It must store the names of the C++ variables passed as the parameters to
  the query operator as its fields. The names of these fields must start with
  "in" and end with "Col" (e.g. "inDataCol") to allow automatic analysis of the
  translated program (see modules mal2morphstore.analysis and
  mal2morphstore.compr).
- It must store the names of the C++ variables of the results obtained from
  the query operator as its fields. The names of these fields must start with
  "out" and end with "Col" (e.g. "outPosCol") to allow automatic analysis of
  the translated program (see module mal2morphstore.analysis and
  mal2morphstore.compr).
- It must store the C++ identifiers of the formats of each parameter and each
  result as its fields. The names of these fields must have the same names as 
  those for the corresponding input/output columns, but end with "F" instead of
  "Col" (e.g. for "inDataCol", there must be "inDataF") to allow the automatic
  configuration of the formats (see module mal2morphstore.compr). These fields
  should be None by default.
- It must provide a __str__()-method returning the C++ code for the call to the
  respective operator with the respective input and output variables. The
  format strings used in the __str__()-methods should include only C++ keywords
  and the names of the MorphStore operators represented by the respective class
  as literals, all other (common) identifiers should be used from the
  dictionary _commonIdentifiers to make them easily exchangable.
- It must have a field named "headers" which is a list of C++ header files that
  need to be included in the generated C++ program for the respective operator.
  Only headers required for the C++ code returned by __str__() should be
  considered here; headers required for parameter values (such as <functional>
  for std::less) are taken into account in module mal2x.translation.

These classes are used in the abtract representation of a translated MAL
program. See module mal2x.translation for details.
  
Whenever a new query operator is added in the C++ code base of MorphStore, then
a new class for the calls to that new operator should be created in this
module.
"""

# TODO Support the optional cardinality estimate parameters of some operators.
# TODO Currently, joins are assumed to be 1:N (see class Join).
# TODO Automatically wrap too long lines in the generated C++ code in a nice
#      way.


import mal2morphstore.processingstyles as ps


# The values of this dictionary are common identifiers frequently needed in the
# C++ code. They can be inserted in the generated C++ code by using the keys of
# this dictionary. That way, changing a C++ identifier can be done easily.
_commonIdentifiers = {
    "ns": "morphstore",
    "ps": ps.PS_VAR,
    "format": "uncompr_f",
    "column": "column",
    "apply": "apply",
    "get_count_values": "get_count_values",
}

class Op:
    """
    The base class of all classes representing a call to a query operator in
    MorphStore.
    """
    pass

class Project(Op):
    """A call to MorphStore's project operator."""
    
    opName = "project"
    headers = [
        "core/operators/{{{}}}/project_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outDataCol, inDataCol, inPosCol):
        self.outDataCol = outDataCol
        self.inDataCol = inDataCol
        self.inPosCol = inPosCol
        self.outDataF = None
        self.inDataF = None
        self.inPosF = None
        
    def __str__(self):
        return "auto {outDataCol} = {opName}<{ps}, {outDataF}, {inDataF}, {inPosF}>({inDataCol}, {inPosCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class Select(Op):
    """A call to MorphStore's select operator."""
    
    opName = "select"
    headers = [
        "core/operators/{{{}}}/select_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outPosCol, op, inDataCol, val):
        self.outPosCol = outPosCol
        self.op = op
        self.inDataCol = inDataCol
        self.val = val
        self.outPosF = None
        self.inDataF = None
        
    def __str__(self):
        return "auto {outPosCol} = {ns}::{opName}<{op}, {ps}, {outPosF}, {inDataF}>({inDataCol}, {val});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class Intersect(Op):
    """A call to MorphStore's intersect operator."""
    
    opName = "intersect_sorted"
    headers = [
        "core/operators/{{{}}}/intersect_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outPosCol, inPosLCol, inPosRCol):
        self.outPosCol = outPosCol
        self.inPosLCol = inPosLCol
        self.inPosRCol = inPosRCol
        self.outPosF = None
        self.inPosLF = None
        self.inPosRF = None
        
    def __str__(self):
        return "auto {outPosCol} = {opName}<{ps}, {outPosF}, {inPosLF}, {inPosRF}>({inPosLCol}, {inPosRCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class Merge(Op):
    """A call to MorphStore's merge operator."""
    
    opName = "merge_sorted"
    headers = [
        "core/operators/{{{}}}/merge_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outPosCol, inPosLCol, inPosRCol):
        self.outPosCol = outPosCol
        self.inPosLCol = inPosLCol
        self.inPosRCol = inPosRCol
        self.outPosF = None
        self.inPosLF = None
        self.inPosRF = None
        
    def __str__(self):
        return "auto {outPosCol} = {opName}<{ps}, {outPosF}, {inPosLF}, {inPosRF}>({inPosLCol}, {inPosRCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class Join(Op):
    """A call to MorphStore's join operator."""
    
    opName = "nested_loop_join"
    headers = [
        "core/operators/{{{}}}/join_uncompr.h".format(ps.INCLUDE_DIR_KEY),
        "tuple"
    ]
    
    def __init__(self, outPosLCol, outPosRCol, inDataLCol, inDataRCol):
        self.outPosLCol = outPosLCol
        self.outPosRCol = outPosRCol
        self.inDataLCol = inDataLCol
        self.inDataRCol = inDataRCol
        self.outPosLF = None
        self.outPosRF = None
        self.inDataLF = None
        self.inDataRF = None
        
    def __str__(self):
        # TODO Handle the cardinality estimate in a proper way, currently each
        #      join is assumed to be 1:N.
        if False:
            # No cardinality estimate.
            return \
                "const {column}<{outPosLF}> * {outPosLCol};\n" \
                "const {column}<{outPosRF}> * {outPosRCol};\n" \
                "std::tie({outPosLCol}, {outPosRCol}) = {opName}<{ps}, {outPosLF}, {outPosRF}, {inDataLF}, {inDataRF}>({inDataLCol}, {inDataRCol});".format(
                opName=self.opName, **self.__dict__, **_commonIdentifiers
            )
        else:
            # Cardinality estimate for 1:N-join, assuming that the 1-side is
            # the larger column.
            return \
                "const {column}<{outPosLF}> * {outPosLCol};\n" \
                "const {column}<{outPosRF}> * {outPosRCol};\n" \
                "std::tie({outPosLCol}, {outPosRCol}) = {opName}<{ps}, {outPosLF}, {outPosRF}, {inDataLF}, {inDataRF}>(\n" \
                "    {inDataLCol},\n" \
                "    {inDataRCol},\n" \
                "    std::max({inDataLCol}->get_count_values(), {inDataRCol}->get_count_values())\n" \
                ");".format(
                opName=self.opName, **self.__dict__, **_commonIdentifiers
            )

class Nto1Join(Op):
    """A call to MorphStore's N:1-join operator."""
    
    opName = "join"
    headers = [
        "core/operators/general_vectorized/join_uncompr.h",
    ]

    
    def __init__(self, outPosLCol, outPosRCol, inDataLCol, inDataRCol):
        self.outPosLCol = outPosLCol
        self.outPosRCol = outPosRCol
        self.inDataLCol = inDataLCol
        self.inDataRCol = inDataRCol
        self.outPosLF = None
        self.outPosRF = None
        self.inDataLF = None
        self.inDataRF = None
        
    def __str__(self):
        return \
            "const {column}<{outPosLF}> * {outPosLCol};\n" \
            "const {column}<{outPosRF}> * {outPosRCol};\n" \
            "std::tie({outPosLCol}, {outPosRCol}) = {opName}<\n" \
            "    {ps},\n" \
            "    {outPosLF},\n" \
            "    {outPosRF},\n" \
            "    {inDataLF},\n" \
            "    {inDataRF}\n" \
            "    >(\n" \
            "    {inDataLCol},\n" \
            "    {inDataRCol},\n" \
            "    {inDataRCol}->get_count_values()\n" \
            ");\n".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
#class LeftSemiNto1Join(Op):
#    """A call to MorphStore's left-semi-N:1-join operator."""
#    
#    opName = "left_semi_nto1_nested_loop_join"
#    headers = [
#        "core/operators/{{{}}}/join_uncompr.h".format(ps.INCLUDE_DIR_KEY),
#    ]
#    
#    def __init__(self, outPosLCol, inDataLCol, inDataRCol):
#        self.outPosLCol = outPosLCol
#        self.inDataLCol = inDataLCol
#        self.inDataRCol = inDataRCol
#        
#    def __str__(self):
#        return \
#            "auto {outPosLCol} = {opName}<{ps}, {format}>({inDataLCol}, {inDataRCol});".format(
#            opName=self.opName, **self.__dict__, **_commonIdentifiers
#        )

class LeftSemiNto1Join(Op):
    """A call to MorphStore's left-semi-N:1-join operator."""
    
    opName = "semi_join"
    headers = [
        "core/operators/general_vectorized/join_uncompr.h",
    ]
    
    def __init__(self, outPosRCol, inDataLCol, inDataRCol):
        self.outPosRCol = outPosRCol
        self.inDataLCol = inDataLCol
        self.inDataRCol = inDataRCol
        self.outPosRF = None
        self.inDataLF = None
        self.inDataRF = None
        
    def __str__(self):
        return \
            "auto {outPosRCol} = {opName}<\n" \
            "    {ps},\n" \
            "    {outPosRF},\n" \
            "    {inDataLF},\n" \
            "    {inDataRF}\n" \
            "    >\n" \
            "({inDataLCol}, {inDataRCol});".format(
                    opName=self.opName, **self.__dict__, **_commonIdentifiers
            )
    
class CalcBinary(Op):
    """A call to MorphStore's binary calculation operator."""
    
    opName = "calc_binary"
    headers = [
        "core/operators/{{{}}}/calc_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outDataCol, op, inDataLCol, inDataRCol):
        self.outDataCol = outDataCol
        self.op = op
        self.inDataLCol = inDataLCol
        self.inDataRCol = inDataRCol
        self.outDataF = None
        self.inDataLF = None
        self.inDataRF = None
        
    def __str__(self):
        return "auto {outDataCol} = {ns}::{opName}<{op}, {ps}, {outDataF}, {inDataLF}, {inDataRF}>({inDataLCol}, {inDataRCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class SumWholeCol(Op):
    """A call to MorphStore's whole-column summation operator."""
    
    opName = "agg_sum"
    headers = [
        "core/operators/{{{}}}/agg_sum_uncompr.h".format(ps.INCLUDE_DIR_KEY)
    ]
    
    def __init__(self, outDataCol, inDataCol):
        self.outDataCol = outDataCol
        self.inDataCol = inDataCol
        # The output format is hardcoded to uncompr_f in MorphStore, since
        # compression does not make sense for a single data element.
        self.inDataF = None
        
    def __str__(self):
        return "auto {outDataCol} = {opName}<{ps}, {inDataF}>({inDataCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class SumGrBased(Op):
    """A call to MorphStore's group-based summation operator."""
    
    opName = "agg_sum"
    # TODO Do not hardcode the processing style (see todo below).
    headers = ["core/operators/scalar/agg_sum_uncompr.h"]
    
    def __init__(self, outDataCol, inGrCol, inDataCol, inExtCol):
        self.outDataCol = outDataCol
        self.inGrCol = inGrCol
        self.inDataCol = inDataCol
        self.inExtCol = inExtCol
        self.outDataF = None
        self.inGrF = None
        self.inDataF = None
        # The format of inExtCol does not matter, since we only use its
        # cardinality here.
        
    def __str__(self):
        # TODO Do not hardcode the processing style. At the moment, we have to
        #      do this, because this operator is only available for the scalar
        #      processing style.
        return \
            "// @todo Currently, the scalar processing style is hardcoded\n" \
            "// in the query translation, because MorphStore still lacks a\n" \
            "// vectorized implementation. As soon as such an\n" \
            "// implementation exists, we should use it here.\n" \
            "auto {outDataCol} = {opName}<scalar<v64<uint64_t>>, {outDataF}, {inGrF}, {inDataF}>({inGrCol}, {inDataCol}, {inExtCol}->{get_count_values}());".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class GroupUnary(Op):
    """A call to MorphStore's unary group operator."""
    
    opName = "group"
    headers = [
        "core/operators/{{{}}}/group_uncompr.h".format(ps.INCLUDE_DIR_KEY),
        "tuple"
    ]
    
    def __init__(self, outGrCol, outExtCol, inDataCol):
        self.outGrCol = outGrCol
        self.outExtCol = outExtCol
        self.inDataCol = inDataCol
        self.outGrF = None
        self.outExtF = None
        self.inDataF = None
        
    def __str__(self):
        return \
            "const {column}<{outGrF}> * {outGrCol};\n" \
            "const {column}<{outExtF}> * {outExtCol};\n" \
            "std::tie({outGrCol}, {outExtCol}) = {opName}<{ps}, {outGrF}, {outExtF}, {inDataF}>({inDataCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )
    
class GroupBinary(Op):
    """A call to MorphStore's binary group operator."""
    
    opName = "group"
    headers = [
        "core/operators/{{{}}}/group_uncompr.h".format(ps.INCLUDE_DIR_KEY),
        "tuple"
    ]
    
    def __init__(self, outGrCol, outExtCol, inGrCol, inDataCol):
        self.outGrCol = outGrCol
        self.outExtCol = outExtCol
        self.inGrCol = inGrCol
        self.inDataCol = inDataCol
        self.outGrF = None
        self.outExtF = None
        self.inGrF = None
        self.inDataF = None
        
    def __str__(self):
        return \
            "const {column}<{outGrF}> * {outGrCol};\n" \
            "const {column}<{outExtF}> * {outExtCol};\n" \
            "std::tie({outGrCol}, {outExtCol}) = {opName}<{ps}, {outGrF}, {outExtF}, {inGrF}, {inDataF}>({inGrCol}, {inDataCol});".format(
            opName=self.opName, **self.__dict__, **_commonIdentifiers
        )