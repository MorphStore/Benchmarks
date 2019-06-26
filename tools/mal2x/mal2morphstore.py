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
This tool translates a MonetDB MAL program to a MorphStore C++ program.

The input Monet Algebraic/Assembly Language (MAL) program must be a plain text
file. Such a file can be obtained by running MonetDB as follows:

mclient -f raw query.sql

Whereby query.sql should be a file containing a SQL query starting with
"EXPLAIN SELECT ". It is important that the output format is set to "raw",
since otherwise the parsing does not work.

The output is a full-fledged C++ source file, which can be compiled within
MorphStore. This file is written to stdout. The generated C++ program is based
on a template file, which provides the overall frame for the translated C++
program. The decisive parts of the translated program are inserted into this
template (see module "output" for details on that). At the moment, the file
"template.cpp" (in the same directory as this script) is always used as the
template file.

In addition to the program translation and C++ code generation, the translated
program is also analyzed to find out some relevant information, e.g., whether
there are unused variables etc.. The outcome of this analysis is included in
the output C++ code in the form of comments.

Known limitations:
- The translation is tailored to the exact MAL dialect of MonetDB-11.31.13. It
  might work with other versions (maybe after some slight changes), but this
  was not tested.
- The translation does not cover all cases that could be encountered in a MAL
  program, but for the moment it should suffice for our purposes.
- So far, the generated C++ code will employ only one of MorphStore's
  processing styles (which must be specified as an argument to this script).
- So far, only operators on uncompressed data are considered.

See the documentations of the modules in package mal2x for further
details.
"""


import mal2morphstore.operators as ops
import mal2morphstore.output
import mal2morphstore.processingstyles as ps
import mal2morphstore.purposes as pp
import mal2morphstore.translation

import argparse
import os
import sys


# *****************************************************************************
# Main programm
# *****************************************************************************

def addFlagArg(parser, argName, default, helpTrue, helpFalse):
    gr = parser.add_mutually_exclusive_group(required=False)
    gr.add_argument(
        "--{}".format(argName), dest=argName, action="store_true",
        help=helpTrue + (" (default)" if default is True else "")
    )
    gr.add_argument(
        "--no-{}".format(argName), dest=argName, action="store_false",
        help=helpFalse + (" (default)" if default is False else "")
    )
    parser.set_defaults(argName=default)

if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Parsing the command line arguments
    # -------------------------------------------------------------------------
    
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Required (positional) arguments.
    parser.add_argument(
        "processingStyle", metavar="PROCESSING_STYLE",
        choices=list(sorted(ps.INCLUDE_DIR_HANDCODED.keys())),
        help="The MorphStore processing style to use for all query operators. "
            "The following processing styles are supported: {}.".format(
                ", ".join(sorted(ps.INCLUDE_DIR_HANDCODED.keys()))
            )
    )
    def quote(s):
        return "'{}'".format(s)
    parser.add_argument(
        "purpose", metavar="PURPOSE",
        choices=pp.PURPOSES,
        help="The purpose of the translated query program. The following "
            "purposes are supported: {}, standing for {}, "
            "respectively.".format(
                ", ".join(map(quote, pp.PURPOSES)),
                ", ".join(map(quote, pp.PURPOSES_LONG))
            )
    )
    # Optional arguments
    FROM_STDIN = "-"
    parser.add_argument(
        "--malfile", dest="inMalFilePath", default=FROM_STDIN, metavar="FILE",
        help="The path to the plain text file containing the MAL program. If "
            "this argument is omitted or if '{}' is specified, then the "
            "MAL program will be read from stdin.".format(FROM_STDIN)
        # TODO Validate existence.
    )
    parser.add_argument(
        "versionSelect", type=int, nargs='?', default = '1',
        help="Are the hand implemented operators used (1), "
            "or the operators using the vector library (2)?"
    )

    args = parser.parse_args()

    if args.inMalFilePath == FROM_STDIN:
        # 0 is the file descriptor of stdin, can be used with open().
        args.inMalFilePath = 0

    # -------------------------------------------------------------------------
    # Program translation and output
    # -------------------------------------------------------------------------

    # If vector-lib implementations of the operators shall be used then the
    # group-operator's name must be "group_vec".
    # TODO Remove this work-around once the operator names have been harmonized
    # in MorphStore.
    if args.versionSelect == 2:
        ops.GroupUnary.opName = "group_vec"
        ops.GroupBinary.opName = "group_vec"

    mal2morphstore.output.generate(
        mal2morphstore.translation.translate(args.inMalFilePath, args.versionSelect, args.processingStyle),
        os.path.join(
            os.path.dirname(sys.argv[0]),
            "template.cpp"
        ),
        args.purpose,
        args.processingStyle,
        args.versionSelect
    )