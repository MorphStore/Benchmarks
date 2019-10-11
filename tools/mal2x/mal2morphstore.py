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

See the documentations of the modules in package mal2x for further
details.
"""


import mal2morphstore.compr as compr
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
    
    trueVals = ["true", "t", "yes", "y", "1"]
    falseVals = ["false", "f", "no", "n", "0"]
    def parseBool(str):
        if str in trueVals:
            return True
        if str in falseVals:
            return False
        raise RuntimeError("invalid bool")
    
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
    
    # General optional arguments
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
    parser.add_argument(
        "--statdir", dest="statDirPath", default=None, metavar="DIR",
        help="The path to the directory containing statistics on the base "
            "columns as created by dbdict.py."
        # TODO Validate existence.
    )
    
    # Compression arguments
    comprArgGr = parser.add_argument_group(
        "compression arguments",
        "The following arguments control the selection of the (un)compressed "
        "formats for each base column and each intermediate result in the "
        "translated query program. The compression strategy determines the "
        "basic approch how to do this. Depending on the strategy, additional "
        "arguments may be allowed. "
        "The following FORMATs are supported: {}".format(
            ", ".join(map(quote, compr.COMPR_FORMATS))
        )
    );
    comprArgGr.add_argument(
        "-c", dest="comprStrategy",
        metavar="COMPRESSION_STRATEGY",
        choices=compr.COMPR_STRATEGIES, default=compr.CS_UNCOMPR,
        help="The strategy to use for deciding the compressed formats of all "
            "base and intermediate columns in the translated query. The "
            "following strategies are supported: {}.".format(
                ", ".join(map(quote, compr.COMPR_STRATEGIES)),
            )
    )
    comprArgGr.add_argument(
        "-crnd", dest="comprRndFormat", metavar="FORMAT",
        choices=compr.COMPR_FORMATS, default=None,
        help="The format to use for columns requiring random access. Only "
            "allowed for the '{}' strategy. Defaults to '{}'".format(
                compr.CS_RULEBASED, compr.FN_UNCOMPR
            )
    )
    comprArgGr.add_argument(
        "-csequ", dest="comprSeqUnsortedFormat", metavar="FORMAT",
        choices=compr.COMPR_FORMATS, default=None,
        help="The format to use for unsorted columns requiring only "
            "sequential access. Only  allowed for the '{}' strategy. Defaults "
            "to the value of -crnd".format(compr.CS_RULEBASED)
    )
    comprArgGr.add_argument(
        "-cseqs", dest="comprSeqSortedFormat", metavar="FORMAT",
        choices=compr.COMPR_FORMATS, default=None,
        help="The format to use for sorted columns requiring only "
            "sequential access. Only  allowed for the '{}' strategy. Defaults "
            "to the value of -csequ".format(compr.CS_RULEBASED)
    )
    comprArgGr.add_argument(
        "-ccbsl", "--ccascblocksizelog", dest="comprCascBlockSizeLog",
        metavar="N", default=1024,
        help="The block size of cascades. Defaults to 1024."
    )
    comprArgGr.add_argument(
        "-cubase", dest="comprUncomprBase", metavar="BOOL",
        choices=trueVals+falseVals, default=falseVals[0],
        help="Whether all base columns shall be uncompressed irrespective of "
            "the other compression parameters. Defaults to false."
    )
    comprArgGr.add_argument(
        "-cuinterm", dest="comprUncomprInterm", metavar="BOOL",
        choices=trueVals+falseVals, default=falseVals[0],
        help="Whether all intermediate columns shall be uncompressed "
            "irrespective of the other compression parameters. Defaults to "
            "false."
    )

    args = parser.parse_args()
    
    # Validation of the combination of the compression arguments.
    if args.comprStrategy == compr.CS_UNCOMPR:
        if (
            args.comprRndFormat is not None or
            args.comprSeqUnsortedFormat is not None or
            args.comprSeqSortedFormat is not None
        ):
            parser.error("Illegal combination of the compression arguments.")
    elif args.comprStrategy == compr.CS_RULEBASED:
        if args.comprRndFormat is None:
            args.comprRndFormat = compr.FN_UNCOMPR
        if args.comprSeqUnsortedFormat is None:
            args.comprSeqUnsortedFormat = args.comprRndFormat
        if args.comprSeqSortedFormat is None:
            args.comprSeqSortedFormat = args.comprSeqUnsortedFormat

    if args.inMalFilePath == FROM_STDIN:
        # 0 is the file descriptor of stdin, can be used with open().
        args.inMalFilePath = 0
        
    if args.purpose == pp.PP_SIZE and args.comprStrategy != compr.CS_UNCOMPR:
        raise RuntimeError("purpose '{}' requires '-c {}'".format(pp.PP_SIZE, compr.CS_UNCOMPR))

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

    # Query translation.
    translationResult = mal2morphstore.translation.translate(
        args.inMalFilePath, args.versionSelect, args.processingStyle
    )
    
    # Compression configuration.
    compr.CASC_BLOCKSIZE_LOG = args.comprCascBlockSizeLog
    compr.initShortNames()
    if args.comprStrategy == compr.CS_UNCOMPR:
        compr.configureUncompr(translationResult)
    elif args.comprStrategy == compr.CS_RULEBASED:
        compr.configureRuleBased(
            translationResult,
            args.processingStyle,
            args.statDirPath,
            args.comprRndFormat,
            args.comprSeqUnsortedFormat,
            args.comprSeqSortedFormat,
            parseBool(args.comprUncomprBase),
            parseBool(args.comprUncomprInterm),
        )
    else:
        # This case should have been handled by the parser before.
        raise RuntimeError(
            "Unsupported compression strategy: '{}'".format(args.comprStrategy)
        )
    compr.checkAllFormatsSet(translationResult)
    if args.comprStrategy != compr.CS_UNCOMPR:
        compr.insertMorphs(translationResult)
    compr.reorderMorphs(translationResult)
    
    # C++-code generation.
    mal2morphstore.output.generate(
        translationResult,
        os.path.join(
            os.path.dirname(sys.argv[0]),
            "template.cpp"
        ),
        args.purpose,
        args.processingStyle,
        args.versionSelect,
        args.statDirPath
    )