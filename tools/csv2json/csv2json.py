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
This tool converts CSV files output by MorphStore's monitoring feature to JSON
files suitable for use in the web interface.

Two kinds of CSV files can be converted, both of which can be created using
"ssb.sh".
- Operator (runtime) measurements: CSV files in the directory "time_sfN"
  created by "ssb.sh" with "-p t".
- Data measurements: CSV files in the directory "dc_sfN" created by "ssb.sh"
  with "-p d".
Where N is the scale factor.

Known limitations:
- Some of the data characteristics are filled with dummy data, since, at the
  moment, they are not recorded in MorphStore.
"""


import argparse
import csv
import json
import random


# *****************************************************************************
# Constants
# *****************************************************************************

MODE_OPS = "ops"
MODE_DATA = "data"
MODES = [MODE_OPS, MODE_DATA]


# *****************************************************************************
# Translation functions
# *****************************************************************************

def convertOperatorFile(inCsvFile):
    ATTR_OPNAME = "opName"
    ATTR_OPIDX = "opIdx"
    ATTR_RUNTIME = "runtime"
    
    # TODO Remove this workaround.
    opNameMap = {
        "my_project_wit_t": "project",
        "group_vec": "group",
        "join": "equi_join",
    }
    
    reader = csv.DictReader(
            inCsvFile,
            fieldnames=[ATTR_OPNAME, ATTR_OPIDX, ATTR_RUNTIME],
            delimiter="\t"
    )
    
    # Skip the useless lines at the beginning.
    for i in range(4):
        next(reader)

    res = {}
    opIdx = 0
    for row in reader:
        opName = row[ATTR_OPNAME]
        
        # Skip the query results.
        if opName == "[RES]":
            break
        # Skip morph-operators.
        if opName == "morph":
            continue
        
        if opName == "query":
            key = opName
        else:
            key = "{}_{}".format(opNameMap.get(opName, opName), opIdx)
            opIdx += 1
            
        res[key] = {
            ATTR_RUNTIME: int(row[ATTR_RUNTIME])
        }
        
    return res

def convertDataChFile(inCsvFile):
    ATTR_OPNAME = "opName"
    ATTR_COLNAME = "colName"
    ATTR_VALUECOUNT = "valueCount"
    ATTR_SIZEUSEDBYTE = "sizeUsedByte"
    ATTR_FORMAT = "formatName"
    ATTR_BWHIST_FS = "bwHist_{}"
    ATTR_SORTED = "sorted"
    ATTR_UNIQUE = "unique"
    ATTR_PHYSIZE = "UsedBytes"
    ATTR_SORT = "Sorted"
    ATTR_UNI = "Unique"
    ATTR_FORM = "format"
    
    reader = csv.DictReader(
            inCsvFile,
            fieldnames=[
                ATTR_OPNAME,
                "opIdx",
                "colRole",
                ATTR_COLNAME,
                "bwHist_1", "bwHist_2", "bwHist_3", "bwHist_4",
                "bwHist_5", "bwHist_6", "bwHist_7", "bwHist_8",
                "bwHist_9", "bwHist_10", "bwHist_11", "bwHist_12",
                "bwHist_13", "bwHist_14", "bwHist_15", "bwHist_16",
                "bwHist_17", "bwHist_18", "bwHist_19", "bwHist_20",
                "bwHist_21", "bwHist_22", "bwHist_23", "bwHist_24",
                "bwHist_25", "bwHist_26", "bwHist_27", "bwHist_28",
                "bwHist_29", "bwHist_30", "bwHist_31", "bwHist_32",
                "bwHist_33", "bwHist_34", "bwHist_35", "bwHist_36",
                "bwHist_37", "bwHist_38", "bwHist_39", "bwHist_40",
                "bwHist_41", "bwHist_42", "bwHist_43", "bwHist_44",
                "bwHist_45", "bwHist_46", "bwHist_47", "bwHist_48",
                "bwHist_49", "bwHist_50", "bwHist_51", "bwHist_52",
                "bwHist_53", "bwHist_54", "bwHist_55", "bwHist_56",
                "bwHist_57", "bwHist_58", "bwHist_59", "bwHist_60",
                "bwHist_61", "bwHist_62", "bwHist_63", "bwHist_64",
                ATTR_VALUECOUNT,
                "isResult",
                ATTR_PHYSIZE,
                ATTR_SORT,
                ATTR_UNI,
                ATTR_FORM
            ],
            delimiter="\t"
    )
    
    # Skip the useless lines at the beginning.
    for i in range(4):
        next(reader)

    # For the dummy data.
    formats = ["uncompr", "static_vbp", "dynamic_vbp", "rle", "other"]
    
    res = {}
    for row in reader:
        opName = row[ATTR_OPNAME]
        colName = row[ATTR_COLNAME]
        if opName == "[RES]":
            break
        if opName == "morph":
            # If this line represents the output column of a morph-operator on
            # a base column.
            if colName[0] != "X" and colName[0] != "C" and "__" in colName:
                colName = colName[:colName.find("__")].replace("_", ".", 1)
            else:
                continue
        res[colName] = {
            ATTR_VALUECOUNT: int(row[ATTR_VALUECOUNT]),
            # Dummy data
#            ATTR_SIZEUSEDBYTE: random.randint(10**2, 10**8),
#            ATTR_FORMAT : formats[random.randrange(len(formats))],
#           ATTR_SORTED : [False, True][random.randrange(2)],
#           ATTR_UNIQUE : [False, True][random.randrange(2)],
#            # Real data
            ATTR_SIZEUSEDBYTE: int(row[ATTR_PHYSIZE]),
#            ATTR_PHYSIZE: int(row[ATTR_PHYSIZE]),
            ATTR_FORMAT : "uncompr" if row[ATTR_FORM] == None else formats[int(row[ATTR_FORM])],
            ATTR_SORTED : True if row[ATTR_SORT]==1 else False,
            ATTR_UNIQUE : True if row[ATTR_UNI]==1 else False,
        }
        for bw in range(1, 64+1):
            key = ATTR_BWHIST_FS.format(bw)
            # Dummy data
#            res[row[ATTR_COLNAME]][key] = int(res[row[ATTR_COLNAME]][ATTR_VALUECOUNT] / 64)
#            # Real data
            #res[row[ATTR_COLNAME]][key] = int(row[ATTR_COLNAME][key])
            res[colName][key] = int(row[key])
        
    return res


# *****************************************************************************
# Main program
# *****************************************************************************

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
        "mode", metavar="MODE", choices=MODES,
        help="The kind of CSV file to convert, the following are supported: "
            "{}.".format(", ".join(MODES))
    )
    # Optional arguments
    FROM_STDIN = "-"
    parser.add_argument(
        "--csvfile", dest="inCsvFilePath", default=FROM_STDIN, metavar="FILE",
        help="The path to the input CSV file. If this argument is omitted or "
            "if '{}' is specified, then the data will be read from "
            "stdin.".format(FROM_STDIN)
        # TODO Validate existence.
    )

    args = parser.parse_args()

    if args.inCsvFilePath == FROM_STDIN:
        # 0 is the file descriptor of stdin, can be used with open().
        args.inCsvFilePath = 0
        
    # -------------------------------------------------------------------------
    # Translating CSV to JSON
    # -------------------------------------------------------------------------

    with open(args.inCsvFilePath, "r", newline="") as inCsvFile:
        if args.mode == MODE_OPS:
            res = convertOperatorFile(inCsvFile)
        elif args.mode == MODE_DATA:
            res = convertDataChFile(inCsvFile)
        else:
            raise RuntimeError("unsupported mode: '{}'".format(args.mode))

    print(json.dumps(res, indent=2, sort_keys=True))