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
Generates the SQL statements for setting up a schema in MonetDB.

This includes creating a new schema, creating tables within this schema and
loading data from CSV files into these tables. The generated statements are
printed to stdout.

Important: The data is assumed to contain only integer columns. Such data can
be obtained with MorphStore's dbdict.py. The integer type of each column will
be defined as specified by argument intType (either the same type for all
columns or the narrowest possible type for each individual column, see below).

The schema information must be provided as a JSON file fulfilling the following
criteria:
- It contains a single object.
- For each table in the schema, this object must have an attribute named after
  the table.
- The value of this attribute must be a list of the names of the columns of
  this table. The order of this list must match the order encountered in the
  CSV file that shall be loaded into the table.
For instance, the contents of the schema file could look like this:

{
    "table1": ["col11", "col12"],
    "table2": ["col21", "col22", "col23"]
}

The CSV files to load the data from must fulfil the following criteria:
- The file extension is ".tbl".
- Columns are separated by the pipe character "|".
- Lines are separated by the newline character "\\n".
These requirements are fulfilled for the files output by MorphStore's
dbdict.py tool.
"""


import argparse
import json
import os


INTTYPE_TINY = "TINYINT"
INTTYPE_SMALL = "SMALLINT"
INTTYPE_INT = "INT"
INTTYPE_BIG = "BIGINT"
INTTYPE_TIGHT = "tight"
INTTYPES= [INTTYPE_TINY, INTTYPE_SMALL, INTTYPE_INT, INTTYPE_BIG, INTTYPE_TIGHT]

def bitwidth(n):
    return 1 if n == 0 else int(n).bit_length()

def getIntType(bw):
    # We use "<" not "<=", since all these SQL integer types are signed.
    if bw < 8:
        return INTTYPE_TINY
    if bw < 16:
        return INTTYPE_SMALL
    if bw < 32:
        return INTTYPE_INT
    return INTTYPE_BIG


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Argument parsing
    # -------------------------------------------------------------------------
    
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Required positional arguments.
    parser.add_argument(
        "schemaName",
        help="The name of the database schema to create."
    )
    parser.add_argument(
        "schemaFilePath", metavar="schemaFile",
        help="The path to the JSON file containing the required schema "
            "information"
        # TODO validate existence
    )
    parser.add_argument(
        "dataDirPath", metavar="dataDir",
        help="The path to the directory containing the CSV files with the "
            "data to be loaded into the newly created database. This "
            "directory is not required to exist when executing this program, "
            "since it will only be inserted into the generated SQL statements."
    )
    parser.add_argument(
        "intType", metavar="intType", choices=INTTYPES,
        help="The integer type to use for all columns. Choose one of {}. "
            "If you specify '{}', then each column is assigned the narrowest "
            "type guaranteeing a lossless representation.".format(
                INTTYPES, INTTYPE_TIGHT
            )
    )
    parser.add_argument(
        "statsDirPath", metavar="statsDir",
        help="The path to the directory containing the statistics on the base "
            "data. Required of you specified '{}' as the integer type.".format(
                INTTYPE_TIGHT
            )
    )
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # Actual work
    # -------------------------------------------------------------------------
    
    with open(args.schemaFilePath, "r") as schemaFile:
        schema = json.load(schemaFile)
        
        print("CREATE SCHEMA {};".format(args.schemaName))
        print("SET SCHEMA {};".format(args.schemaName))
        print()
        
        for tblName, colNames in schema.items():
            if args.intType == INTTYPE_TIGHT:
                statsFilePath = os.path.join(
                    args.statsDirPath,
                    "{}.json".format(tblName)
                )
                with open(statsFilePath) as statsFile:
                    stats = json.load(statsFile)
            
            countCols = len(colNames)
            print("CREATE TABLE {} (".format(tblName))
            for colIdx, colName in enumerate(colNames):
                colIntType = getIntType(bitwidth(stats[colName])) \
                    if args.intType == INTTYPE_TIGHT \
                    else args.intType
                print("\t{} {}{}".format(
                    colName, colIntType, "," if colIdx < countCols - 1 else ""
                ))
            print(");")
            # MonetDB expects an absolute path here.
            dataFilePath = os.path.abspath(os.path.join(
                args.dataDirPath, "{}.tbl".format(tblName)
            ))
            print("COPY INTO {} FROM '{}' USING DELIMITERS '|','\\n';".format(
                tblName, dataFilePath
            ));
            print()