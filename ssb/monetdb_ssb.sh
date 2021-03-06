#!/bin/bash

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

function print_help () {
    echo "Usage: monetdb_ssb.sh [-h] [-sf N] [-q {N.N}] [-r N] [-t INT_TYPE]"
    echo "                      [--pathMonetDB] [--pathMonetDBFarm]"
    echo "                      [--pathMorphStore] [--pathData]"
    echo ""
    echo "Time measurements of the Star Schema Benchmark (SSB) in MonetDB."
    echo ""
    echo "Assumptions:"
    echo "  - the SSB database has already been set up in MonetDB using ssb.sh"
    echo "  - MonetDB and our tools are reachable at the same relative paths "
    echo "    where ssb.sh expects them (see 'ssb.sh -h')."
    echo ""
    echo "In general, this script is rather an extension to ssb.sh, not a "
    echo "self-contained one on its own. Always use ssb.sh before."
    echo ""
    echo "The output are some status messages on stderr and a CSV table on "
    echo "stdout. The table is tab-separated and contains:"
    echo "  - the query number (major and minor)"
    echo "  - the number of the repetition of this query"
    echo "  - the query execution time in MonetDB in milliseconds (measured "
    echo "    using the 'run' entry output by the '-t performace' flag of "
    echo "    mclient)"
    echo ""
    echo "Optional arguments:"
    echo "  -h, --help              Show this help message and exit."
    echo "  -sf N, --scaleFactor N  The SSB scale factor. Defaults to 1."
    echo "  -q {N.N}, --query {N.N}, --queries {N.N}"
    echo "                          The numbers of the queries to execute. "
    echo "                          Multiple queries can be specified by "
    echo "                          passing a space-separated list enclosed "
    echo "                          in quotation marks. Defaults to all queries."
    echo "  -r N, --repetitions N   The number of times to execute each "
    echo "                          query. Defaults to 1."
    echo "  -t INT_TYPE, --intType INT_TYPE"
    echo "                          The integer type to use. Note that the"
    echo "                          SSB database must have been created using"
    echo "                          this integer type with ssb.sh. Defaults to"
    echo "                          $intType."
    echo ""
    echo "Examples:"
    echo "  monetdb_ssb.sh"
    echo "    Executes all SSB queries once at scale factor 1."
    echo "  monetdb_ssb.sh -sf 10"
    echo "    Executes all SSB queries once at scale factor 10."
    echo "  monetdb_ssb.sh -r 5"
    echo "    Executes all SSB queries five times at scale factor 1."
    echo "  monetdb_ssb.sh -sf 10 -r 5"
    echo "    Executes all SSB queries five times at scale factor 10."
}

# Defaults.
scaleFactor=1
queries="1.1 1.2 1.3 2.1 2.2 2.3 3.1 3.2 3.3 3.4 4.1 4.2 4.3"
repetitions=1
intType="BIGINT"
pathMonetDB=""
pathMonetDBFarm=""
pathMorphStore=""
pathData=""

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -h|--help)
            print_help
            exit 0
            ;;
        -sf|--scaleFactor)
            scaleFactor=$2
            shift
            ;;
        -q|--query|--queries)
            queries=$2
            shift
            ;;
        -r|--repetitions)
            repetitions=$2
            shift
            ;;
        -t|--intType)
            intType=$2
            shift
            ;;
        --pathMonetDB)
            pathMonetDB=$2
            shift
            ;;
        --pathMonetDBFarm)
            pathMonetDBFarm=$2
            shift
            ;;
        --pathMorphStore)
            pathMorphStore=$2
            shift
            ;;
        --pathData)
            pathData=$2
            shift
            ;;
        *)
            printf "unknown option: $key\n"
            exit -1
            ;;
    esac
    shift
done

# TODO The following lines were copied from ssb.sh. Put them in a central place.

# Related to this script.
pathQueries=queries

# Related to MonetDB.
if [[ ! $pathMonetDB ]]
then
    pathMonetDB=../../monetdb
fi
monetdbd=$pathMonetDB/bin/monetdbd
monetdb=$pathMonetDB/bin/monetdb
mclient=$pathMonetDB/bin/mclient
if [[ ! $pathMonetDBFarm ]]
then
    pathMonetDBFarm=../../monetdbfarm
fi

# Related to MorphStore.
if [[ ! $pathMorphStore ]]
then
    pathMorphStore=../..
fi
pathTools=$pathMorphStore/Benchmarks/tools
qdict=$pathTools/dict/qdict.py

# Directories used for the data.
# Keep the names of the sub-directories consistent with dbdict.py.
if [[ ! $pathData ]]
then
    pathData=data_sf$scaleFactor
fi
pathDataDicts=$pathData/dicts

# The Name of the Benchmark.
benchmark=ssb

# The name of the database in MonetDB.
dbName=${benchmark}_sf${scaleFactor}_${intType}

printf "Starting MonetDB daemon... " >&2
eval $monetdbd start $pathMonetDBFarm
printf "done.\n" >&2

# Set MonetDB to single-threaded execution, just like MorphStore.
eval $monetdb set nthreads=1 $dbName
# Set MonetDB to read-only execution, just like MorphStore.
eval $monetdb set readonly=yes $dbName
# Tell MonetDB to use its default optimization pipeline. We need to do this
# because for the query translation in ssb.sh, we explicitly use the sequential
# pipeline.
eval $monetdb set optpipe=default_pipe $dbName

printf "query\trepetition\truntime [ms]\n"

for query in $queries
do
    for rep in $(seq $repetitions)
    do
        printf "${query}\t${rep}\t"

        runtime=$( \
            printf "SET SCHEMA $benchmark;\n" \
            | cat - $pathQueries/q$query.sql \
            | $qdict $pathDataDicts \
            | $mclient -d $dbName -f raw -t performance \
            2>&1 > /dev/null \
            | tail -n 2 \
            | head -n 1 \
            | grep -P "(?<=run:)\d+\.\d+(?= ms)" -o \
        )

        printf $runtime"\n"
    done
done

printf "Stopping MonetDB daemon... " >&2
eval $monetdbd stop $pathMonetDBFarm
printf "done.\n" >&2