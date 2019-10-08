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
    echo "Usage: monetdb_ssb.sh [-h] [-sf N] [-r N]"
    echo ""
    echo "Time measurements of the Star Schema Benchmark (SSB) in MonetDB."
    echo ""
    echo "Assumptions:"
    echo "  - the SSB database has already been set up in MonetDB using ssb.sh"
    echo "  - MonetDB and out tools are reachable at the same relative paths "
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
    echo "  -r N, --repetitions N   The number of times to execute each "
    echo "                          query. Defaults to 1."
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
repetitions=1

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
        -r|--repetitions)
            repetitions=$2
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
pathMonetDB=../../monetdb
monetdbd=$pathMonetDB/bin/monetdbd
mclient=$pathMonetDB/bin/mclient
pathMonetDBFarm=../../monetdbfarm

# Related to MorphStore.
pathMorphStore=../..
pathTools=$pathMorphStore/Benchmarks/tools
qdict=$pathTools/dict/qdict.py

# Directories used for the data.
# Keep the names of the sub-directories consistent with dbdict.py.
pathData=data_sf$scaleFactor
pathDataDicts=$pathData/dicts

# The Name of the Benchmark.
benchmark=ssb

# The name of the database in MonetDB.
dbName=${benchmark}_sf$scaleFactor

printf "Starting MonetDB daemon... " >&2
eval $monetdbd start $pathMonetDBFarm
printf "done.\n" >&2

printf "major\tminor\trepetition\truntime [ms]\n"

for major in 1 2 3 4
do
    for minor in 1 2 3
    do
        for rep in $(seq $repetitions)
        do
            printf $major"\t"$minor"\t"$rep"\t"

            runtime=$( \
                printf "SET SCHEMA $benchmark;\n" \
                | cat - $pathQueries/q$major.$minor.sql \
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
done

printf "Stopping MonetDB daemon... " >&2
eval $monetdbd stop $pathMonetDBFarm
printf "done.\n" >&2