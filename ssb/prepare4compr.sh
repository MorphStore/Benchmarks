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

# TODO This script uses several constants and expressions which are also used
# in ssb.sh and must be kept consistent. We should store these things in a
# central place.

#******************************************************************************
# Help/Usage
#******************************************************************************

function print_help () {
    echo "Usage: prepare4compr.sh [-h] [-sf N] [-maxps PS] [-r N]"
    echo ""
    echo "Prepararion for using compression in the Star Schema Benchmark "
    echo "(SSB) in MorphStore."
    echo ""
    echo "Produces all artifacts required for employing compression according "
    echo "to our cost-based decision strategies."
    echo ""
    echo "In particular:"
    echo "- Recording of the data characteristics of all required base "
    echo "  columns and intermediate results"
    echo "- Recording of the compressed sizes of all these columns in all "
    echo "  formats (for determining the globally miminal memory footprint "
    echo "  for evaluation purposes)"
    echo "- The calibration phase of our cost models (creation of various "
    echo "  profiles)"
    echo ""
    echo "Optional arguments:"
    echo "  -h, --help              Show this help message and exit."
    echo "  -sf N, --scaleFactor N  The SSB scale factor to use. Default is 1."
    echo "  -maxps PROCESSING_STYLE The processing style up to which "
    echo "                          artifacts shall be made available. Choose "
    echo "                          from 'scalar', 'sse', 'avx2', and "
    echo "                          'avx512'. Defaults to 'scalar'."
    echo "  -r N                    The number of repetitions for the "
    echo "                          calibration measurements. Defaults to 1."
}


#******************************************************************************
# Some constants
#******************************************************************************

# -----------------------------------------------------------------------------
# Processing styles / vector extensions
# -----------------------------------------------------------------------------

psScalar="scalar"
psSSE="sse"
psAVX2="avx2"
psAVX512="avx512"

declare -A psLongMap=(
    [$psScalar]="scalar<v64<uint64_t>>"
    [$psSSE]="sse<v128<uint64_t>>"
    [$psAVX2]="avx2<v256<uint64_t>>"
    [$psAVX512]="avx512<v512<uint64_t>>"
)


#******************************************************************************
# Argument parsing
#******************************************************************************

# Defaults.
scaleFactor=1
maxPs=$psScalar
countReps=1

# Parsing.
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
        -maxps)
            maxPs=$2
            shift
            ;;
        -r)
            countReps=$2
            shift
            ;;
        *)
            printf "unknown option: $key\n"
            exit -1
            ;;
    esac
    shift
done

# Post-processing.
if [[ $maxPs = $psScalar ]]
then
    pss="$psScalar"
    psFlag=""
elif [[ $maxPs = $psSSE ]]
then
    pss="$psScalar $psSSE"
    psFlag="-sse4"
elif [[ $maxPs = $psAVX2 ]]
then
    pss="$psScalar $psSSE $psAVX2"
    psFlag="-avxtwo"
elif [[ $maxPs = $psAVX512 ]]
then
    pss="$psScalar $psSSE $psAVX2 $psAVX512"
    psFlag="-avx512"
else
    printf "unknown processing style: $maxPs\n"
    exit -1
fi


#******************************************************************************
# Execution
#******************************************************************************

generalFlags="-mem noSelfManaging -um s -sf $scaleFactor -s t"

set -e

# TODO Data characteristics and data sizes could run in parallel for different
# queries on different cores, since they do not measure runtimes.

# -----------------------------------------------------------------------------
# Data characteristics
# -----------------------------------------------------------------------------

./ssb.sh $generalFlags -p d

# -----------------------------------------------------------------------------
# (Un)compressed data sizes
# -----------------------------------------------------------------------------

for ps in $pss
do
    ./ssb.sh $generalFlags -p s -ps ${psLongMap[$ps]}
    mv size_sf$scaleFactor size_sf${scaleFactor}_${psLongMap[$ps]}
done

# -----------------------------------------------------------------------------
# Cost model calibration
# -----------------------------------------------------------------------------

cd ../../Engine
./build.sh -noSelfManaging -hi $psFlag -mon -bCa
mkdir --parents ../Benchmarks/ssb/compr_profiles
build/src/calibration/bw_prof $countReps > ../Benchmarks/ssb/compr_profiles/bw_prof_alone.csv
build/src/calibration/bw_prof_casc $countReps > ../Benchmarks/ssb/compr_profiles/bw_prof_casc.csv
build/src/calibration/const_prof $countReps > ../Benchmarks/ssb/compr_profiles/const_prof_casc.csv

set +e