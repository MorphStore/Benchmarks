#!/bin/bash

psShortScalar=scalar
psShortSSE=sse
psShortAVX2=avx2
psShortAVX512=avx512

sf=1
comprs="alluncompr allstaticvbp alldynamicvbp"
pss="$psShortScalar $psShortSSE $psShortAVX2 $psShortAVX512"

declare -A psMap=(
    [$psShortScalar]="scalar<v64<uint64_t>>"
    [$psShortSSE]="sse<v128<uint64_t>>"
    [$psShortAVX2]="avx2<v256<uint64_t>>"
    [$psShortAVX512]="avx512<v512<uint64_t>>"
)

mkdir --parents demo

set -e

for compr in $comprs
do
    for ps in $pss
    do
        key="$ps"_"$compr"_0

        ./ssb.sh -s t -um s -c $compr -ps ${psMap[$ps]} -p d
        mv dc_sf"$sf" demo/dc_sf"$sf"_$key
        ./ssb.sh -s t -um s -c $compr -ps ${psMap[$ps]} -p t
        mv time_sf"$sf" demo/time_sf"$sf"_$key
    done
done