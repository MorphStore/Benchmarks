#!/bin/bash

for extension in $1
do
    for comprBase in $2
    do
        for variant in $3
        do
            key=$extension"_"$comprBase"_"$variant
            mkdir --parents $key
            for major in 1 2 3 4
            do
                for minor in 1 2 3
                do
                    ./csv2json.py --csvfile ../../ssb/demo/time_sf1_$key/q$major.$minor.csv ops \
                        > $key/ops_q$major.$minor.json
                    ./csv2json.py --csvfile ../../ssb/demo/dc_sf1_$key/q$major.$minor.csv data \
                        > $key/data_q$major.$minor.json
                done
            done
        done
    done
done
