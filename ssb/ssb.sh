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


#******************************************************************************
# Help/Usage
#******************************************************************************

function print_help () {
    echo "Usage: ssb.sh [-h] [-s STEP] [-e STEP] [-sf N] [-p PURPOSE]"
    echo "              [-ps PROCESSING_STYLE] [-v vectorVersion]"
    echo "              [-c COMPRESSION_STRATEGY] [-crnd FORMAT]"
    echo "              [-csequ FORMAT] [-cseqs FORMAT] [-ccbsl N]"
    echo "              [-um WAY_TO_USE_MONETDB] [-noSelfManaging]"
    echo ""
    echo "Star Schema Benchmark (SSB) in MorphStore."
    echo ""
    echo "This script provides the means for running SSB in MorphStore. By "
    echo "default, it executes the following sequence of steps. The arguments "
    echo "-s and -e can be used to start and stop at selected steps."
    echo ""
    echo "Steps:"
    echo "  c, clean"
    echo "      Deletes everything previously generated by this script. This "
    echo "      includes all data files for the specified scale factor, the "
    echo "      database in MonetDB, and the generated C++ source files, if "
    echo "      they already exist."
    echo "  g, generate"
    echo "      Creates the SSB data. This includes the data generation using "
    echo "      SSB's dbgen tool, the offline dictionary coding of that data "
    echo "      using MorphStore's dbdict.py tool, the creation of a database "
    echo "      in MonetDB, and loading the dictionary coded data into "
    echo "      MonetDB. A directory named 'data_sfN' is created in the "
    echo "      current directory, whereby N is the specified scale factor."
    echo "  t, translate"
    echo "      Creates C++ source files for all SSB queries in MorphStore. "
    echo "      This includes running the queries with EXPLAIN and with "
    echo "      string literals replaced by MorphStore's qdict.py tool using "
    echo "      MonetDB, translating the so-obtained MAL programs to "
    echo "      MorphStore C++ using MorphStore's mal2x.py tool, and "
    echo "      creating a CMakeLists.txt file for the generated C++ programs."
    echo "  v, visualize"
    echo "      Creates Dot files for all SSB queries in MorphStore. "
    echo "  b, build"
    echo "      Builds MorphStore using MorphStore's build.sh script."
    echo "  r, run"
    echo "      Runs all SSB queries. What exactly is done in this step "
    echo "      depends on the selected purpose (see below)."
    echo ""
    echo "This script can be run for different purposes. By default, the "
    echo "purpose is to check the correctness of the query results. The "
    echo "argument -p can be used to select a purpose. Note that the steps "
    echo "clean and generate are not affected by the purpose. The following "
    echo "purposes are supported:"
    echo ""
    echo "Purposes:"
    echo "  c, check"
    echo "      Verify that MorphStore's query results are correct by "
    echo "      comparing them to those of MonetDB. No further files are "
    echo "      created."
    echo "  r, results"
    echo "      Like check, but also writes the query results of both MonetDB "
    echo "      and MorphStore to files. A directory 'res_sfN' is created in "
    echo "      the current directory, whereby N is the specified scale "
    echo "      factor. This directory contains the query results as CSV "
    echo "      files. Note that this directory is NOT deleted in the "
    echo "      cleaning step."
    echo "  t, time"
    echo "      Measure the runtimes achieved by MorphStore. A directory "
    echo "      'time_sfN' is created in the current directory, whereby N is "
    echo "      the specified scale factor. In this directory, one file per "
    echo "      query will be generated, containing the measurements for the "
    echo "      respective query. Note that this directory is NOT deleted in "
    echo "      the cleaning step."
    echo "  d, datacharacteristics"
    echo "      Analyze the data characteristics of all base and intermediate "
    echo "      columns involved in the query in MorphStore. A directory "
    echo "      'dc_sfN' is created in the current directory, whereby N is "
    echo "      the specified scale factor. In this directory, one file per "
    echo "      query will be generated, containing the data characteristics "
    echo "      for the respective query. Note that this directory is NOT "
    echo "      deleted in the cleaning step."
    echo "  s, size"
    echo "      Morph each base and intermediate column to each format and "
    echo "      record the sizes. A directory 'size_sfN' is created in the "
    echo "      current directory, whereby N is the specified scale factor. "
    echo "      In this directory, one file per query will be generated, "
    echo "      containing the physical sizes for the respective query. Note "
    echo "      that this directory is NOT deleted in the cleaning step. "
    echo "      This purpose requires '-c uncompr' and '-noSelfManaging'."
    echo ""
    echo "Vector Versions:"
    echo "  h, byhand"
    echo "      Hand implemented scalar and vectorized operators"
    echo "  l, lib"
    echo "      Uses the vector lib to build all operators. "
    echo "      This requires a processing style (-ps) in the form "
    echo "      vectorExtension < vectorSize < basetype > >. The brackets have " 
    echo "      to be escaped."
    echo "      Examples: avx2\<v256\<uint64_t\>\>"
    echo "                scalar\<v64\<uint64_t\>\>"
    echo ""
    echo "The formats of all base columns and intermediate results are "
    echo "determined by a compression strategy. The following strategies are "
    echo "available:"
    echo ""
    echo "Compression Strategies:"
    echo "  uncompr"
    echo "      All columns are uncompressed (uncompr_f in MorphStore)."
    echo "  rulebased"
    echo "      Applies a simple rule-based strategy. This allows the "
    echo "      specification of the arguments -crnd, -csequ, -cseqs, each of "
    echo "      which must be followed by a format name. See the help of "
    echo "      mal2morphstore for more details."
    echo ""
    echo "This script depends on MonetDB, since the 'translate'-step requires "
    echo "MAL programs from MonetDB and the 'run'-step (with the 'check'- or "
    echo "'results'-purpose) requires reference query results from MonetDB. "
    echo "There are different ways how these artifacts can be used."
    echo ""
    echo "Ways to use MonetDB"
    echo "  p, pipeline"
    echo "      Run MonetDB on this system. Directly pipe the outputs of "
    echo "      MonetDB into the scripts and tools that need them, but do not "
    echo "      store these outputs to files."
    echo "  m, materialize"
    echo "      Run MonetDB on this system. Store MonetDB's outputs to files "
    echo "      so that they can be used to run this script without MonetDB "
    echo "      at some later point in time or on another system where "
    echo "      MonetDB is not installed."
    echo "      The 'translate'-step will create a directory 'mal_sfN' in the "
    echo "      current directory (N is the specified scale factor), which "
    echo "      contains the MAL programs output by MonetDB."
    echo "      The 'run'-step will create a directory 'refres_sfN' in the "
    echo "      current directory (N is the specified scale factor), which )"
    echo "      contains the reference query results obtained from MonetDB."
    echo "  s, saved"
    echo "      Do not run MonetDB on this system. Instead obtain the "
    echo "      required outputs of MonetDB from files that were previously "
    echo "      created using the 'materialize'-option above."
    echo ""
    echo "Optional arguments:"
    echo "  -h, --help              Show this help message and exit."
    echo "  -s STEP, --start STEP   The step to start with. Defaults to clean "
    echo "                          (the first step)."
    echo "  -e STEP, --end STEP     The step to stop after. Defaults to run "
    echo "                          (the final step)."
    echo "  -sf N, --scaleFactor N  The scale factor to use for the SSB data "
    echo "                          generation. Defaults to 1."
    echo "  -p PURPOSE, --purpose PURPOSE"
    echo "                          The purpose of the query execution. "
    echo "                          Defaults to check."
    echo "  -ps PROCESSING_STYLE, --processingStyle PROCESSING_STYLE"
    echo "                          The processing style to use in MorphStore."
    echo "                          Supported values are scalar, vec128, and "
    echo "                          vec256. Defaults to scalar."
    echo "  -c COMPRESSION_CONFIG, --comprConfig COMPRESSION_CONFIG"
    echo "                          The compression configuration to use for "
    echo "                          the translated queries. Defaults to "
    echo "                          alluncompr."
    echo "  -um WAY_TO_USE_MONETDB, --useMonetDB WAY_TO_USE_MONETDB"
    echo "                          The way to use MonetDB."
    echo "  -noSelfManaging         The query executables will use standard "
    echo "                          C++ memory management instead of "
    echo "                          MorphStore's own memory manager."
    echo ""
    echo "Examples:"
    echo "  ssb.sh"
    echo "      Executes all steps for scale factor 1, verifying the "
    echo "      correctness of the query results."
    echo "  ssb.sh -e g"
    echo "      Cleans already existing data and re-generates the data for "
    echo "      scale factor 1."
    echo "  ssb.sh -s t"
    echo "      Translates MAL to MorphStore C++, builds MorphStore, and "
    echo "      runs all queries for scale factor 1, verifying the "
    echo "      correctness of the query results."
    echo "  ssb.sh -s t -e t -p m"
    echo "      Only translates MAL to MorphStore, including the code "
    echo "      required for measurements."
    echo "  ssb.sh -e c"
    echo "      Cleans already existing data."
    echo "  ssb.sh -sf 10"
    echo "      Executes all steps for scale factor 10, verifying the "
    echo "      correctness of the query results."
    echo "  ssb.sh -p m"
    echo "      Executes all steps for scale factor 1, measuring the runtimes "
    echo "      of the queries in MorphStore."
    echo ""
    echo "Requirements:"
    echo "- This script assumes that the following directories (or links to "
    echo "  directories) are present in '../..', unless you specify '-um s':"
    echo "  - 'ssb-dbgen'   The directory containing the sources of the SSB's"
    echo "                  dbgen tool."
    echo "  - 'monetdb'     The directory MonetDB was installed to."
    echo "  - 'monetdbfarm' The directory of a MonetDB farm."
    echo "- Furthermore, it is assumed that '../..' is the MorphStore root "
    echo "  directory containing the directories 'Engine' and 'Benchmarks'."
}


#******************************************************************************
# Utility functions
#******************************************************************************

function print_headline1 () {
    printf "\n"
    printf "################################################################\n"
    printf "# $1\n"
    printf "################################################################\n"
    printf "\n"
}

function print_headline2 () {
    printf "\n"
    printf "================================================================\n"
    printf "= $1\n"
    printf "================================================================\n"
    printf "\n"
}


#******************************************************************************
# Functions for the individual steps
#******************************************************************************

function clean () {
    print_headline1 "Cleaning"

    # Delete the generated data.
    rm -rf $pathData

    # Delete the database in MonetDB.
    if [[ $useMonetDB != $umSaved ]]
    then
        eval $monetdb destroy -f $dbName
    fi

    # Delete the generated MorphStore C++ files.
    rm -rf $pathSrc

    print_headline1 "Done"
}

function generate () {
    print_headline1 "Creating SSB data for MorphStore and MonetDB"

    set -e

    if [[ $useMonetDB = $umSaved ]]
    then
        printf "the generate step requires MonetDB, use '-um p' or '-um m'\n"
        exit -1
    fi

    print_headline2 "Generating SSB data"
    local oldPwd=$(pwd)
    cd $pathDBGen
    make
    ./dbgen -f -s $scaleFactor -T a
    cd $oldPwd

    print_headline2 "Dictionary coding"
    mkdir $pathData
    eval $dbdict $schemaFile $pathDBGen $pathData

    print_headline2 "Loading data into MonetDB"
    eval $monetdb create $dbName
    # Deactivating multi-threading is important, since mal2x.py cannot
    # translate multi-threaded MAL plans.
    eval $monetdb set nthreads=1 $dbName
    eval $monetdb release $dbName
    eval $createload $benchmark $schemaFile $pathDataTblsDict \
        | $mclient -d $dbName

    print_headline2 "Deleting .tbl-files"
    rm -f $pathDBGen/*.tbl
    rm -rf $pathDataTblsDict

    set +e

    print_headline1 "Done"
}

function translate () {
    print_headline1 "Translating queries"

    set -e

    case $useMonetDB in
        $umMaterialize)
            mkdir --parents $pathMal
            ;;
        $umSaved)
            if [[ ! ( -d $pathMal ) ]]
            then
                printf "you specified to use saved MAL programs, but the directory '$pathMal' does not exist\n"
                exit -1
            fi
            ;;
    esac

    mkdir --parents $pathSrc
    local cmakeListsFile=$pathSrc/CMakeLists.txt

    rm -f $cmakeListsFile

    local comprFlags="-c $comprStrategy"
    if [[ $comprRnd ]]
    then
        comprFlags="$comprFlags -crnd $comprRnd"
    fi
    if [[ $comprSeqUnsorted ]]
    then
        comprFlags="$comprFlags -csequ $comprSeqUnsorted"
    fi
    if [[ $comprSeqSorted ]]
    then
        comprFlags="$comprFlags -cseqs $comprSeqSorted"
    fi
    if [[ $comprCascBlockSizeLog ]]
    then
        comprFlags="$comprFlags -ccbsl $comprCascBlockSizeLog"
    fi

    local statFlag="--statdir $pathDataStatsDict"

    printf "if( BUILD_ALL OR BUILD_SSB )\n" >> $cmakeListsFile
    for major in 1 2 3 4
    do
        for minor in 1 2 3
        do
            printf "$benchmark q$major.$minor: "

            case $useMonetDB in
                $umPipeline)
                    printf "SET SCHEMA $benchmark;\nEXPLAIN " \
                        | cat - $pathQueries/q$major.$minor.sql \
                        | $qdict $pathDataDicts \
                        | $mclient -d $dbName -f raw \
                        | $mal2morphstore $processingStyle $purpose $versionSelect $comprFlags $statFlag \
                        > $pathSrc/q$major.$minor.cpp
                    ;;
                $umMaterialize)
                    printf "SET SCHEMA $benchmark;\nEXPLAIN " \
                        | cat - $pathQueries/q$major.$minor.sql \
                        | $qdict $pathDataDicts \
                        | $mclient -d $dbName -f raw \
                        > $pathMal/q$major.$minor.mal
                    cat $pathMal/q$major.$minor.mal \
                        | $mal2morphstore $processingStyle $purpose $versionSelect $comprFlags $statFlag \
                        > $pathSrc/q$major.$minor.cpp
                    ;;
                $umSaved)
                    cat $pathMal/q$major.$minor.mal \
                        | $mal2morphstore $processingStyle $purpose $versionSelect $comprFlags $statFlag \
                        > $pathSrc/q$major.$minor.cpp
                    ;;
                *)
                    printf "unknown way to use MonetDB (in translate step): $useMonetDB\n"
                    exit -1
                    ;;
            esac

            local targetName=q$major.$minor"_sf"$scaleFactor

            # TODO Maybe we should outsource this snippet to a file.
            printf "\tadd_executable( $targetName q$major.$minor.cpp )\n"      >> $cmakeListsFile
            printf "\ttarget_compile_options( $targetName PRIVATE\n"           >> $cmakeListsFile
            # TODO Remove -Wno-ignored-attributes as soon as we have it at a
            #      higher-level in the build script.
            printf "\t                        -Wno-unused-parameter\n"         >> $cmakeListsFile
            printf "\t                        $<$<CONFIG:DEBUG>:-DDEBUG> )\n"  >> $cmakeListsFile
            printf "\ttarget_link_libraries( $targetName PRIVATE \"-ldl\" )\n" >> $cmakeListsFile
            printf "\n"                                                        >> $cmakeListsFile

            printf "done.\n"
        done
    done
    printf "endif( BUILD_ALL OR BUILD_SSB )\n" >> $cmakeListsFile

    set +e

    print_headline1 "Done"
}

function translateToDot () {
    print_headline1 "Translating queries to DOT"

    set -e

    case $useMonetDB in
        $umMaterialize)
            mkdir --parents $pathMal
            ;;
        $umSaved)
            if [[ ! ( -d $pathMal ) ]]
            then
                printf "you specified to use saved MAL programs, but the directory '$pathMal' does not exist\n"
                exit -1
            fi
            ;;
    esac

    for major in 1 2 3 4
    do
        for minor in 1 2 3
        do
            printf "$benchmark q$major.$minor: "
            filename=$pathSrc/q$major.$minor
            case $useMonetDB in
                $umPipeline)
                    printf "SET SCHEMA $benchmark;\nEXPLAIN " \
                        | cat - $pathQueries/q$major.$minor.sql \
                        | $qdict $pathDataDicts \
                        | $mclient -d $dbName -f raw \
                        | $dotvisualize $major $minor\
                        > $pathSrc/q$major.$minor.dot
                    dot -Tsvg -o $filename.svg $filename.dot
                    sed -i '/^<title/ d' $filename.svg
                    ;;
                $umMaterialize)
                    printf "SET SCHEMA $benchmark;\nEXPLAIN " \
                        | cat - $pathQueries/q$major.$minor.sql \
                        | $qdict $pathDataDicts \
                        | $mclient -d $dbName -f raw \
                        > $pathMal/q$major.$minor.mal
                    cat $pathMal/q$major.$minor.mal \
                        | $dotvisualize $major $minor\
                        > $pathSrc/q$major.$minor.dot
                    dot -Tsvg -o $filename.svg $filename.dot
                    sed -i '/^<title/ d' $filename.svg
                    ;;
                $umSaved)
                    cat $pathMal/q$major.$minor.mal \
                        | $dotvisualize $major $minor \
                        > $pathSrc/q$major.$minor.dot
                    dot -Tsvg -o $filename.svg $filename.dot
                    sed -i '/^<title/ d' $filename.svg
                    ;;
                *)
                    printf "unknown way to use MonetDB (in translate step): $useMonetDB\n"
                    exit -1
                    ;;
            esac

            printf "done q$major.$minor .\n"
        done
    done

    set +e

    print_headline1 "Done"
}

function build () {
    print_headline1 "Compiling MorphStore"

    set -e

    if [[ $purpose = $purposeCheck || $purpose = $purposeResults ]]
    then
        local monitoringFlag=""
    elif [[ $purpose = $purposeTime || $purpose = $purposeDataCh || $purpose = $purposeSize ]]
    then
        local monitoringFlag="-mon"
    else
        printf "unsupported purpose (in step build): $purpose\n"
        exit -1
    fi

    local oldPwd=$(pwd)
    cd $pathMorphStore/Engine
    if [[ $processingStyle = $psSSE ]]
    then
        local extensionFlags="-sse4"
    elif [[ $processingStyle = $psAVX2 ]]
    then
        local extensionFlags="-sse4 -avxtwo"
    elif [[ $processingStyle = $psAVX512 ]]
    then
        local extensionFlags="-sse4 -avxtwo -avx512"
    else
        local extensionFlags=""
    fi
    if [[ $scaleFactor -eq 1 ]]
    then
        local vbpFlag="--vbpLimitRoutinesForSSBSF1"
    else
        local vbpFlag=""
    fi
    # TODO Do not hard-code the arguments for build.sh.
    ./build.sh -hi -j8 $monitoringFlag $extensionFlags -bSSB $noSelfManaging $vbpFlag
    cd $oldPwd

    set +e

    print_headline1 "Done"
}

function run () {
    print_headline1 "Running queries"

    if [[ $purpose = $purposeCheck || $purpose = $purposeResults ]]
    then
        print_headline2 "Comparing query results of MorphStore and MonetDB"
    elif [[ $purpose = $purposeTime ]]
    then
        print_headline2 "Measuring runtimes in MorphStore"
        mkdir --parents $pathTime
    elif [[ $purpose = $purposeDataCh ]]
    then
        print_headline2 "Analyzing the data characteristics in MorphStore"
        mkdir --parents $pathDataCh
    elif [[ $purpose = $purposeSize ]]
    then
        print_headline2 "Recording the data sizes in MorphStore"
        mkdir --parents $pathSize
    else
        printf "unsupported purpose (in step run): $purpose\n"
        exit -1
    fi

    if [[ $purpose = $purposeResults ]]
    then
        mkdir --parents $pathRes
    fi

    case $useMonetDB in
        $umMaterialize)
            mkdir --parents $pathRefRes
            ;;
        $umSaved)
            if [[ ! ( -d $pathRefRes ) ]]
            then
                printf "you specified to use saved reference query results, but the directory '$pathRefRes' does not exist\n"
                exit -1
            fi
    esac

    for major in 1 2 3 4
    do
        for minor in 1 2 3
        do
            printf "$benchmark q$major.$minor: "

            local targetName=q$major.$minor"_sf"$scaleFactor

            # TODO Reduce the code duplication between the check and results
            #      purposes.
            case $purpose in
                $purposeCheck)
                    # TODO Remove the sort in the pipe once MorphStore supports
                    #      sorting.
                    case $useMonetDB in
                        $umPipeline)
                            cmp --silent \
                                <( \
                                    printf "SET SCHEMA $benchmark;\n" \
                                        | cat - $pathQueries/q$major.$minor.sql \
                                        | $qdict $pathDataDicts \
                                        | $mclient -d $dbName -f csv \
                                        | sort \
                                ) \
                                <( \
                                    $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                        | sort \
                                )
                            ;;
                        $umMaterialize)
                            printf "SET SCHEMA $benchmark;\n" \
                                | cat - $pathQueries/q$major.$minor.sql \
                                | $qdict $pathDataDicts \
                                | $mclient -d $dbName -f csv \
                                | sort \
                                > $pathRefRes/q$major.$minor.csv
                            cmp --silent \
                                $pathRefRes/q$major.$minor.csv \
                                <( \
                                    $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                        | sort \
                                )
                            ;;
                        $umSaved)
                            cmp --silent \
                                $pathRefRes/q$major.$minor.csv \
                                <( \
                                    $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                        | sort \
                                )
                            ;;
                    esac
                    if [[ $? -eq 0 ]]
                    then
                        printf "good\n"
                    else
                        printf "BAD\n"
                    fi
                    ;;
                $purposeResults)
                    # TODO Remove the sort in the pipe once MorphStore supports
                    #      sorting.
                    case $useMonetDB in
                        $umPipeline)
                            local resFileMorphSt=$pathRes/q$major."$minor"_MorphStore.csv
                            local resFileMonetDB=$pathRes/q$major."$minor"_MonetDB.csv
                            printf "SET SCHEMA $benchmark;\n" \
                                | cat - $pathQueries/q$major.$minor.sql \
                                | $qdict $pathDataDicts \
                                | $mclient -d $dbName -f csv \
                                | sort \
                                > $resFileMonetDB
                            eval $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                | sort \
                                > $resFileMorphSt
                            cmp --silent $resFileMonetDB $resFileMorphSt
                            ;;
                        $umMaterialize)
                            local resFileMorphSt=$pathRes/q$major."$minor"_MorphStore.csv
                            local resFileMonetDB=$pathRes/q$major."$minor"_MonetDB.csv
                            printf "SET SCHEMA $benchmark;\n" \
                                | cat - $pathQueries/q$major.$minor.sql \
                                | $qdict $pathDataDicts \
                                | $mclient -d $dbName -f csv \
                                | sort \
                                > $resFileMonetDB
                            cp $resFileMonetDB $pathRefRes/q$major.$minor.csv
                            eval $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                | sort \
                                > $resFileMorphSt
                            cmp --silent $resFileMonetDB $resFileMorphSt
                            ;;
                        $umSaved)
                            local resFileMorphSt=$pathRes/q$major."$minor"_MorphStore.csv
                            eval $pathExe/$targetName $pathDataColsDict 2> /dev/null \
                                | sort \
                                > $resFileMorphSt
                            cmp --silent $pathRefRes/q$major.$minor.csv $resFileMorphSt
                            ;;
                    esac
                    if [[ $? -eq 0 ]]
                    then
                        printf "good\n"
                    else
                        printf "BAD\n"
                    fi
                    ;;
                $purposeTime)
                    printf "\n"
                    eval $pathExe/$targetName $pathDataColsDict > $pathTime/q$major.$minor.csv
                    printf "\n"
                    ;;
                $purposeDataCh)
                    printf "\n"
                    eval $pathExe/$targetName $pathDataColsDict > $pathDataCh/q$major.$minor.csv
                    printf "\n"
                    ;;
                $purposeSize)
                    printf "\n"
                    eval $pathExe/$targetName $pathDataColsDict > $pathSize/q$major.$minor.csv
                    printf "\n"
                    ;;
            esac
        done
    done

    print_headline1 "Done"
}


# *****************************************************************************
# Some configuration
# *****************************************************************************

# The Name of the Benchmark.
benchmark=ssb

# ----------------------------------------------------------------------------
# Paths to files and executables.
# ----------------------------------------------------------------------------

# Related to this script.
schemaFile=schema.json
pathQueries=queries

# Related to MonetDB.
pathMonetDB=../../monetdb
monetdbd=$pathMonetDB/bin/monetdbd
monetdb=$pathMonetDB/bin/monetdb
mclient=$pathMonetDB/bin/mclient
pathMonetDBFarm=../../monetdbfarm

# Related to the benchmark's data generation.
pathDBGen=../../ssb-dbgen

# Related to MorphStore.
pathMorphStore=../..
pathTools=$pathMorphStore/Benchmarks/tools
createload=$pathTools/monetdb_create+load.py
dbdict=$pathTools/dict/dbdict.py
qdict=$pathTools/dict/qdict.py
mal2morphstore=$pathTools/mal2x/mal2morphstore.py
dotvisualize=$pathTools/mal2x/mal2dot.py

# -----------------------------------------------------------------------------
# Steps of this script's execution.
# -----------------------------------------------------------------------------

stepClean=1
stepGenerate=2
stepTranslate=3
stepBuild=4
stepRun=5
stepVisualize=6

declare -A stepMap=(
    [c]=$stepClean
    [clean]=$stepClean
    [g]=$stepGenerate
    [generate]=$stepGenerate
    [t]=$stepTranslate
    [translate]=$stepTranslate
    [b]=$stepBuild
    [build]=$stepBuild
    [r]=$stepRun
    [run]=$stepRun
    [v]=$stepVisualize
    [visualize]=$stepVisualize
)

# -----------------------------------------------------------------------------
# Purposes of this script's execution.
# -----------------------------------------------------------------------------

purposeCheck="c"
purposeResults="r"
purposeTime="t"
purposeDataCh="d"
purposeSize="s"

declare -A purposeMap=(
    [c]=$purposeCheck
    [check]=$purposeCheck
    [r]=$purposeResults
    [results]=$purposeResults
    [t]=$purposeTime
    [time]=$purposeTime
    [d]=$purposeDataCh
    [datacharacteristics]=$purposeDataCh
    [s]=$purposeSize
    [size]=$purposeSize
)

# -----------------------------------------------------------------------------
# Vectorized version selection
# -----------------------------------------------------------------------------

handImplemented=1
usingLib=2

declare -A versionMap=(
    [h]=$handImplemented
    [byhand]=$handImpemented
    [l]=$usingLib
    [lib]=$usingLib
)

# -----------------------------------------------------------------------------
# Processing styles / vector extensions
# -----------------------------------------------------------------------------

psScalar="scalar<v64<uint64_t>>"
psSSE="sse<v128<uint64_t>>"
psAVX2="avx2<v256<uint64_t>>"
psAVX512="avx512<v512<uint64_t>>"

# -----------------------------------------------------------------------------
# The use of MonetDB.
# -----------------------------------------------------------------------------

umPipeline="p"
umMaterialize="m"
umSaved="s"

declare -A umMap=(
    [p]=$umPipeline
    [pipeline]=$umPipeline
    [m]=$umMaterialize
    [materialize]=$umMaterialize
    [s]=$umSaved
    [saved]=$umSaved
)

# *****************************************************************************
# Argument parsing
# *****************************************************************************

# Defaults.
startStep=$stepClean
endStep=$stepRun
scaleFactor=1
purpose=$purposeCheck
versionSelect=$usingLib
processingStyle=$psScalar
comprStrategy=uncompr
comprRnd=""
comprSeqUnsorted=""
comprSeqSorted=""
comprCascBlockSizeLog=""
useMonetDB=$umPipeline
noSelfManaging=""

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -h|--help)
            print_help
            exit 0
            ;;
        -s|--start)
            if [[ ${stepMap[$2]+_} ]]
            then
                startStep=${stepMap[$2]}
                shift
            else
                printf "unknown step: $2\n"
                exit -1
            fi
            ;;
        -e|--end)
            if [[ ${stepMap[$2]+_} ]]
            then
                endStep=${stepMap[$2]}
                shift
            else
                printf "unknown step: $2\n"
                exit -1
            fi
            ;;
        -sf|--scaleFactor)
            scaleFactor=$2
            shift
            ;;
        -p|--purpose)
            if [[ ${purposeMap[$2]+_} ]]
            then
                purpose=${purposeMap[$2]}
                shift
            else
                printf "unknown purpose: $2\n"
                exit -1
            fi
            ;;
        -v|--versionSelect)
            if [[ ${versionMap[$2]+_} ]]
            then
                versionSelect=${versionMap[$2]}
                shift
            else
                printf "unknown version: $2\n"
                exit -1
            fi
            ;;
        -ps|--processingStyle)
            processingStyle=$2
            shift
            ;;
        -c|--comprStrategy)
            comprStrategy=$2
            shift
            ;;
        -crnd)
            comprRnd=$2
            shift
            ;;
        -csequ)
            comprSeqUnsorted=$2
            shift
            ;;
        -cseqs)
            comprSeqSorted=$2
            shift
            ;;
        -ccbsl)
            comprCascBlockSizeLog=$2
            shift
            ;;
        -um|--useMonetDB)
            if [[ ${umMap[$2]+_} ]]
            then
                useMonetDB=${umMap[$2]}
                shift
            else
                printf "unknown way to use MonetDB: $2\n"
                exit -1
            fi
            ;;
        -noSelfManaging)
            noSelfManaging="-noSelfManaging"
            ;;
        *)
            printf "unknown option: $key\n"
            exit -1
            ;;
    esac
    shift
done

if [[ $startStep -gt $endStep ]]
then
    printf "the start step must not come after the end step\n"
    exit -1
fi

if [[ $purpose = $purposeSize ]] && [[ $noSelfManaging = "" ]]
then
    printf "you selected purpose '$purpose', which requires '-noSelfManaging'\n"
    exit -1
fi

# Directories used for the data.
# Keep the names of the sub-directories consistent with dbdict.py.
pathData=data_sf$scaleFactor
pathDataTblsDict=$pathData/tbls_dict
pathDataDicts=$pathData/dicts
pathDataColsDict=$pathData/cols_dict
pathDataStatsDict=$pathData/stats_dict

# Directories for the generated source and executable files.
pathSrc=$pathMorphStore/Engine/src/"$benchmark"_sf$scaleFactor
pathExe=$pathMorphStore/Engine/build/src/"$benchmark"_sf$scaleFactor

# Directory for the measured runtimes.
pathTime=time_sf$scaleFactor

# Directory for the data characteristics.
pathDataCh=dc_sf$scaleFactor

# Directory for the sizes.
pathSize=size_sf$scaleFactor

# Directory for the query results.
pathRes=res_sf$scaleFactor

# Directory for the MAL plans from MonetDB.
pathMal=mal_sf$scaleFactor

# Directory for the reference query results from MonetDB.
pathRefRes=refres_sf$scaleFactor

# The name of the database in MonetDB.
dbName="$benchmark"_sf$scaleFactor


# *****************************************************************************
# Checking the existence of some required directories
# *****************************************************************************

if [[ $useMonetDB != $umSaved ]]
then
    function print_error () {
        echo "Aborting."
        echo ""
        echo "This script expects $1 "
        echo "to be reachable at '$pathMonetDB' from the current directory."
        echo ""
        echo "Consider"
        echo "- making it available as a soft link OR"
        echo "- using this script without running MonetDB via the argument "
        echo "  '-um s'"
        echo ""
        echo "Run this script with '--help' for more information."
    }

    if ! [[ -d $pathMonetDB ]]
    then
#        echo "Aborting."
#        echo ""
#        echo "This script expects the directory in which MonetDB was installed to "
#        echo "be reachable at '$pathMonetDB' from the current directory."
        print_error "the directory in which MonetDB was installed"
        exit -1
    fi

    if ! [[ -d $pathMonetDBFarm ]]
    then
#        echo "Aborting."
#        echo ""
#        echo "This script expects the directory of a MonetDB farm to be reachable "
#        echo "at '$pathMonetDBFarm' from the current directory."
        print_error "the directory of a MonetDB farm"
        exit -1
    fi

    if ! [[ -d $pathDBGen ]]
    then
#        echo "Aborting."
#        echo ""
#        echo "This script expects the directory of the source code of the SSB's "
#        echo "dbgen tool to be reachable at '$pathDBGen' from the current "
#        echo "directory."
        print_error "the directory of the source code of the SSB's dbgen tool"
        exit -1
    fi
fi


# *****************************************************************************
# Execution of the selected steps
# *****************************************************************************

if [[ $useMonetDB != $umSaved ]]
then
    # TODO Starting the MonetDB daemon is not required if the user only wants to
    #      build MorphStore.
    printf "Starting MonetDB daemon... "
    eval $monetdbd start $pathMonetDBFarm
    printf "done.\n"
fi

if [[ $startStep -le $stepClean ]] && [[ $stepClean -le $endStep ]]
then
    clean
fi

if [[ $startStep -le $stepGenerate ]] && [[ $stepGenerate -le $endStep ]]
then
    generate
fi

if [[ $startStep -le $stepTranslate ]] && [[ $stepTranslate -le $endStep ]]
then
    translate
fi

if [[ $startStep -le $stepBuild ]] && [[ $stepBuild -le $endStep ]]
then
    build
fi

if [[ $startStep -le $stepRun ]] && [[ $stepRun -le $endStep ]]
then
    run
fi

if [[ $startStep -le $stepVisualize ]] && [[ $stepVisualize -le $endStep ]]
then
    translateToDot
fi

if [[ $useMonetDB != $umSaved ]]
then
    # TODO Stop the MonetDB daemon only if it was not running before.
    printf "Stopping MonetDB daemon... "
    eval $monetdbd stop $pathMonetDBFarm
    printf "done.\n"
fi