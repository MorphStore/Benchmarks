#!/usr/bin/env python3



## Star Schema Benchmark (SSB) in MorphStore -- Evaluation
#**What is this?**
#- This notebook can be used to load and visualize the performance measurements obtained with `ssb.sh`.
#
#**Todos**
#- Do not hardcode the scale factor here.
#- Make the bar diagrams' y-limits account for the texts above the bars.
#- I think, all these bar charts can be done much easier with seaborn.



import re
import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import os
import pandas as pd
import seaborn as sns
sns.set_context("talk")
# sns.set_style("whitegrid")
#%matplotlib inline



## The Measurements Data to Use



BASEDIR = ""
RUN_NAMES = [
    # enter result sets here
]
diaDirName = "dias"
os.mkdir(diaDirName)



## Data Utilities



# Column names for the data frames.
COL_RUNNAME      = "runName"
COL_SF           = "sf"
COL_MAJOR        = "major"
COL_MINOR        = "minor"
COL_OPNAME       = "opName"
COL_OPIDX        = "opIdx"
COL_OPRUNTIME_MS = "opRuntime [ms]"



# Function for loading the measurements data from a file.
# TODO Use pd.read_csv() as soon as we separate the outputs of the monitoring and the query results.
pMonitoringLine = re.compile(r"(?P<name>.+?)\t(?P<idx>\d+)\t(?P<runtime>\d+)")
def load(filePath):
    opNames = []
    opIdxs = []
    opRuntimes = []
    foundStart = False
    with open(filePath, "r") as f:
        for line in f:
            line = line[:-1]
            if not foundStart:
                if line == "[MEA]":
                    f.readline() # Skip CSV headline.
                    foundStart = True
            else:
                if line == "[RES]":
                    break
                m = pMonitoringLine.fullmatch(line)
                if m is None:
                    raise RuntimeError("could not parse line '{}'".format(line))
                opNames   .append(    m.group("name"))
                opIdxs    .append(int(m.group("idx")))
                opRuntimes.append(int(m.group("runtime")))
    res = pd.DataFrame({COL_OPIDX: opIdxs, COL_OPNAME: opNames, COL_OPRUNTIME_MS: opRuntimes})
    res[COL_OPRUNTIME_MS] /= 1000
    return res



# Loading all measurements data into one large data frame.
dfAll = pd.DataFrame()
for runName in RUN_NAMES:
    for sf in [1]:
        for major in range(1, 4+1):
            for minor in range(1, 3+1):
                dfQuery = load(os.path.join(BASEDIR, "time_sf{}_{}".format(sf, runName), "q{}.{}.csv".format(major, minor)))
                dfQuery[COL_RUNNAME] = runName
                dfQuery[COL_SF] = sf
                dfQuery[COL_MAJOR] = major
                dfQuery[COL_MINOR] = minor
                dfAll = dfAll.append(dfQuery)
                
                
                
# Loading all measurements data into one large data frame.
dfAll = pd.DataFrame()
for runName in RUN_NAMES:
    for sf in [1]:
        for major in range(1, 4+1):
            for minor in range(1, 3+1):
                dfQuery = load(os.path.join(BASEDIR, "time_sf{}_{}".format(sf, runName), "q{}.{}.csv".format(major, minor)))
                dfQuery[COL_RUNNAME] = runName
                dfQuery[COL_SF] = sf
                dfQuery[COL_MAJOR] = major
                dfQuery[COL_MINOR] = minor
                dfAll = dfAll.append(dfQuery)
                
                
                
## Diagram Utilities



pal = sns.hls_palette(8, l=0.5, s=0.5)
colorByOpName = {
    "query": (0.6, 0.6, 0.6),
    
    "select"                         : pal[0],
    "project"                        : pal[1],
    "intersect_sorted"               : pal[2],
    "merge_sorted"                   : pal[2],
    # TODO Harmonize the names of the join operators.
    "nested_loop_join"               : pal[4],
    "left_semi_nto1_nested_loop_join": pal[4],
    "semi_join"                      : pal[4],
    "equi_join"                      : pal[4],
    "group"                          : pal[5],
    "agg_sum"                        : pal[6],
    "calc_unary"                     : pal[7],
    "calc_binary"                    : pal[7],
}
# Intended to be used for parameter barVisPropsFunc of function bar.
def getColorByOpName(row):
    return {"color": colorByOpName[row[COL_OPNAME]]}



# Intended to be used for parameter labelFunc of function bar.
def getQueryName(row):
    return "Q{}.{}".format(row[COL_MAJOR], row[COL_MINOR])



# Generates a bar diagram.
def bar(df, idx, count, labelFunc, barVisPropsFunc={}, ax=None, figsize=None):
    # df must have a column named COL_OPRUNTIME_MS.
    # The index of df must be a sequence number.
    
    df = df.copy()
    
    if ax is None:
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111)
    else:
        fig = ax.get_figure()
    
#     maxRuntime = df[COL_OPRUNTIME_MS].max()
#     if maxRuntime < 1000:
#         unit = "ms"
#     else:
#         df[COL_OPRUNTIME_MS] /= 1000
#         unit = "s"
    unit = "ms"
    totalRuntime = df[COL_OPRUNTIME_MS].sum()
    
    w = 0.8 / count
    
    for rowIdx, row in df.iterrows():
        opRuntime = row[COL_OPRUNTIME_MS]
        if isinstance(barVisPropsFunc, dict):
            barVisProps = barVisPropsFunc
        elif callable(barVisPropsFunc):
            barVisProps = barVisPropsFunc(row)
        else:
            raise RuntimeError("parameter barVisPropsFunc must be a dict or a callable")
        x = rowIdx + idx * w + w / 2 - w * count / 2
        ax.bar(
            x, opRuntime, w,
            edgecolor="black", linewidth=1,
            **barVisProps
        )
        relOpRuntime = opRuntime / totalRuntime
        ax.text(
#             x, opRuntime, "  {:.1f} {} ({:.1%})".format(opRuntime, unit, relOpRuntime) if relOpRuntime >= 0.01 else "  ≈0",
            x, opRuntime, "  {:.1f} {}".format(opRuntime, unit) if relOpRuntime >= 0.01 else "  ≈0",
            ha="center", va="bottom", color="black", rotation="vertical"
        )
        
#     ax.set_ylim(top=ax.get_ylim()[1] * 1.1)
    ax.set_xticks(df.index)
    if isinstance(labelFunc, str):
        xTickLabels = df[labelFunc]
    elif callable(labelFunc):
        xTickLabels = df.apply(labelFunc, axis=1)
    else:
        raise RuntimeError("parameter labelFunc must be either a str or a callable")
    ax.set_xticklabels(xTickLabels, rotation="vertical")
    ax.set_ylabel("runtime [{}]".format(unit))
    
    return fig, ax



def saveDia(filename):
    plt.savefig("{}/{}.pdf".format(diaDirName, filename), bbox_inches="tight")
    
    
    
## Evalution



### Total runtimes of all SSB queries
#- Shows which query takes how long to execute
#- The reported runtimes
#  - include
#    - everything from immediately before the first operator until immediately after the last operator
#  - do not include
#    - loading base data from disk
#    - printing the query results



ax = None
for runIdx, runName in enumerate(RUN_NAMES):
    for sf in [1]:
        df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == sf) & (dfAll[COL_OPIDX] == 0)]
        df.index = range(len(df))
        fig, ax = bar(df, runIdx, len(RUN_NAMES), getQueryName, {"color": (.6, .6, .6)}, figsize=(12, 5), ax=ax)
        ax.set_title("SSB (sf {}): Total query runtimes".format(sf))
saveDia("total_query_runtimes")



### Total runtimes per operator
#- Shows how much time is spent on which operator
#- Runtimes are sums over all queries and operator occurences



ax = None
for runIdx, runName in enumerate(RUN_NAMES):
    for sf in [1]:
        df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == sf) & (dfAll[COL_OPIDX] > 0)][[COL_OPNAME, COL_OPRUNTIME_MS]].groupby(COL_OPNAME, as_index=False).sum()
        fig, ax = bar(df, runIdx, len(RUN_NAMES), COL_OPNAME, getColorByOpName, figsize=(12, 5), ax=ax)
        ax.set_title("SSB (sf {}): Total runtime per operator, aggregated over all queries".format(sf))
saveDia("total_operator_runtimes")



### Percentage of end-to-end query time spent outside operators
#- Shows in how far the measured total query runtime differs from the sum of the individual operator runtimes
#- *These values should be very low*
#  - High values would indicate that there is something inefficient outside the operators (e.g. something related to the monitoring)



if False:
    for runName in RUN_NAMES:
        print("Run '{}'".format(runName))
        for sf in [1]:
            print("\tSSB (sf {})".format(sf))
            for major in range(1, 4+1):
                for minor in range(1, 3+1):
                    df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == 1) & (dfAll[COL_MAJOR] == major) & (dfAll[COL_MINOR] == minor)]
                    totalOp = df[df[COL_OPIDX] > 0][COL_OPRUNTIME_MS].sum()
                    totalQu = df[df[COL_OPIDX] == 0][COL_OPRUNTIME_MS].values[0]
                    remainder = totalQu - totalOp
                    print("\t\tQ{}.{}: {:.2%}".format(major, minor, (totalQu - totalOp) / totalQu))
                    
                    
                    
### Detailled information on every single SSB query
#- One row per query, three diagrams per row
#  - total query runtime
#  - total runtime per operator
#    - sum over all occurences of the operator in the query
#  - individual operator runtimes
#    - from left to right in the order of occurence in the query
#- Shows
#  - where the time goes
#  - which query operators offer the highest potential for performance improvements
  
  
  
colWs = [1, 3, 10]
for sf in [1]:
    for majorIdx, major in enumerate([1, 2, 3, 4]):
        for minorIdx, minor in enumerate([1, 2, 3]):
            fig = plt.figure(figsize=(40, 6), constrained_layout=True)
            gs = GridSpec(1, sum(colWs), figure=fig)

            axTotalRuntime = fig.add_subplot(gs[0, sum(colWs[:0]):sum(colWs[:1])])
            axRuntimePerOp = fig.add_subplot(gs[0, sum(colWs[:1]):sum(colWs[:2])])
            axIndividualOp = fig.add_subplot(gs[0, sum(colWs[:2]):sum(colWs[:3])])

            fig.text(
                0.5, 1.05, "SSB (sf {}) Q{}.{}".format(sf, major, minor),
                fontsize=24, fontweight="bold", ha="center", va="bottom"
            )
            
            for runIdx, runName in enumerate(RUN_NAMES):
                
                df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == sf) & (dfAll[COL_MAJOR] == major) & (dfAll[COL_MINOR] == minor) & (dfAll[COL_OPIDX] == 0)]
                fig, axTotalRuntime = bar(df, runIdx, len(RUN_NAMES), COL_OPNAME, getColorByOpName, axTotalRuntime)
                axTotalRuntime.set_title("Total runtime")

                df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == sf) & (dfAll[COL_MAJOR] == major) & (dfAll[COL_MINOR] == minor) & (dfAll[COL_OPIDX] > 0)][[COL_OPNAME, COL_OPRUNTIME_MS]].groupby(COL_OPNAME, as_index=False).sum()
                fig, axRuntimePerOp = bar(df, runIdx, len(RUN_NAMES), COL_OPNAME, getColorByOpName, axRuntimePerOp)
                axRuntimePerOp.set_title("Aggregated runtime per operator")

                df = dfAll[(dfAll[COL_RUNNAME] == runName) & (dfAll[COL_SF] == sf) & (dfAll[COL_MAJOR] == major) & (dfAll[COL_MINOR] == minor) & (dfAll[COL_OPIDX] > 0)]
                df.index = df[COL_OPIDX]
                fig, axIndividualOp = bar(df, runIdx, len(RUN_NAMES), COL_OPNAME, getColorByOpName, axIndividualOp)
                axIndividualOp.set_title("Individual operator runtimes")

            saveDia("q{}.{}".format(major, minor))
#            display(fig)
            plt.close()