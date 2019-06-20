import sys
import mal2morphstore.operators as ops
import mal2dot.graph as graph

def dotAnalyze(translationResult, str_name, str_direction="BT"):
    columnsIn = set()
    columnsOut = set()
    lastOperator = None
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            lastOperator = el
            for key in el.__dict__:
                if key.startswith("out"):
                    columnsOut.add(getattr(el, key))
                elif key.startswith("in"):
                    columnsIn.add(getattr(el, key))
    columnsResult = translationResult.resultCols

    columnsUsedOut = columnsOut.intersection(columnsIn)
    columnsBase = columnsIn.difference(columnsOut)
    columnsUnusedOut = (columnsOut.difference(columnsIn)).difference(columnsResult)
    # print("", file=sys.stderr)
    # print("Base Columns:", file=sys.stderr)
    # for c in columnsBase:
    #     print(c, end=" ", file=sys.stderr)
    # print("", file=sys.stderr)
    # print("Used Out Columns:", file=sys.stderr)
    # for c in columnsUsedOut:
    #     print(c, end=" ", file=sys.stderr)
    # print("", file=sys.stderr)
    # print("UnUsed Out Columns:", file=sys.stderr)
    # for c in columnsUnusedOut:
    #     print(c, end=" ", file=sys.stderr)
    # print("", file=sys.stderr)
    # print("Result Columns:", file=sys.stderr)
    # for c in columnsResult:
    #     print(c, end=" ", file=sys.stderr)
    # print("", file=sys.stderr)

    query_graph = graph.QueryGraph(str_name, str_direction, columnsBase, columnsUsedOut, columnsUnusedOut,columnsResult)
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            query_graph.addOperator(el)
    return query_graph
















































