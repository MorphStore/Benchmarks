import mal2morphstore.operators as ops
import mal2dot.columns as columns
import mal2dot.operators as operators
import sys

class DataFlow:
    def __init__(self, operatornode, columnnodesInList, columnnodesOutList):
        self.__operatornode = operatornode
        self.__columnnodesInList = columnnodesInList
        self.__columnnodesOutList = columnnodesOutList

    def print_in(self, _tab):
        result = ""
        for inNodes in self.__columnnodesInList:
            result += "{tab}{inNode} -> {operatorNode} [id={id}];\n".format(
                tab=_tab,
                inNode=inNodes.get_escaped_name_str(),
                operatorNode=self.__operatornode.get_name_str(),
                id=self.__operatornode.get_name_str() + "_" + inNodes.get_escaped_name_str()
            )
        return result

    def print_out(self, _tab):
        result = ""
        for outNodes in self.__columnnodesOutList:
            if isinstance(outNodes, columns.IntermediateColumnNode):
                if not outNodes.isUsed:
                    style="dashed"
                else:
                    style="solid"
            else:
                style="solid"
            result += "{tab}{operatorNode} -> {outNode} [style={style}, id={id}];\n".format(
                tab=_tab,
                operatorNode=self.__operatornode.get_name_str(),
                outNode=outNodes.get_escaped_name_str(),
                style=style,
                id=(self.__operatornode.get_name_str()) + "_" + outNodes.get_escaped_name_str()
            )
        return result



class QueryGraph:
    def __init__(self, str_name, str_direction, columnsBase, columnsUsedOut, columnsUnusedOut, columnsResult):
        self.__operator_id = 0
        self.__column_id = 0
        self.__name = str_name
        self.__direction = str_direction
        self.__columnsBase = columnsBase
        self.__columnsUsedOut = columnsUsedOut
        self.__columnsUnusedOut = columnsUnusedOut
        self.__columnsResult = columnsResult
        self.__tab = "   "
        self.__opNameMap = {
            "my_project_wit_t": "project",
            "group_vec": "group",
            "join": "equi_join",
        }
        self.__operator_node_smybol_map = {
            "project":"&#960;",
            "my_project_wit_t": "&#960;",
            "select":"&#963;",
            "intersect_sorted":"&#8745;",
            "merge_sorted":"&#8746;",
            "nested_loop_join":"&#10781;",
            "equi_join":"&#10781;",#<sup>Hash</sup>",
            "semi_join":"&#x22c9;",#<sup>Hash</sup>",
            "calc_binary":"",
            "agg_sum":"&#931;",
            #SumGrBased is missing
            "group":"&#947;",
            "group_vec": "&#947;"
        }
        self.__operators = list()
        self.__columns = set()
        self.__flow = list()

    @property
    def name(self):
        return self.__name

    @property
    def direction(self):
        return self.__direction

    def addOperator(self, op):
        opName = op.opName
        if opName in self.__opNameMap:
            opName = self.__opNameMap[opName]

        if isinstance(op, ops.Select):
            opNode = operators.OperatorNodeSelect(
                opName,
                self.__operator_node_smybol_map[opName],
                self.__operator_id,
                op.op,
                op.val
            )
        elif isinstance(op, ops.Nto1Join):
            opNode = operators.OperatorNodeNto1Join(
                opName, self.__operator_node_smybol_map["equi_join"], self.__operator_id
            )
        elif isinstance(op, ops.LeftSemiNto1Join):
            opNode = operators.OperatorNodeNto1Join(
                opName, self.__operator_node_smybol_map["semi_join"], self.__operator_id
            )
        elif isinstance(op, ops.CalcBinary):
            #print(operator.op, file=sys.stderr)
            opNode = operators.OperatorNodeCalcBinary(
                opName,
                self.__operator_node_smybol_map[opName],
                self.__operator_id,
                op.op
            )
        else :
            opNode = operators.OperatorNode(opName, self.__operator_node_smybol_map[opName], self.__operator_id)

        self.__operators.append(
            opNode
        )
        self.__operator_id+=1
        columnsIn = list()
        columnsOut = list()
        for key in op.__dict__:
            if key.startswith("out") and key.endswith("Col"):
                colName = getattr(op, key)
                colNode = columns.columnFactory(
                    colName,
                    self.__columnsBase,
                    self.__columnsUsedOut,
                    self.__columnsUnusedOut,
                    self.__columnsResult
                )
                columnsOut.append(colNode)
                self.__columns.add(colNode)
            elif key.startswith("in") and key.endswith("Col"):
                colName = getattr(op, key)
                colNode = columns.columnFactory(
                    colName,
                    self.__columnsBase,
                    self.__columnsUsedOut,
                    self.__columnsUnusedOut,
                    self.__columnsResult
                )
                columnsIn.append(colNode)
                self.__columns.add(colNode)

        # print("", file=sys.stderr)
        # print("Operator:", end=" ", file=sys.stderr)
        # print(op.opName, file=sys.stderr)
        # print("In Columns:", file=sys.stderr)
        # for c in columnsIn:
        #     print(c, end=" ", file=sys.stderr)
        # print("Out Columns:", file=sys.stderr)
        # for c in columnsOut:
        #     print(c, end=" ", file=sys.stderr)
        # print("", file=sys.stderr)
        self.__flow.append(DataFlow(opNode, columnsIn, columnsOut))

    def __str__(self):
        result = \
            "digraph {graphname} {{\n" \
            "{tab}graph[\n" \
            "{tab}{tab}charset = \"UTF-8\";\n" \
            "{tab}rankdir = {direction}\n" \
            "{tab}];".format(
                graphname=self.__name,
                tab=self.__tab,
                direction=self.__direction
            )

        for operator in self.__operators:
            result += "{}{}".format(self.__tab, operator)
        for columns in self.__columns:
            result += "{}{}".format(self.__tab, columns)
        for flow in self.__flow:
            result += "{}".format(flow.print_in(self.__tab))
            result += "{}".format(flow.print_out(self.__tab))
        result += "}\n"
        return result
