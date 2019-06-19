import mal2morphstore.operators as ops
import sys

class ColumnNode:
    def __init__(self, column_name, label=None):
        self.__column_name = column_name
        self.__column_shape = "cylinder"
        self.__label = label

    @property
    def label(self):
        return self.__label

    def get_name_str(self):
        return self.__column_name.replace(".", "_")

    def __hash__(self):
        return hash(self.__column_name)

    def __eq__(self, other):
        return self.__column_name == other.get_name_str

    def __ne__(self, other):
        return not(self == other)

    def __str__(self):
        if self.__label is None:
            return \
                "{columnname} [shape={shape}];\n".format(
                    columnname=self.get_name_str(),
                    shape="\"" + self.__column_shape + "\""
                )
        else:
            return \
                "{columnname} [shape={shape}, label={label}];\n".format(
                    columnname=self.get_name_str(),
                    shape="\"" + self.__column_shape + "\"",
                    label="\"" + self.__label + "\""
                )

class OperatorNode:
    def __init__(self, str_operator_name, str_operator_symbol, operator_id):
        self.__operator_name_delimeter = "_"
        self.__operator_symbol_id_tag_begin = "<sub>"
        self.__operator_symbol_id_tag_end = "</sub>"
        self.__operator_shape = "circle"
        self.__operator_fontsize = "24"
        self.__operator_shape_size = "0.8"
        self.__operator_name = str_operator_name
        self.__operoator_id = operator_id
        self.__operator_symbol = str_operator_symbol
    @property
    def operator_name(self):
        return self.__operator_name.replace(".", "_")
    @property
    def symbol(self):
        return self.__operator_symbol

    def get_name_str(self):
        return \
            "{}{}{}".format(
                self.__operator_name,
                self.__operator_name_delimeter,
                str(self.__operoator_id)
            )

    def get_symbol_str(self):
        return \
            "\"" + self.__operator_symbol + \
            self.__operator_symbol_id_tag_begin + \
            str(self.__operoator_id) + \
            self.__operator_symbol_id_tag_end + "\""
    def __str__(self):
        print(self.get_name_str(), file=sys.stderr)
        return \
            "{operatorname} [shape={shape}, label={symbol}, fontsize={fontsize}, width={size}, height={size}];\n".format(
                operatorname=self.get_name_str(),
                shape="\""+self.__operator_shape+"\"",
                symbol=self.get_symbol_str(),
                fontsize=self.__operator_fontsize,
                size=self.__operator_shape_size
            )

class DataFlow:
    def __init__(self, operatornode, columnnodesInList, columnnodesOutList):
        self.__operatornode = operatornode
        self.__columnnodesInList = columnnodesInList
        self.__columnnodesOutList = columnnodesOutList

    def print_in(self, _tab):
        result = ""
        for inNodes in self.__columnnodesInList:
            result += "{tab}{inNode} -> {operatorNode};\n".format(
                tab=_tab,
                inNode=inNodes.get_name_str(),
                operatorNode=self.__operatornode.get_name_str()
            )
        return result

    def print_out(self, _tab):
        result = ""
        for outNodes in self.__columnnodesOutList:
            result += "{tab}{operatorNode} -> {outNode};\n".format(
                tab=_tab,
                operatorNode=self.__operatornode.get_name_str(),
                outNode=outNodes.get_name_str()
            )
        return result




class QueryGraph:
    def __init__(self, str_name, str_direction):
        self.__operator_id = 0
        self.__column_id = 0
        self.__name = str_name
        self.__direction = str_direction
        self.__tab = "   "
        self.__operator_node_smybol_map = {
            "project":"&#960;",
            "select":"&#963;",
            "intersect_sorted":"&#8745;",
            "merge_sorted":"&#8746;",
            "nested_loop_join":"&#10781;",
            "equi_join":"&#10781;<sup>Hash</sup>",
            "semi_join":"&#x22c9;<sup>Hash</sup>",
            "calc_binary":"CALC",
            "agg_sum":"&#931;",
            #SumGrBased is missing
            "group":"GROUP"
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

    def addOperator(self, operator):
        opNode = OperatorNode(operator.opName, self.__operator_node_smybol_map[operator.opName], self.__operator_id)
        self.__operators.append(
            opNode
        )
        self.__operator_id+=1
        columnsIn = list()
        columnsOut = list()
        for key in operator.__dict__:
            if key.startswith("out"):
                colNode = ColumnNode(getattr(operator, key))
                columnsOut.append(colNode)
                self.__columns.add(colNode)
            elif key.startswith("in"):
                colNode = ColumnNode(getattr(operator, key))
                columnsIn.append(colNode)
                self.__columns.add(colNode)
        self.__flow.append(DataFlow(opNode, columnsIn, columnsOut))

    def __str__(self):
        result = \
            "digraph {graphname} {{\n" \
            "{tab}rankdir = {direction};\n".format(
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

def dotAnalyze(translationResult, str_name, str_direction="BT"):
    graph = QueryGraph(str_name, str_direction)
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            graph.addOperator(el)
    return graph

















































