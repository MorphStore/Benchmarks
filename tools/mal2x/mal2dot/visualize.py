import mal2morphstore.operators as ops
import sys

class ColumnNode:
    def __init__(self, column_name, label=None):
        self.__column_name = column_name
        self.__column_shape = "cylinder"
        self.__label = label
        self.__width=1.0
        self.__height=1.0

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
                "{columnname} [shape={shape}, label=\"\", id={columnname}, width={width}, height={height}];\n".format(
                    columnname=self.get_name_str(),
                    shape="\"" + self.__column_shape + "\"",
                    width=self.__width,
                    height=self.__height
                )
        else:
            return \
                "{columnname} [shape={shape}, label={label}, id={columnname}, width={width}, height={height}];\n".format(
                    columnname=self.get_name_str(),
                    shape="\"" + self.__column_shape + "\"",
                    label="\"" + self.__label + "\"",
                    width=self.__width,
                    height=self.__height
                )

class OperatorNode:
    def __init__(self, str_operator_name, str_operator_symbol, operator_id):
        self.__operator_name_delimeter = "_"
        #self.__operator_symbol_id_tag_begin = "<sub>"
        #self.__operator_symbol_id_tag_end = "</sub>"
        self.__operator_shape = "circle"
        self.__operator_fontsize = 24
        self.__operator_shape_size = "1.2"
        self.__operator_name = str_operator_name
        self.__operoator_id = operator_id
        self.__operator_symbol = str_operator_symbol
    @property
    def operator_name(self):
        return self.__operator_name.replace(".", "_")
    @property
    def symbol(self):
        return self.__operator_symbol
    @property
    def fontsize(self):
        return self.__operator_fontsize

    def get_name_str(self):
        return \
            "{}{}{}".format(
                self.__operator_name,
                self.__operator_name_delimeter,
                str(self.__operoator_id)
            )

    def get_symbol_str(self):
        return \
            self.__operator_symbol

    def __str__(self):
        #print(self.get_name_str(), file=sys.stderr)
        return \
            "{operatorname} [fixedsize=shape, shape={shape}, label=<{symbol}>, id={operatorname}, fontsize={fontsize}, width={size}, height={size}];\n".format(
                operatorname=self.get_name_str(),
                shape="\""+self.__operator_shape+"\"",
                symbol=self.get_symbol_str(),
                fontsize=self.__operator_fontsize,
                size=self.__operator_shape_size
            )

class OperatorNodeSelect(OperatorNode):
    def __init__(self, str_operator_name, str_operator_symbol, operator_id, op, predicate):
        OperatorNode.__init__(self, str_operator_name, str_operator_symbol, operator_id)
        self.__op_symbol_map = {
            "equal": "=",
            "inequal": "&#x2260;",
            "less": "&lt;",
            "lessequal": "&#x2264;",
            "greater": "&gt;",
            "greaterequal": "&#x2265;"
        }
        self.__op = self.__op_symbol_map[op]
        self.__predicate = str(predicate)

    def get_symbol_str(self):
        return \
            "{}<FONT POINT-SIZE=\"{}\">{}{}</FONT>".format(
                super().get_symbol_str(),
                str((super().fontsize) / 2 ),
                self.__op,
                self.__predicate
            )
class OperatorNodeNto1Join(OperatorNode):
    def __init__(self, str_operator_name, str_operator_symbol, operator_id):
        OperatorNode.__init__(self, str_operator_name, str_operator_symbol, operator_id)

    def get_symbol_str(self):
        return \
            "{}<FONT POINT-SIZE=\"{}\">{}</FONT>".format(
                super().get_symbol_str(),
                str((super().fontsize) / 2),
                "1:N"
            )

class OperatorNodeCalcBinary(OperatorNode):
    def __init__(self, str_operator_name, str_operator_symbol, operator_id, op):
        OperatorNode.__init__(self, str_operator_name, str_operator_symbol, operator_id)
        self.__op_symbol_map = {
            "mul": "*",
            "sub": "-",
        }
        self.__op = self.__op_symbol_map[op]

    def get_symbol_str(self):
        return \
            "[{}]".format(
                self.__op
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
            "equi_join":"&#10781;",#<sup>Hash</sup>",
            "semi_join":"&#x22c9;",#<sup>Hash</sup>",
            "calc_binary":"",
            "agg_sum":"&#931;",
            #SumGrBased is missing
            "group":"&#947;"
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
        if isinstance(operator, ops.Select):
            opNode = OperatorNodeSelect(
                operator.opName,
                self.__operator_node_smybol_map[operator.opName],
                self.__operator_id,
                operator.op,
                operator.val
            )
        elif isinstance(operator, ops.Nto1Join):
            opNode = OperatorNodeNto1Join(
                operator.opName, self.__operator_node_smybol_map["equi_join"], self.__operator_id
            )
        elif isinstance(operator, ops.LeftSemiNto1Join):
            opNode = OperatorNodeNto1Join(
                operator.opName, self.__operator_node_smybol_map["semi_join"], self.__operator_id
            )
        elif isinstance(operator, ops.CalcBinary):
            #print(operator.op, file=sys.stderr)
            opNode = OperatorNodeCalcBinary(
                operator.opName,
                self.__operator_node_smybol_map[operator.opName],
                self.__operator_id,
                operator.op
            )
        else :
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
            "{tab}graph[\n" \
            "{tab}{tab}charset = \"UTF-8\";\n" \
            "{tab}{tab}label = \"{graphname}\",\n" \
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

def dotAnalyze(translationResult, str_name, str_direction="BT"):
    graph = QueryGraph(str_name, str_direction)
    for el in translationResult.prog:
        if isinstance(el, ops.Op):
            graph.addOperator(el)
    return graph

















































