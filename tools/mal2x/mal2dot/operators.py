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
