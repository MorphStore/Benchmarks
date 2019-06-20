class ColumnNode:
    def __init__(self, column_name, label=None):
        self.__column_name = column_name
        self.__column_shape = "cylinder"
        self.__label = label
        self.__width=1.0
        self.__height=1.0

    @property
    def label(self):
        if self.__label is None:
            return ""
        else:
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
        return \
            "{columnname} [" \
            "shape={shape}, " \
            "fillcolor=\"{color}\"," \
            "style=\"filled,{additional_style}\"," \
            "label=\"{label}\", " \
            "id={columnname}, " \
            "width={width}, " \
            "height={height}" \
            "];\n".format(
                columnname=self.get_name_str(),
                shape=self.__column_shape,
                color=self.get_color(),
                additional_style=self.get_additional_style(),
                label=self.label,
                width=self.__width,
                height=self.__height
            )

class BaseColumnNode(ColumnNode):
    def __init__(self,  column_name, label=None):
        # ColumnNode.__init__(self,  column_name, column_name)
        ColumnNode.__init__(self,  column_name, label)
        self.__color = "#7b8596"

    def get_color(self):
        return self.__color

    def get_additional_style(self):
        return ""

class IntermediateColumnNode(ColumnNode):
    def __init__(self,  column_name, isUsed, label=None):
        ColumnNode.__init__(self,  column_name, label)
        self.__isUsed = isUsed
        self.__color = "#f9f25c"
        self.__transparency = "ff" if isUsed else "80"

    @property
    def isUsed(self):
        return self.__isUsed

    def get_color(self):
        return self.__color + self.__transparency

    def get_additional_style(self):
        if self.__isUsed:
            return ""
        else:
            return "dashed"

class ResultColumnNode(ColumnNode):
    def __init__(self,  column_name, label=None):
        ColumnNode.__init__(self,  column_name, label)
        self.__color = "#1a91f2"

    def get_color(self):
        return self.__color

    def get_additional_style(self):
        return ""

def columnFactory(columnName, columnsBase, columnsUsedOut, columnsUnusedOut, columnsResult, label=None):
    if columnName in columnsBase:
        return BaseColumnNode(columnName, label)
    elif columnName in columnsUsedOut:
        return IntermediateColumnNode(columnName, True, label)
    elif columnName in columnsUnusedOut:
        return IntermediateColumnNode(columnName, False, label)
    elif columnName in columnsResult:
        return ResultColumnNode(columnName, label)
    else:
        raise Exception("Columns could not be found")