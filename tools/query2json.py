

class Query:
    def __init__(self, indentation, query_name, query_string):
        self.__indentation = indentation
        self.__tab = "   "
        self.__query_name = query_name
        self.__query_string = query_string

    def __str__(self):
        return \
            "{indent}{{\n" \
            "{indent}{tab}\"name\": \"{name}\", \n" \
            "{indent}{tab}\"query\": \"{query}\" \n" \
            "{indent}}\n".format(
                indent=self.__indentation,
                tab=self.__tab,
                name=self.__query_name,
                query=self.__query_string
            )


class Benchmark:
    def __init__(self, indentation, benchmark_name):
        self.__indent = indentation
        self.__name = benchmark_name
        self.__queries = list()

    def __str__(self):
        return \
            "{indent}Benchmark {\n" \