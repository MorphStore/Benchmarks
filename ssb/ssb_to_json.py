import json
import ntpath


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


class JSONSerializer:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class Operator:
    def split_operator_name_from_file(self, operator_name_from_file):
        self.operator_name = ...
        self.operator_vector_extension = ...
        self.operator_compress_process_type = [specialized/transient]
        self.operator_input_formats_list = ...
        self.operator_output_formats_list = ...

    def __init__(self, operator_name_from_file):
       self.split_operator_name_from_file(operator_name_from_file)



#
# class Configuration:
#     def __init__(self, operatorList):


class Benchmark(JSONSerializer):
    def __init__(self, benchmark_name, filename):
        self.benchmark_name = benchmark_name
        self.filename = filename
        self.query = path_leaf(filename)


a = Benchmark("blubb","/home/test/abc.csv")
print(a.toJSON())