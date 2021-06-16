import importlib

from pypel import Transformer
from pypel import Process


class MinimalTransformer(Transformer):
    def format_str_columns(self, df):
        pass


class ProcessFactory:

    def __init__(self, path_to_refs):
        self.path_to_refs = path_to_refs

    def create_process(self, process_name, config, index_pattern):
        canonical_class_name = config["class"]
        class_name_splitted = canonical_class_name.split('.')
        class_name = class_name_splitted[-1]
        module_name = ".".join(class_name_splitted[:-1])

        mod = importlib.import_module(module_name)
        klass = getattr(mod, class_name)
        if not klass:
            raise ValueError(f"{canonical_class_name} not found in ProcessFactory")
        if Process == klass:
            return Process(transformer=MinimalTransformer)
        else:
            return klass(index_pattern, path_to_refs=self.path_to_refs)
