import importlib
from pypel.processes.Process import Process


class ProcessFactory:

    def __init__(self, path_to_refs=None):
        self.path_to_refs = path_to_refs

    def create_process(self, config):
        extractors = self.create_subclasses(config.get("Extractors"))
        transformers = self.create_subclasses(config.get("Transformers"))
        loaders = self.create_subclasses(config.get("Loaders"))
        return Process(extractor=extractors,
                       transformer=transformers,
                       loader=loaders)

    def create_subclasses(self, config):
        if isinstance(config, list):
            return [self.create_single_class(conf) for conf in config]
        else:
            return self.create_single_class(config)

    def create_single_class(self, config):
        canonical_class_name = config.pop("name")
        class_name_splitted = canonical_class_name.split('.')
        class_name = class_name_splitted[-1]
        module_name = ".".join(class_name_splitted[:-1])
        mod = importlib.import_module(module_name)
        klass = getattr(mod, class_name)
        if not klass:
            raise ValueError(f"{canonical_class_name} not found in ProcessFactory")
        return klass(**config)
