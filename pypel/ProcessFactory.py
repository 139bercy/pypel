import importlib
from pypel.processes import Process
from typing import Optional, Dict, Union, List, TypedDict


class ProcessConfigMandatory(TypedDict):
    Extractor: Dict[str, str]
    Transformers: List[Dict[str, str]]
    Loader: Dict[str, str]


class ProcessConfig(ProcessConfigMandatory, total=False):
    name: str


class ProcessFactory:
    def __init__(self, path_to_refs: Optional[str] = None):
        self.path_to_refs = path_to_refs

    def create_process(self, process_config: ProcessConfig) -> Process:
        """
        Generates a Process matching a configuration passed as parameter.

        EXAMPLE
        =======
        >>> my_config = {"Extractors": {"name": "pypel.Extractor"},
        ...              "Transformers": [{"name": "pypel.BaseTransformer"}, {"name": "pypel.MinimalTransformer"}],
        ...              "Loaders": {"name": "pypel.Loader", "backup": True, "path_to_export_folder": "/",
        ...                          "indice": "my_es_indice",
        ...                          "time_freq": "_%Y",
        ...                          "es_config": {"user": "elastic",
        ...                                        "pwd": "changeme",
        ...                                        "cafile": "path_to_cafile",
        ...                                        "scheme": "https",
        ...                                        "port": "9200",
        ...                                        "host": "localhost"}}}
        This config would generate a process with the default Extractor, the transformers BaseTransformer
            AND MinimalTransformer and a loader `Loader` with instance parameters `backup` and `path_to_export_folder`
            set to `True` and `/` respectively. Loader will try to connect to elasticsearch using parameters from
            "es_config".

        :param process_config: Configuration of the process' E/T/L classes as a dictionnary
        :return: A Process instance with the E/T/L classes specified in the configuration
        """
        extractor = self.create_subclasses(process_config.get("Extractor"))
        transformers = self.create_subclasses(process_config.get("Transformers"))
        loader = self.create_subclasses(process_config.get("Loader"))
        return Process(extractor=extractor,
                       transformer=transformers,
                       loader=loader)

    def create_subclasses(self, class_config: Union[Dict[str, str], List[Dict[str, str]]]):
        if class_config is None:
            return None
        if isinstance(class_config, list):
            return [self.create_single_class(conf) for conf in class_config]
        else:
            return self.create_single_class(class_config)

    def create_single_class(self, instance_config: Dict[str, str]):
        canonical_class_name = instance_config.pop("name")
        class_name_splitted = canonical_class_name.split('.')
        class_name = class_name_splitted[-1]
        module_name = ".".join(class_name_splitted[:-1])
        mod = importlib.import_module(module_name)
        class_ = getattr(mod, class_name, None)
        if class_ is None:
            raise ValueError(f"{canonical_class_name} not found in module {module_name}")
        else:
            return class_(**instance_config)
