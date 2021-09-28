import importlib
from pypel.processes import Process
from pypel.loaders.Loaders import ElasticsearchConfig
from typing import Optional, Dict, TypedDict, Union, List
from pypel.main import ProcessConfig


class ProcessFactory:
    def __init__(self, path_to_refs: Optional[str] = None):
        self.path_to_refs = path_to_refs

    def create_process(self, process_config: ProcessConfig, es_instance: Elasticsearch) -> Process:
        """
        Generates a Process matching a configuration passed as parameter.

        EXAMPLE
        =======
        >>> my_config = {"Extractors": {"name": "pypel.Extractor"},
        ...              "Transformers": [{"name": "pypel.BaseTransformer"}, {"name": "pypel.MinimalTransformer"}],
        ...              "Loaders": {"name": "pypel.Loader", "backup": True, "path_to_export_folder": "/"}}
        Will generate a process with the default Extractor, the transformers BaseTransformer AND MinimalTransformer and
            a loader `Loader` with instance parameters `backup` and `path_to_export_folder` set to `True` and `/`
            respectively

        :param process_config: Configuration of the process' E/T/L classes as a dictionnary
        :param es_instance: The Elasticsearch instance to passTYPE_CHECKING to Loader at instanciation
        :return: A Process instance with the E/T/L classes specified in the configuration
        """
        extractors = self.create_subclasses(process_config.get("Extractors"))
        transformers = self.create_subclasses(process_config.get("Transformers"))
        loaders = self.create_subclasses(process_config.get("Loaders"), es_instance=es_instance)
        return Process(extractor=extractors,
                       transformer=transformers,
                       loader=loaders)

    def create_subclasses(self, class_config: Union[Dict[str, str], List[Dict[str, str]]],
                          es_instance: Optional[Elasticsearch] = None):
        if class_config is None:
            return None
        if isinstance(class_config, list):
            return [self.create_single_class(conf, es_instance=es_instance) for conf in class_config]
        else:
            return self.create_single_class(class_config, es_instance=es_instance)

    def create_single_class(self, instance_config: Dict[str, str], es_instance: Optional[Elasticsearch] = None):
        canonical_class_name = instance_config.pop("name")
        class_name_splitted = canonical_class_name.split('.')
        class_name = class_name_splitted[-1]
        module_name = ".".join(class_name_splitted[:-1])
        mod = importlib.import_module(module_name)
        klass = getattr(mod, class_name, None)
        if klass is None:
            raise ValueError(f"{canonical_class_name} not found in module {module_name}")
        if es_instance:
            return klass(es_instance, **instance_config)
        else:
            return klass(**instance_config)
