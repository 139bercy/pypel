from pypel.processes import Process
from pypel.processes import ProcessFactory
from pypel.transformers import Transformer
from pypel.loaders import Loader
from pypel.extractors import Extractor
from pypel.main import *
from pypel.utils.elk.init_index import *
from pypel.utils.elk.clean_index import *
from _config.config import set_config, get_config

__all__ = ["Process", "ProcessFactory", "process_into_elastic", "init_index", "clean_index",
           "Loader", "Transformer", "Extractor", "logger", "set_config", "get_config"]

if get_config()["LOGS"]:
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))
