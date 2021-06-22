from pypel.processes.Process import Process
from pypel.processes.ProcessFactory import ProcessFactory
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader
from pypel.extractors.Extractor import Extractor
from pypel.main import process_into_elastic
from pypel.utils.elk.init_index import *
from pypel.utils.elk.clean_index import *
from pypel._config.config import set_config, get_config

__all__ = ["Process", "ProcessFactory", "process_into_elastic", "init_index", "clean_index",
           "Loader", "Transformer", "Extractor", "logger", "set_config", "get_config"]

import logging

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))
