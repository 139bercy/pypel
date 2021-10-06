import pypel.processes as processes
from pypel.ProcessFactory import ProcessFactory
import pypel.transformers as transformers
import pypel.loaders as loaders
import pypel.extractors as extractors
from pypel.main import process_from_config
from pypel.utils.elk.init_index import *
from pypel.utils.elk.clean_index import *
from pypel.config.config import set_config, get_config

__all__ = ["processes", "ProcessFactory", "process_from_config", "init_index", "clean_index", "extractors",
           "loaders", "logger", "set_config", "get_config"]

import logging

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))
