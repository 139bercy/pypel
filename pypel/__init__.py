from pypel.processes import *
from pypel.main import *
from pypel.init_indice import *
from pypel.clean_indice import *
from pypel.import_kibana_data import *
from _config.config import set_config, get_config
__all__ = ["Process", "ProcessFactory", "process_into_elastic", "init_indice", "clean_indice",
           "import_into_kibana", "Loader", "Transformer", "Extractor", "logger", "set_config", "get_config"]

if get_config()["LOGS"]:
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))
