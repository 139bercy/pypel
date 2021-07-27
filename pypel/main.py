import os
import elasticsearch.helpers
import argparse
import pypel
import pypel.utils.elk.clean_index as clean_index
import pypel.utils.elk.init_index as init_index
import logging.handlers
from typing import Optional


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def process_into_elastic(conf: dict, params: dict, mappings: dict, process: Optional[str] = None):
    """
    Given a set of configurations (global configuration `conf`, process configuration `params` and mapping configuration
    `mappings`, load all processes related to `process`, or all of them if `process` is omitted in to the Elasticsearch
    specified in `conf.elastic_ip`
    :param conf: the configuration file
    :param params: the process' configuration file
    :param mappings: an export of the Elasticsearch indices' mappings
    :param process: the process to load - should be omitted to load all processes
    :return: does not return
    """
    es = elasticsearch.Elasticsearch(hosts=conf["elastic_ip"])
    es_index_client = elasticsearch.client.IndicesClient(es)
    if process is None:
        process_range = get_process_range()
    else:
        process_range = process
    indice = get_indice(params, process_range)
    clean_index.clean_index(mappings, es_index_client, indice)
    init_index.init_index(mappings, es_index_client, indice)
    processes = params.get("Process")
    if processes is None:
        raise ValueError("key 'Process' not found in the passed config, nothing to do.")
    for process_name, parameters in processes:
        if process_range == "all" or process_name in process_range:
            processor = pypel.ProcessFactory().create_process(parameters, es)
            for file in os.listdir(parameters["path_to_data"]):
                processor.process(file, parameters.get("indice"))


def get_process_range():
    """Return the process concerned - in case it's specified at the command line."""
    parser = argparse.ArgumentParser(description='Process the type of Process')
    parser.add_argument("-p", "--process-type", default="all", type=str, help="get the type of Process")
    args = parser.parse_args()
    return args.process_type


def get_indice(params, process):
    """Return the indice corresponding to current process(es)."""
    if process == "all":
        indice = "all"
    elif process in params["Data360Process"].keys():
        indice = params["Data360Process"][process]["indice"]
    else:
        raise Exception(f"The given excel_process argument {process} does not exist in config.json.")
    return indice


def get_path_to_class_data(params, process_name, path_to_data):
    """Return the absolute path to a specific process's subdirectory."""
    return os.path.join(path_to_data, params["Processes"][process_name]["path"])