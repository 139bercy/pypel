import json
import os
import pathlib
import warnings
import elasticsearch
import argparse
import ssl
from pypel.processes.ProcessFactory import ProcessFactory
# import pypel.utils.elk.clean_index as clean_index
# import pypel.utils.elk.init_index as init_index
import logging.handlers
from typing import List, Dict, TypedDict, Union

logger = logging.getLogger(__name__)


class ProcessConfigMandatory(TypedDict):
    Extractors: Dict[str, str]
    Transformers: Union[Dict[str, str], List[Dict[str, str]]]
    Loaders: Dict[str, str]
    indice: str


class ProcessConfig(ProcessConfigMandatory):
    name: str


class Config(TypedDict):
    Process: List[ProcessConfig]


def select_process_from_config(_es: elasticsearch.Elasticsearch,
                               processes: Config,
                               process: str,
                               files: pathlib.Path,
                               indice: Union[None, str]):
    """
    Given a set of configurations (global configuration `conf`, process configuration `params` and mapping configuration
    `mappings`, load all processes related to `process`, or all of them if `process` is omitted in to the Elasticsearch
    specified in `conf.elastic_ip`
    :param _es: the elasticsearch instance
    :param processes: the configuration file
    :param process: the process to execute
    :param files: file or list of files to load
    :param indice: the indice to load into
    :return: does not return
    """
    if processes is None:
        raise ValueError("key 'Processes' not found in the passed config, no idea what to do.")
    else:
        assert isinstance(processes, list)
    if process == "all":
        for proc in processes:
            process_from_config(_es, proc, files, indice)
    else:
        if process in [conf["name"] for conf in processes]:
            process_from_config(_es, [conf for conf in processes if conf["name"] == process][0], files, indice)
        else:
            raise ValueError(f"process {process} not found in the configuration file !")


def process_from_config(_es, process, files, indice):
    if indice:
        _indice = indice
    else:
        _indice = process["indice"]
    processor = ProcessFactory().create_process(process, _es)
    if os.path.isdir(files):
        file_paths = [os.path.join(files, f_).__str__() for f_ in os.listdir(files)]
        bulk_op = {indice: file_paths}
        processor.bulk(bulk_op)
    else:
        processor.process(files, _indice, _es)


def get_args():
    """Return the process concerned - in case it's specified at the command line."""
    parser = argparse.ArgumentParser(description='Process the type of Process')
    parser.add_argument("-c", "--config-file", default="./conf/config.json", type=str,
                        help="get the path to the config file to load from")
    parser.add_argument("-f", "--source-path", type=str, help="path to the file or directory to load from")
    # TODO: should that be a pypel functionnality ?
    #  parser.add_argument("-c", "--clean", default=False, type=str, help="should elasticsearch indices be cleared")
    #  parser.add_argument("-m", "--mapping", default="./conf/index_mappings.json", type=pathlib.Path,
    #                    help="path to the elasticsearch indices's mappings")
    parser.add_argument("-p", "--process", default="all", help="specify the process(es) to execute")
    parser.add_argument("-i", "--indice", default=None, help="specify the indice to load into")
    return parser.parse_args()


def get_es_instance(conf):
    _host = (conf.get("user"), conf.get("pwd"))
    if _host == (None, None):
        _host = None
    if "cafile" in conf:
        _context = ssl.create_default_context(cafile=conf["cafile"])
        es_instance = elasticsearch.Elasticsearch(
            conf.get("host", "localhost"),
            http_auth=_host,
            use_ssl=True,
            scheme=conf["scheme"],
            port=conf["port"],
            ssl_context=_context,
        )
    else:
        es_instance = elasticsearch.Elasticsearch(
            conf.get("host", "localhost"),
            http_auth=_host,
        )
    return es_instance


if __name__ == "__main__":  # pragma: no cover
    args = get_args()
    for arg in args.__dict__:
        logger.debug(arg, args.__getattribute__(arg))
    path_to_conf = os.path.join(os.getcwd(), args.config_file)
    try:
        with open(path_to_conf) as f:
            config = json.load(f)
    except FileNotFoundError as e:
        raise ValueError("Cannot find file passed through the -c / --config-file argument") from e
    es = get_es_instance(config)
    """
    if args.clean:
        path_to_mapping = os.path.join(os.getcwd(), args.mapping)
        try:
            with open(path_to_mapping) as f:
                mappings = json.load(f)
        except FileNotFoundError as e:
            raise ValueError("Cannot find file passed through the -m / --mapping argument") from e
        es_index_client = es.indices
        clean_index.clean_index(mappings, es_index_client)
        init_index.init_index(mappings, es_index_client)
    """
    logger.debug(config["Processes"])
    select_process_from_config(es, config["Processes"], args.process, args.source_path, args.indice)
