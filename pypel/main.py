import json
import os
import pathlib
import sys
import argparse
from pypel.ProcessFactory import ProcessFactory, ProcessConfig
# import pypel.utils.elk.clean_index as clean_index
# import pypel.utils.elk.init_index as init_index
import logging.handlers
from typing import List, TypedDict, Union

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("PYPEL_LOGS", "INFO"))


class Config(TypedDict):
    Processes: List[ProcessConfig]


def select_process_from_config(processes: Config,
                               process: str,
                               files: Union[pathlib.Path, str]):
    """
    Given a pair of configurations (global & process configuration `conf`and mapping configuration
    `mappings`), load all processes related to `process`, or all of them if `process` is omitted in to the Elasticsearch
    specified in `conf.elastic_ip`

    :param processes: the configuration file
    :param process: the process to execute
    :param files: file or list of files to load
    :return: does not return
    """
    if processes is None:
        raise ValueError("key 'Processes' not found in the passed config, no idea what to do.")
    else:
        try:
            assert isinstance(processes, list)
        except AssertionError as e_:
            raise ValueError("Processes is not a list, please encapsulate Processes inside a list even if you only "
                             "have a single Process") from e_
    if process == "all":
        for proc in processes:
            process_from_config(proc, files)
    else:
        if process in [conf.get("name") for conf in processes]:
            process_from_config([conf for conf in processes if conf.get("name") == process][0], files)
        else:
            raise ValueError(f"process {process} not found in the configuration file !")


def process_from_config(process: ProcessConfig, files: Union[pathlib.Path, str]):
    """
    Instantiate the process from its passed configuration, and execute it on passed file or directory

    :param process: the configuration of the process to instantiate
    :param files: the file or directory to process (on)
    :return: None
    """
    processor = ProcessFactory().create_process(process)
    if os.path.isdir(files):
        file_paths = [os.path.join(files, f_).__str__() for f_ in os.listdir(files)]
        processor.bulk(file_paths)
    elif os.path.isfile(files):
        processor.process(files)
    else:
        raise FileNotFoundError(f"Could not find file {files}")


def get_args(args_):
    """Return the process concerned - in case it's specified at the command line."""
    parser = argparse.ArgumentParser(description='Process the type of Process')
    parser.add_argument("-f", "--source-path", type=str, help="path to the file or directory to load from",
                        required=True)
    parser.add_argument("-c", "--config-file", default="./conf/config.json", type=str,
                        help="get the path to the config file to load from")
    # TODO: should that be a pypel functionnality ?
    #  parser.add_argument("-c", "--clean", default=False, type=str, help="should elasticsearch indices be cleared")
    #  parser.add_argument("-m", "--mapping", default="./conf/index_mappings.json", type=pathlib.Path,
    #                    help="path to the elasticsearch indices's mappings")
    parser.add_argument("-p", "--process", default="all", help="specify the process(es) to execute")
    return parser.parse_args(args_)


if __name__ == "__main__":  # pragma: no cover
    args = get_args(sys.argv[1:])
    for arg in args.__dict__:
        logger.debug(arg, args.__getattribute__(arg))
    path_to_conf = os.path.join(os.getcwd(), args.config_file)
    try:
        with open(path_to_conf) as f:
            config = json.load(f)
    except FileNotFoundError as e:
        raise ValueError("Cannot find file passed through the -c / --config-file argument") from e
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
    logger.debug(config.get("Processes"))
    select_process_from_config(config.get("Processes"), args.process, args.source_path)
