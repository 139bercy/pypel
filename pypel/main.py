import json
import os
import warnings
import elasticsearch
import argparse
import ssl
import pypel
import pypel.utils.elk.clean_index as clean_index
import pypel.utils.elk.init_index as init_index
import logging.handlers


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def process_into_elastic(_es, processes: dict):
    """
    Given a set of configurations (global configuration `conf`, process configuration `params` and mapping configuration
    `mappings`, load all processes related to `process`, or all of them if `process` is omitted in to the Elasticsearch
    specified in `conf.elastic_ip`
    :param processes: the configuration file
    :param _es: the elasticsearch instance
    :return: does not return
    """
    # TODO: type hint with a TypedDict
    if processes is None:
        raise ValueError("key 'Processes' not found in the passed config, no idea what to do.")
    for process_name, parameters in processes:
        processor = pypel.ProcessFactory().create_process(parameters, _es)
        # TODO: trouver un meilleur nom que "todo"
        if isinstance(parameters["todo"], list):
            for bulk_op in parameters["todo"]:
                processor.bulk(bulk_op)
        elif isinstance(parameters["todo"], dict):
            processor.bulk(parameters["todo"])
        else:
            warnings.warn("This format of 'todo' is not supported")


def get_args():
    """Return the process concerned - in case it's specified at the command line."""
    parser = argparse.ArgumentParser(description='Process the type of Process')
    parser.add_argument("-f", "--config-file", default="./conf/config.json", type=os.PathLike,
                        help="get the path to the config file to load from")
    parser.add_argument("-c", "--clean", default=False, type=str, help="should elasticsearch indices be cleared")
    parser.add_argument("-m", "--mapping", default="./conf/index_mappings.json", type=os.PathLike,
                        help="path to the elasticsearch indices's mappings")
    return parser.parse_args()


def get_path_to_class_data(params, process_name, path_to_data):
    """Return the absolute path to a specific process's subdirectory."""
    return os.path.join(path_to_data, params["Processes"][process_name]["path"])


def get_es_instance(conf):
    _host = (conf.get("user"), conf.get("pwd"))
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


if __name__ == "__main__":
    args = get_args()
    path_to_conf = os.path.join(os.getcwd(), args.config_file)
    try:
        with open(path_to_conf)as f:
            config = json.load(f)
    except FileNotFoundError as e:
        raise ValueError("Cannot find file passed through the -f / --config-file argument") from e
    path_to_mapping = os.path.join(os.getcwd(), args.mapping)
    try:
        with open(path_to_mapping) as f:
            mappings = json.load(f)
    except FileNotFoundError as e:
        raise ValueError("Cannot find file passed through the -m / --mapping argument") from e
    es = get_es_instance(config)
    if args.clean:
        es_index_client = elasticsearch.client.IndicesClient(es)
        clean_index.clean_index(mappings, es_index_client)
        init_index.init_index(mappings, es_index_client)
    process_into_elastic(es, config)
