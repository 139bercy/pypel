#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 09:35:55 2020

@author: qdimarellis-adc
"""
import os
import elasticsearch
import argparse
from elasticsearch import helpers
import pypel.processes as processes
import pypel.init_indice as init_indice
import pypel.clean_indice as clean_indice
import copy
import logging.handlers


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


def process_into_elastic(conf: dict, params: dict, mappings: dict):
    es = elasticsearch.Elasticsearch(hosts=conf["elastic_ip"])
    es_index_client = elasticsearch.client.IndicesClient(es)
    path_to_data = conf["path_to_data"]
    process_range = get_process_range()
    indice = get_indice(params, process_range)
    clean_indice.clean_indice(mappings, es_index_client, indice)
    init_indice.init_indice(mappings, es_index_client, indice)
    bulk_all_es_actions(es, params, path_to_data, process_range)


def get_process_range():
    parser = argparse.ArgumentParser(description='Process the type of Process')
    parser.add_argument("-p", "--process-type", default="all", type=str, help="get the type of Process")
    args = parser.parse_args()
    return args.process_type


def get_indice(params, process):
    if process == "all":
        indice = "all"
    elif process in params["Data360Process"].keys():
        indice = params["Data360Process"][process]["indice"]
    else:
        raise Exception(f"The given excel_process argument {process} does not exist in config.json.")
    return indice


def bulk_all_es_actions(es, params, path_to_data, process_range: str = "all"):
    process_names = params["Processes"]
    process_factory = processes.ProcessFactory()

    if process_range == "all":
        for process_name in process_names.keys():
            bulk_es_actions(es,
                            params,
                            process_factory,
                            process_name,
                            path_to_data=path_to_data)
    else:
        if process_range in process_names.keys():
            process_name = process_range
            bulk_es_actions(es,
                            params,
                            process_factory,
                            process_name,
                            path_to_data=path_to_data)
        else:
            logger.warning(f"Excel process {process_range} not found")


def bulk_es_actions(es, params, process_factory, process_name, path_to_data):
    indice = params["Processes"][process_name]["indice"]
    process = process_factory.create_process(process_name, indice)
    path_to_class_data = get_path_to_class_data(params, process_name, path_to_data=path_to_data)
    shared_params = copy.deepcopy(params)
    shared_params.pop("Processes")

    files = [os.path.join(path_to_class_data, file)
             for file in os.listdir(path_to_class_data)
             if not os.path.isdir(os.path.join(path_to_class_data, file))]

    bulk_actions = [action
                    for file in files
                    for action in process.get_es_actions(file,
                                                         params["Processes"][process_name],
                                                         global_params=shared_params)]

    success, failed, errors = 0, 0, []
    for ok, item in elasticsearch.helpers.streaming_bulk(es, bulk_actions, raise_on_error=False):
        if not ok:
            errors.append(item)
            failed += 1
        else:
            success += 1
    logger.info(f"{success} successfully inserted into {indice}")
    if errors:
        logger.warning(f"{failed} errors detected\nError details : {errors}")


def get_path_to_class_data(params, process_name, path_to_data):
    return os.path.join(path_to_data, params["Processes"][process_name]["path"])
