#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 10:54:19 2020

@author: qdimarellis-adc
"""
import argparse
import json

import elasticsearch

from utils.logs import initialize_logs

logger = initialize_logs(__name__)


def init_index(mappings, es_index_client, indices: str = "all"):
    if indices == "all":
        for key in mappings.keys():
            if es_index_client.exists([key]):
                logger.warning(f"Index patterns already exists !\nUse clean_index.py to delete !")
            else:
                create_indice(mappings, key, es_index_client)
    elif indices == "none":
        pass
    else:
        if indices in mappings.keys():
            if es_index_client.exists([indices]):
                logger.warning(f"Index patterns already exists !\nUse clean_index.py to delete !")
            else:
                create_indice(mappings, indices, es_index_client)


def get_index_range():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index-name", default="all", type=str, help="get the name of the index")
    args = parser.parse_args()
    return args.index_name


def create_indice(mappings, indice, es_index_client):
    body = {"mappings": mappings[indice]["mappings"]}
    es_index_client.create(indice, body)
    logger.info(f"Index {indice} successfully created !")


if __name__ == "__main__":
    with open("./exports_kibana/index_mappings.json") as f:
        MAPPINGS = json.load(f)

    ES = elasticsearch.Elasticsearch(["127.0.0.1:9200"])
    ES_INDICES_CLIENT = elasticsearch.client.IndicesClient(ES)

    init_index(MAPPINGS, ES_INDICES_CLIENT, get_index_range())
