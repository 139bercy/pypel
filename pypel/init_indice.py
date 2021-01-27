#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 10:54:19 2020

@author: qdimarellis-adc
"""
import json
import elasticsearch
import logging
import argparse


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def init_indice(mappings, es_indice_client, indices: str = "all"):
    if indices == "all":
        for key in mappings.keys():
            if es_indice_client.exists([key]):
                logger.warning(f"Indice already exists ! " + key)
            else:
                create_indice(mappings, key, es_indice_client)
    elif indices == "none":
        return
    else:
        if indices in mappings.keys():
            if es_indice_client.exists([indices]):
                logger.warning(f"Indice already exists ! " + indices)
            else:
                create_indice(mappings, indices, es_indice_client)


def get_indice_range():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--indice-name", default="all", type=str, help="get the name of the indice")
    args = parser.parse_args()
    return args.indice_name


def create_indice(mappings, key, es_indice_client):
    body = {"mappings": mappings[key]["mappings"]}
    es_indice_client.create(key, body)
    logger.info(f"Indice {key} successfully created !")


def create_agg_indices(agg_mappings, agg_indice, es_indice_client, base_agg_indice_name):
    body = {"mappings": agg_mappings[base_agg_indice_name]["mappings"]}
    es_indice_client.create(agg_indice, body)
    logger.info(f"Indice {agg_indice} successfully created !")
