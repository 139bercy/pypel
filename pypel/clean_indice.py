# -*- coding: utf-8 -*-
import json
import elasticsearch
import logging
import argparse


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
fh = logging.FileHandler("./logging/index.log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)-25s - %(levelname)-8s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)


def clean_indice(mappings, es_indice_client, indices: str = "all"):
    if indices == "all":
        for key in mappings.keys():
            if es_indice_client.exists([key]):
                es_indice_client.delete([key])
                logger.info(f"Indice {key} deleted!")
            else:
                logger.warning(f"Indice {key} doesnt exist !")
    elif indices == "none":
        return
    else:
        if indices in mappings.keys():
            if es_indice_client.exists([indices]):
                es_indice_client.delete([indices])
                logger.info(f"Indice {indices} deleted !")
            else:
                logger.warning(f"Indice {indices} doesnt exist !")
        else:
            logger.warning(f"Indice {indices} not found in index_mappings.json")


def clean_aggregated_indice(agg_mappings, mappings, es_indice_client, indices: str = "all"):
    if indices == "all":
        for base_agg_indice_name in agg_mappings.keys():
            agg_indices = [base_agg_indice_name + "_" + str(indice)
                           for indice in mappings.keys()]

            for agg_indice in agg_indices:
                if (es_indice_client.exists(agg_indice)):
                    es_indice_client.delete(agg_indice)
                    logger.info(f"Indices {agg_indice} successfully deleted")
                else:
                    logger.warning(f"Indice {base_agg_indice_name} does not exist")
    elif indices == "none":
        return
    else:
        if indices in mappings.keys():
            if es_indice_client.exists([indices]):
                es_indice_client.delete(indices)
                logger.info(f"Indices {indices} successfully deleted")
            else:
                logger.warning(f"Indice {indices} does not exist")

def get_indice_range():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--indice-name", default="all", type=str, help="get the name of the Indice")
    args = parser.parse_args()
    return args.index_name



if __name__ == "__main__":
    with open("./exports_kibana/index_mappings.json") as f:
        MAPPINGS = json.load(f)
    with open("./conf/config.json") as f:
        ip_conf = json.load(f)
    ES = elasticsearch.Elasticsearch(hosts=ip_conf["elastic_ip"])
    ES_INDICES_CLIENT = elasticsearch.client.IndicesClient(ES)

    clean_indice(MAPPINGS, ES_INDICES_CLIENT, get_indice_range())
