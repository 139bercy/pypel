import json
import elasticsearch
import logging
import argparse


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


def get_indice_range():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--indice-name", default="all", type=str, help="get the name of the Indice")
    args = parser.parse_args()
    return args.index_name
