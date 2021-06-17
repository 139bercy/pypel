# -*- coding: utf-8 -*-
import argparse
import json
import elasticsearch
from pypel.utils.logs import initialize_logs

logger = initialize_logs(__name__)


def clean_index(mappings, es_index_client, indices: str = "all"):
    if indices == "all":
        for key in mappings.keys():
            if es_index_client.exists([key]):
                es_index_client.delete([key])
                logger.info(f"Index {key} deleted!")
            else:
                logger.warning(f"Index {key} doesnt exist !")
    elif indices == "none":
        pass
    else:
        if indices in mappings.keys():
            if es_index_client.exists([indices]):
                es_index_client.delete([indices])
                logger.info(f"Index {indices} deleted !")
            else:
                logger.warning(f"Index {indices} doesnt exist !")
        else:
            logger.warning(f"Index pattern {indices} not found in index_mappings.json")


def get_index_range():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index-name", default="all", type=str, help="get the name of the index")
    args = parser.parse_args()
    return args.index_name


if __name__ == "__main__":
    with open("./exports_kibana/index_mappings.json") as f:
        MAPPINGS = json.load(f)

    ES = elasticsearch.Elasticsearch(["127.0.0.1:9200"])
    ES_INDICES_CLIENT = elasticsearch.client.IndicesClient(ES)

    clean_index(MAPPINGS, ES_INDICES_CLIENT, get_index_range())
