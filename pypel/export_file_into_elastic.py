# -*- coding: UTF-8 -*-
import pandas as pd
import json
import argparse
import elasticsearch
import elasticsearch.helpers
import numpy as np


def main():
    try:
        with open("./conf/config.json") as f:
            config = json.load(f)
    except FileNotFoundError:
        config = {}
    file_path, indice, sep = get_args()
    export_file_into_elastic(file_path, indice, config, sep)


def export_file_into_elastic(file, indice, config, sep):
    elastic_ip = config.get("elastic_ip", "127.0.0.1:9200")
    user = config.get("user", "")
    pwd = config.get("pwd", "")

    es = elasticsearch.Elasticsearch([elastic_ip], http_auth=(user, pwd), scheme="http", request_timeout=60)

    if file.endswith(".xlsx"):
        df = pd.read_excel(file)
    elif file.endswith(".csv"):
        df = pd.read_csv(file, sep=sep)
    else:
        raise ValueError(f"{file} is not a valid file.")
    df.replace({np.nan: None, pd.NaT: None}, inplace=True)
    df.fillna(0, inplace=True)
    data_dict = df.to_dict(orient="index")
    actions = [
        {
            "_index": indice,
            "_source": value
        }
        for value in data_dict.values()
    ]
    success, failed, errors = 0, 0, []
    for ok, item in elasticsearch.helpers.streaming_bulk(es, actions, raise_on_error=False):
        if not ok:
            errors.append(item)
            failed += 1
        else:
            success += 1
    print(f"{success} successfully inserted into {indice}")
    if errors:
        print(f"{failed} errors detected\nError details : {errors}")


def get_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument("-f", "--file-path", default=None, type=str, help="")
    parser.add_argument("-i", "--index", default=None, type=str, help="")
    parser.add_argument("-s", "--separator", default=",", type=str)
    args = parser.parse_args()
    return args.file_path, args.index, args.separator


if __name__ == "__main__":
    main()

