# -*- coding: UTF-8 -*-
import os
import json
import subprocess
import shlex
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
fh = logging.FileHandler("./logging/import.log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)-45s - %(levelname)-8s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)


def import_kibana_data(path_to_exports, kibana_host_and_port):
    for file in os.listdir(path_to_exports):
        if "export" in file and file.endswith(".ndjson"):
            import_file(file, kibana_host_and_port, path_to_exports)


def import_file(file, kibana_host_and_port, path_to_exports):
    curlcommand = create_curl_command(path_to_exports, file, kibana_host_and_port)
    args = shlex.split(curlcommand)
    proc = subprocess.Popen(args=args, stderr=subprocess.PIPE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    if out:
        clean_out = str(out)[2:-1]
        data = json.loads(clean_out)
        if data.get("error", False):
            logger.warning(f"Failed to import file \'{file}\'!")
            logger.debug(f"Response message : {clean_out}")
        else:
            logger.info(f"Successfully created space {data.get('name')}")
            logger.debug(f"Import data details : {clean_out}")


def create_curl_command(path_to_exports, file, kibana_host_and_port):
    path = os.path.join(path_to_exports, file)
    return ('curl -X POST '
            + kibana_host_and_port
            + '/api/saved_objects/_import?overwrite=true -H "kbn-xsrf: true" --form file=@'
            + path)


if __name__ == "__main__":
    with open("./conf/config.json") as f:
        conf = json.load(f)
    with open("./conf/parameters.json") as f:
        params = json.load(f)

    path_to_kibana_exports = conf["path_to_kibana_exports"]
    kibana_info = conf["kibana_info"]

    import_kibana_data(path_to_kibana_exports, kibana_info)
