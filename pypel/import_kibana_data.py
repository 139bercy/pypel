# -*- coding: UTF-8 -*-
import os
import json
import subprocess
import shlex
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class _KibanaExporter:
    def __init__(self, path_to_exports, kibana_host_and_port):
        self.path_to_exports = path_to_exports
        self.kibana_host_and_port = kibana_host_and_port

    def import_file(self, file):
        curlcommand = self.create_curl_command(file)
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

    def create_curl_command(self, file):
        path = os.path.join(file)
        return ('curl -X POST '
                + self.kibana_host_and_port
                + '/api/saved_objects/_import?overwrite=true -H "kbn-xsrf: true" --form file=@'
                + path)

    def import_kibana_data(self):
        for file in os.listdir(self.path_to_exports):
            if "export" in file and file.endswith(".ndjson"):
                self.import_file(file)


def import_into_kibana(path_to_exports, kibana_host_and_port):
    exporter = _KibanaExporter(path_to_exports, kibana_host_and_port)
    exporter.import_kibana_data()
