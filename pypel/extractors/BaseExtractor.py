import pandas as pd
import logging.handlers
import re
import os
import json
import datetime as dt
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BaseExtractor:
    def __init__(self, converters=None, dates=False, dates_format='%Y-%m-%d'):
        self.converters = converters
        self.dates = dates
        self.dates_format = dates_format

    def init_dataframe(self, file_path):
        """
        Read the passed file and return it as a dataframe.
        Uses pandas.read_csv's `converters` and `parse_dates` parameters.

        :param file_path: absolute path to the file
        :return: pandas.Dataframe object
        """
        with open(file_path) as file:
            row_count = sum(1 for row in file)
        file_name = re.findall(r"(?<=/)[^/]*$", file_path)[0]
        logger.info(f"{row_count} rows (including header) detected in the csv {file_name}")
        return pd.read_csv(file_path, converters=self.converters, parse_dates=self.dates)


