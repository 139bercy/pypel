# -*- coding: UTF-8 -*-
import os
import json
import pandas as pd
import xlrd
from pypel.processes.Process import Process
import logging.handlers
import re
from datetime import datetime


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ExcelProcess(Process):

    def init_dataframe(self, file_path: str, sheet_name: str = 0, skiprows=None):
        if skiprows is None:
            skiprows = skiprows
        else:
            skiprows = ExcelProcess.arrayer(skiprows)
        if file_path.endswith(".csv"):
            return super().init_dataframe(file_path)
        else:
            wb = xlrd.open_workbook(file_path)
            if sheet_name != 0:
                sheet = wb.sheet_by_name(sheet_name)
            else:
                sheet = wb.sheet_by_index(0)
            excel_rows = sheet.nrows
            try:
                file_name = re.findall(r"(?<=/)[.\s\w_-]+$", file_path)[0]
            except IndexError:
                logger.error(f"Could not get file name from file path : {file_path}")
                file_name = "ERROR"
            logger.info(f"{excel_rows} rows in the excel sheet \'{sheet_name}\' from file \'{file_name}\'")
            return pd.read_excel(io=file_path,
                                 skiprows=skiprows,
                                 sheet_name=sheet_name,
                                 converters=self.types,
                                 parse_dates=self.dates)

    @staticmethod
    def arrayer(integer: int):
        return [x for x in range(integer + 1)]
