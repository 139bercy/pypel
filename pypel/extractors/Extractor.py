import pandas as pd
import re
import openpyxl
from pypel.utils.utils import arrayer
import warnings
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Extractor:
    def __init__(self, converters=None, dates=False, dates_format='%Y-%m-%d', sheet_name=0, skiprows=None, **kwargs):
        self.converters = converters
        self.dates = dates
        self.dates_format = dates_format
        self.sheet_name = sheet_name
        self.skiprows = skiprows
        self.additional_pandas_args = kwargs

    def init_dataframe(self, file_path: str):
        """
        Read the passed file and return it as a dataframe.
        Uses pandas.read_csv's `converters` and `parse_dates` parameters.

        :param file_path: absolute path to the file
        :return: pandas.Dataframe object
        """
        if file_path.endswith(".csv"):
            with open(file_path) as file:
                row_count = sum(1 for row in file)
            file_name = re.findall(r"(?<=/)[^/]*$", file_path)[0]
            warnings.warn(f"{row_count} rows (including header)"
                          f"detected in the csv {file_name}")
            return pd.read_csv(file_path,
                               converters=self.converters,
                               parse_dates=self.dates)
        elif file_path.endswith(".xlsx"):
            wb = openpyxl.load_workbook(filename=file_path)
            if self.sheet_name != 0:
                sheet = wb[self.sheet_name]
                self.sheet = self.sheet_name
            else:
                sheet = wb[wb.sheetnames[0]]
                self.sheet = sheet.title
            excel_rows = sheet.max_row
            try:
                file_name = re.findall(r"(?<=/)[.\s\w_-]+$", file_path)[0]
            except IndexError:
                warnings.warn(f"Could not get file name from file path :"
                              f"{file_path}")
                file_name = "ERROR"
            logger.info(f"{excel_rows} rows in the excel sheet \'{self.sheet}\'   from file \'{file_name}\'")
            if self.skiprows is None:
                skiprows = self.skiprows
            else:
                skiprows = arrayer(self.skiprows)
            return pd.read_excel(io=file_path,
                                 skiprows=skiprows,
                                 sheet_name=self.sheet_name,
                                 converters=self.converters,
                                 **self.additional_pandas_args)
        else:
            raise ValueError("File has unsupported file extension")


