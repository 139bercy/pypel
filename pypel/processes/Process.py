import abc
import pandas as pd
import logging.handlers
import re
import os
import json
import datetime as dt


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Process(abc.ABC):
    """
    Default process that only manages .csv and performs minimal modifications :
    - parse dates in columns `dates` according to format `dates_format`
    - strip columns in `strip`
    - uses pandas-type `converters` to read data
    - exports data just before sending it to Elasticsearch's bulk API if `backup` is True.
    """
    def __init__(self,
                 index_pattern,
                 converters=None,
                 dates=False,
                 strip: list = None,
                 dates_format='%Y-%m-%d',
                 backup=True):
        self.index_pattern = index_pattern
        self.column_replace = {'é': 'e',
                               'î': 'i',
                               'ô': 'o',
                               'è': 'e',
                               'ê': 'e',
                               'ç': 'c',
                               'à': 'a',
                               ' ': '_',
                               r'[(]': '',
                               r'[)]': '',
                               r'-': '_',
                               r'\.': '_'}
        self.df_replace = {"Â(?=°)": "",
                           "à¨": "è",
                           "à®": "î",
                           "à©": "é",
                           "à´": "ô"}
        self.converters = converters
        self.dates = dates
        if strip is None:
            self.columns_to_strip = []
        else:
            self.columns_to_strip = strip
        self.dates_format = dates_format
        self.backup_uploaded_data = backup
        super().__init__()

    def get_es_actions(self, file_path, process_params=None, global_params=None):
        """Wrapper that calls all transform functions and returns an action (in the Elasticsearch python API sense)."""
        sheet_name = process_params.get("sheet_name")
        skiprows = process_params.get("skiprows")
        if isinstance(self, Process) and (sheet_name is not None or skiprows is not None):
            raise TypeError("ERROR: sheet_name/skiprows parameters are not supported by pypel.BaseProcess, please use"
                            "pypel.ExcelProcess instead !\n")
        self.df = self.init_dataframe(file_path, sheet_name=sheet_name, skiprows=skiprows) # noqa
        self.format_dataframe()
        self.wrap_process_specific_transform(process_params, global_params)
        self.parse_date()
        self.fill_all_na()
        return self.wrap_df_in_actions()

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

    def format_dataframe(self):
        """
        returns df_ with no accents in column names, column names all UPPERCASE and _ separated
        also drops column with all na values
        """
        self.df.columns = self.df.columns.str.strip()
        self.df.replace(self.df_replace, regex=True, inplace=True)
        self.df.columns = self.df.columns.to_series().replace(self.column_replace, regex=True).apply(str.upper)
        # remove space for each row in specific column
        # en maj, espace remplacé par "_"
        for column in self.columns_to_strip:
            try:
                self.df[column] = self.df[column].str.strip()
            except KeyError:
                logger.warning(f"NO SUCH COLUMN {column} IN DATAFRAME for indice {self.index_pattern}")

    def wrap_process_specific_transform(self, process_params, global_params):
        pass

    def parse_date(self):
        """
        reformats self.df with DATE columns as datetimes of format year-month-day
        """
        for column in self.df.columns:
            if "DATE" in str(column):
                self.df[column] = pd.to_datetime(self.df[column], errors='coerce', dayfirst=True)
                self.df[column] = self.df[column].dt.strftime(self.dates_format)

    def fill_all_na(self):
        """
        fills missing values of self.df with None
        """
        self.df = self.df.applymap(lambda x: None if x == "NaT" or x == "nan" else x) # noqa
        self.df = self.df.where(pd.notnull(self.df), None) # noqa

    def wrap_df_in_actions(self):
        """
        Reformats the dataframe object as a list of Elasticsearch actions, fit for elasticsearch's bulk API.
        If self.backup is True, save a copy of the dataframe as csv for debugging.

        :return: returns a list of actions (cf Elasticsearch python API documentation)
        """
        if self.backup_uploaded_data:
            self.export_csv()
        logger.info(f"{len(self.df.index)} rows in the dataframe")
        data_dict = self.df.to_dict(orient="index")
        actions = [
            {
                "_index": self.index_pattern,
                "_source": value
            }
            for value in data_dict.values()
        ]
        return actions

    def export_csv(self):
        """Save a the dataframe as csv in a sub-directory of the process's data directory."""
        name_file = "exported_data_" + str(self.index_pattern) + self.get_today_date() + ".csv"
        path_to_csv = os.path.join(self.path_to_folder(), name_file)
        self.df.to_csv(path_to_csv, sep='|', index=False)

    def path_to_folder(self):
        """Return the absolute path to the backup directory, a subdirectory of the process's data directory."""
        path_to_data = Process.get_path_to_data()
        name_folder = "exported_data_" + self.get_today_date()
        path_to_folder = os.path.join(path_to_data, name_folder)
        if not (os.path.exists(path_to_folder)):
            os.mkdir(path_to_folder)
        return path_to_folder

    def get_today_date(self):
        """Return today's date in the process's date format"""
        return dt.datetime.today().strftime(self.dates_format)

    @staticmethod
    def get_path_to_data():
        """Return the absolute path to the data directory."""
        with open("./conf/config.json") as f:  # TODO: this should NOT be hard-coded !!!
            path_to_data = json.load(f)["path_to_data"]
        return path_to_data
