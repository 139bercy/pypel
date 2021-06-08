import elasticsearch.helpers
import pandas as pd
import logging
import warnings
import datetime as dt
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Loader:
    """
    Encapsulates all the loading logic.

    :param indice: str
        the elasticserach indice in which data is to be loaded
    :param elasticsearch_instance: elasticsearch.Elasticsearch
        the elasticsearch connection to load into. MUST be an instance of elasticsearch.Elasticsearch
    :param path_to_export_folder: str
    :param backup:
    :param name_export:
    :param dont_append_date:
    """
    def __init__(self,
                 indice: str,
                 elasticsearch_instance: elasticsearch.Elasticsearch,
                 path_to_export_folder=None,
                 backup=False,
                 name_export=None,
                 dont_append_date=False):
        if backup:
            if path_to_export_folder is None:
                raise ValueError("No export folder passed but backup set to true !")
            if not os.path.isdir(path_to_export_folder):
                raise ValueError("Export folder does not exist but backup set to true !")
        self.indice = indice
        self.backup_uploaded_data = backup
        self.path_to_folder = path_to_export_folder
        self.name_export_file = name_export
        self.es = elasticsearch_instance
        self.append_date = not dont_append_date

    def load(self, dataframe: pd.DataFrame):
        """
        Load passed dataframe using current Loader's parameters

        :param dataframe:
            the dataframe to load
        :return: None
        """
        df = dataframe.copy()
        if self.backup_uploaded_data:
            self._export_csv(df)
        actions = self._wrap_df_in_actions(df)
        self._bulk_into_elastic(actions)

    def _bulk_into_elastic(self, actions: list):
        """
        Attempts to load actions into elasticsearch using the bulk API.
        Successful loads are logged, errors are sent as warnings

        :param actions: a list of elasticsearch actions
        :return: None
        """
        success, failed, errors = 0, 0, []
        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, actions, raise_on_error=False):
            if not ok:
                errors.append(item)
                failed += 1
            else:
                success += 1
        logging.info(f"{success} successfully inserted into {self.indice}")
        if errors:
            warnings.warn(f"{failed} errors detected\nError details : {errors}")

    def _wrap_df_in_actions(self, df: pd.DataFrame):
        """
        Reformats the dataframe object as a list of Elasticsearch actions, fit for elasticsearch's bulk API.
        If self.backup is True, save a copy of the dataframe as csv for debugging.

        :param df: pd.DataFrame
            the DataFrame to upload

        :return: returns a list of actions (cf Elasticsearch python API documentation)
        """
        logger.info(f"{len(df.index)} rows in the dataframe")
        data_dict = df.to_dict(orient="index")
        actions = [
            {
                "_index": self.indice + self._get_date() if self.append_date else self.indice,
                "_source": value
            }
            for value in data_dict.values()
        ]
        return actions

    def _export_csv(self, df: pd.DataFrame, sep: str = '|'):
        """
        Appends the dataframe to the csv located in the loader's backup folder `self.path_to_folder`, creating said csv
            if missing. This means a single Loader has a single backup file, regardless of how many dataframes it loads.

        :param df: the dataframe to save
        :param sep: a single character to use as separator in the resulting csv file
        :return: None
        """
        if not self.name_export_file:
            name_file = "exported_data_" + str(self.indice) + self._get_date() + ".csv"
        else:
            name_file = self.name_export_file
        path_to_csv = os.path.join(self.path_to_folder, name_file)
        df.to_csv(path_to_csv, sep=sep, index=False, mode='a')

    def _get_date(self):
        return dt.datetime.today().strftime("_%m_%d")
