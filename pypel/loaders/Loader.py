import elasticsearch.helpers
import pandas as pd
import logging
import warnings
import datetime as dt
import os
from pypel._config.config import get_config


if get_config().get("LOGS"):
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))


class BaseLoader:
    """Dummy class that all Loaders should inherit from."""
    pass


class Loader(BaseLoader):
    """
    Encapsulates all the loading logic.

    :param elasticsearch_instance: elasticsearch.Elasticsearch
        the elasticsearch connection to load into. MUST be an instance of elasticsearch.Elasticsearch
    :param path_to_export_folder: str
    :param backup:
    :param name_export:
    :param dont_append_date:
    """
    def __init__(self,
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
        self.backup_uploaded_data = backup
        self.path_to_folder = path_to_export_folder
        self.name_export_file = name_export
        self.es = elasticsearch_instance
        self.append_date = not dont_append_date

    def load(self, dataframe: pd.DataFrame, indice: str):
        """
        Load passed dataframe using current Loader's parameters

        :param dataframe:
            the dataframe to load
        :param indice:
            the elasticsearch index in which to upload
        :return: None
        """
        df = dataframe.copy()
        if self.backup_uploaded_data:
            self._export_csv(df, indice)
        actions = self._wrap_df_in_actions(df, indice)
        self._bulk_into_elastic(actions, indice)

    def _bulk_into_elastic(self, actions: list, indice: str):
        """
        Attempts to load actions into elasticsearch using the bulk API.
        Successful loads are logged, errors are sent as warnings

        :param actions: a list of elasticsearch actions
        :param indice: str
            the elasticserach indice in which data is to be loaded
        :return: None
        """
        success, failed, errors = 0, 0, []
        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, actions, raise_on_error=False):
            if not ok:
                errors.append(item)
                failed += 1
            else:
                success += 1
        logging.info(f"{success} successfully inserted into {indice}")
        if errors:
            warnings.warn(f"{failed} errors detected\nError details : {errors}")

    def _wrap_df_in_actions(self, df: pd.DataFrame, indice: str):
        """
        Reformats the dataframe object as a list of Elasticsearch actions, fit for elasticsearch's bulk API.
        If self.backup is True, save a copy of the dataframe as csv for debugging.

        :param df: pd.DataFrame
            the DataFrame to upload
        :param indice: str
            the indice in which to upload
        :return: returns a list of actions (cf Elasticsearch python API documentation)
        """
        logger.info(f"{len(df.index)} rows in the dataframe")
        data_dict = df.to_dict(orient="index")
        actions = [
            {
                "_index": indice + self._get_date() if self.append_date else indice,
                "_source": value
            }
            for value in data_dict.values()
        ]
        return actions

    def _export_csv(self, df: pd.DataFrame, indice: str, sep: str = '|'):
        """
        Appends the dataframe to the csv located in the loader's backup folder `self.path_to_folder`, creating said csv
            if missing. Filename is `exported_data_` OR Loader's name_export_file parameter, followed by target indice
            followed by month & day. This means there is a single backup file per day, per indice. These backups should
            be cleared at least once a year because the year is not added to the filename.

        Example :
        >>> import pandas, elasticsearch
        >>> loader = Loader(elasticsearch.Elasticsearch(), path_to_export_folder="/", backup=True)
        >>> loader.load(pandas.DataFrame(), "my_indice")
        Will create a file `exported_data_my_indice_01_01.csv` in the root directory if you execute it on January 1st.

        >>> loader2 = Loader(elasticsearch.Elasticsearch(), path_to_export_folder="/", backup=True, name_export="EX")
        >>> loader2.load(pandas.DataFrame(), "another_indice")
        Will create a file `EX_another_indice_01_01.csv` in the root directory if executed on the same day.

        :param df: the dataframe to save
        :param indice: str
            the elasticserach indice in which data is to be loaded
        :param sep: a single character to use as separator in the resulting csv file
        :return: None
        """
        if not self.name_export_file:
            name_file = f"exported_data_{indice}{self._get_date()}.csv"
        else:
            name_file = f"{self.name_export_file}{indice}{self._get_date()}.csv"
        path_to_csv = os.path.join(self.path_to_folder, name_file)
        df.to_csv(path_to_csv, sep=sep, index=False, mode='a')

    def _get_date(self):
        return dt.datetime.today().strftime("_%m_%d")
