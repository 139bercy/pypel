import elasticsearch.helpers
import pandas as pd
import logging
import warnings
import datetime as dt
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Loader:
    def __init__(self,
                 indice,
                 elasticsearch_instance,
                 path_to_export_folder=None,
                 backup=False,
                 name_export=None,
                 dont_append_date=False):
        if not path_to_export_folder and backup:
            warnings.warn("No path for export but backup set to true !")
            return
        self.indice = indice
        self.backup_uploaded_data = backup
        self.path_to_folder = path_to_export_folder
        self.name_export_file = name_export
        self.es = elasticsearch_instance
        self.append_date = not dont_append_date

    def load(self, dataframe: pd.DataFrame):
        df = dataframe.copy()
        if self.backup_uploaded_data:
            self.export_csv(df)
        actions = self.wrap_df_in_actions(df)
        self.bulk_into_elastic(actions)

    def bulk_into_elastic(self, actions: list):
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

    def wrap_df_in_actions(self, df: pd.DataFrame):
        """
        Reformats the dataframe object as a list of Elasticsearch actions, fit for elasticsearch's bulk API.
        If self.backup is True, save a copy of the dataframe as csv for debugging.

        :return: returns a list of actions (cf Elasticsearch python API documentation)
        """
        logger.info(f"{len(df.index)} rows in the dataframe")
        data_dict = df.to_dict(orient="index")
        actions = [
            {
                "_index": self.indice + self.get_date() if self.append_date else self.indice,
                "_source": value
            }
            for value in data_dict.values()
        ]
        return actions

    def export_csv(self, df: pd.DataFrame):
        """Save a the dataframe as csv in a sub-directory of the process's data directory."""
        if not self.name_export_file:
            name_file = "exported_data_" + str(self.indice) + self.get_date() + ".csv"
        else:
            name_file = self.name_export_file
        path_to_csv = os.path.join(self.path_to_folder(), name_file)
        df.to_csv(path_to_csv, sep='|', index=False)

    def get_date(self):
        return dt.datetime.today().strftime("_%m_%d")
