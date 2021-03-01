import elasticsearch.helpers
import pandas as pd
import logging
import datetime as dt
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Loader:
    def __init__(self,
                 indice,
                 df: pd.DataFrame,
                 elasticsearch_instance,
                 path_to_export_folder=None,
                 backup=False,
                 name_export=None):
        if not path_to_export_folder and backup:
            logger.error("No path for export but backup set to true !")
            return
        self.indice = indice
        self.backup_uploaded_data = backup
        self.df = df.copy()
        self.path_to_folder = path_to_export_folder
        self.name_export_file = name_export
        self.es = elasticsearch_instance

    def load(self):
        if self.backup_uploaded_data:
            self.export_csv()
        self.wrap_df_in_actions()
        self.bulk_into_elastic()

    def bulk_into_elastic(self):
        success, failed, errors = 0, 0, []
        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, self.actions, raise_on_error=False):
            if not ok:
                errors.append(item)
                failed += 1
            else:
                success += 1
        logger.info(f"{success} successfully inserted into {self.indice}")
        if errors:
            logger.warning(f"{failed} errors detected\nError details : {errors}")

    def wrap_df_in_actions(self):
        """
        Reformats the dataframe object as a list of Elasticsearch actions, fit for elasticsearch's bulk API.
        If self.backup is True, save a copy of the dataframe as csv for debugging.

        :return: returns a list of actions (cf Elasticsearch python API documentation)
        """
        logger.info(f"{len(self.df.index)} rows in the dataframe")
        data_dict = self.df.to_dict(orient="index")
        actions = [
            {
                "_index": self.indice + self.get_date(),
                "_source": value
            }
            for value in data_dict.values()
        ]
        self.actions = actions

    def export_csv(self):
        """Save a the dataframe as csv in a sub-directory of the process's data directory."""
        if not self.name_export_file:
            name_file = "exported_data_" + str(self.indice) + self.get_date() + ".csv"
        else:
            name_file = self.name_export_file
        path_to_csv = os.path.join(self.path_to_folder(), name_file)
        self.df.to_csv(path_to_csv, sep='|', index=False)

    def get_date(self):
        return dt.datetime.today().strftime("%d_%m")
