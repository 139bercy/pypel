import elasticsearch.helpers
import pandas as pd
import logging
import datetime as dt
import os
import abc
from pypel.config.config import get_config
from typing import Union, Optional, List, Any, TypedDict, Literal, overload
import ssl


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, get_config()["LOGS_LEVEL"]))


class Action(TypedDict):
    _index: str
    _source: Any


class ElasticsearchHost(TypedDict, total=False):
    host: str


class ElasticsearchMinimal(ElasticsearchHost):
    user: str
    pwd: str


class ElasticsearchSSL(ElasticsearchMinimal):
    cafile: Union[str, bytes, os.PathLike]
    scheme: Literal["https", "ssh"]
    port: str


class BaseLoader:
    """Dummy class that all Loaders should inherit from."""
    @abc.abstractmethod
    def load(self, *args, **kwargs) -> Any:
        """This method must be implemented"""


class Loader(BaseLoader):
    """
    Encapsulates all the loading logic.

    :param es_conf: the configuration to instantiate elasticsearch from
    :param indice: the indice in which to load
    :param time_freq: the strftime format to append to the indice
    :param path_to_export_folder: str
    :param backup:
    :param name_export:
    :param overwrite:  if True, indice with the same name will be trashed and re-created
    """
    @overload
    def __init__(self,
                 es_conf: ElasticsearchMinimal,
                 indice: str,
                 time_freq: str = "_%m_%Y",
                 path_to_export_folder: Union[None, str, bytes, os.PathLike] = None,
                 backup: bool = False,
                 name_export: Optional[str] = None,
                 overwrite: bool = False) -> None: ...

    def __init__(self,
                 es_conf: ElasticsearchSSL,
                 indice: str,
                 time_freq: str = "_%m_%Y",
                 path_to_export_folder: Union[None, str, bytes, os.PathLike] = None,
                 backup: bool = False,
                 name_export: Optional[str] = None,
                 overwrite: bool = False) -> None:
        if backup:
            if path_to_export_folder is None:
                raise ValueError("No export folder passed but backup set to true !")
            if not os.path.isdir(path_to_export_folder):
                raise ValueError("Export folder does not exist but backup set to true !")
        self.backup_uploaded_data = backup
        self.path_to_folder = path_to_export_folder
        self.name_export_file = name_export
        self.es = self._instantiate_es(es_conf)
        self.time_frequency = time_freq
        self.indice = indice + self._get_date()
        self.overwrite = overwrite

    def load(self, dataframe: pd.DataFrame) -> None:
        """
        Load passed dataframe using current Loader's parameters

        :param dataframe:
            the dataframe to load
        :return: None
        """
        df = dataframe.copy()
        if self.overwrite:
            self._recreate_indice()
        if self.backup_uploaded_data:
            self._export_csv(df)
        actions = self._wrap_df_in_actions(df)
        self._bulk_into_elastic(actions)

    def _bulk_into_elastic(self, actions: List[Action]):
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
        logger.info(f"{success} successfully inserted into {self.indice}")
        if errors:
            logger.warning(f"{failed} errors detected")
            logger.debug(f"Error details : {errors}")

    def _wrap_df_in_actions(self, df: pd.DataFrame) -> List[Action]:
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
                "_index": self.indice,
                "_source": value
            }
            for value in data_dict.values()
        ]
        return actions

    def _export_csv(self, df: pd.DataFrame, sep: str = '|') -> None:
        """
        Appends the dataframe to the csv located in the loader's backup folder `self.path_to_folder`, creating said csv
            if missing. Filename is `exported_data_` OR Loader's name_export_file parameter, followed by target indice
            followed by month & day. This means there is a single backup file per day, per indice. These backups should
            be cleared at least once a year because the year is not added to the filename.

        Example :
        >>> import pandas, elasticsearch
        >>> loader = Loader({}, path_to_export_folder="/", backup=True)
        >>> loader.load(pandas.DataFrame(), "my_indice")
        Will create a file `exported_data_my_indice_01_01.csv` in the root directory if you execute it on January 1st.

        >>> loader2 = Loader({}, path_to_export_folder="/", backup=True, name_export="EX")
        >>> loader2.load(pandas.DataFrame(), "another_indice")
        Will create a file `EX_another_indice_01_01.csv` in the root directory if executed on the same day.

        :param df: the dataframe to save
            the elasticserach indice in which data is to be loaded
        :param sep: a single character to use as separator in the resulting csv file
        :return: None
        """
        if not self.name_export_file:
            name_file = f"exported_data_{self.indice}{self._get_date()}.csv"
        else:
            name_file = f"{self.name_export_file}{self.indice}{self._get_date()}.csv"
        path_to_csv = os.path.join(self.path_to_folder, name_file)
        df.to_csv(path_to_csv, sep=sep, index=False, mode='a')

    def _get_date(self) -> str:
        return dt.datetime.today().strftime(self.time_frequency)

    def _instantiate_es(self, es_config) -> elasticsearch.Elasticsearch:
        """
        Instanciates an Elasticsearch connection instance based on connection parameters from the configuration

        :param es_config: the configuration from which to instantiate the es connection
        :return: an elasticsearch.Elasticsearch instance
        """
        _host = (es_config.get("user"), es_config.get("pwd"))
        if _host == (None, None):
            _host = None
        if es_config.get("cafile"):
            _context = ssl.create_default_context(cafile=es_config["cafile"])
            es_instance = elasticsearch.Elasticsearch(
                es_config.get("host", "localhost"),
                http_auth=_host,
                use_ssl=True,
                scheme=es_config["scheme"],
                port=es_config["port"],
                ssl_context=_context,
            )
        else:
            es_instance = elasticsearch.Elasticsearch(
                es_config.get("host", "localhost"),
                http_auth=_host,
            )
        return es_instance

    def _recreate_indice(self) -> None:
        """
        Checks if self.indice exists in the cluster.
        If it does, queries elasticsearch for count of documents in the indice.
        If that count is > 0, queries elasticsearch to to retrieve the indice's mapping, then deletes and recreates it.

        :return: None
        """
        if self.es.indices.exists(self.indice):
            if int(self.es.cat.count(index=self.indice, h="count")[:-1]) > 0:
                props = self.es.indices.get(self.indice).get(self.indice)
                self.es.indices.delete(self.indice)
                self.es.indices.create(self.indice, body={k: props[k] for k in props.keys() & {"mappings"}})


class CSVWriter(BaseLoader):
    """
    Loader that saves the dataframe in a csv file
    """
    def load(self, dataframe: pd.DataFrame, path: os.PathLike, **kwargs) -> None:
        """
        Saves `dataframe` into the csv `path`

        :param dataframe:
            the dataframe to save
        :param path:
            PathLike to the future csv
        :param kwargs:
            Accepts any keyword parameter accepted by pandas.DataFrame.to_csv
        :return: None
        """
        dataframe.to_csv(path, **kwargs)
