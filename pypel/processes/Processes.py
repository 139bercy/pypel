import os
from pypel.extractors.Extractors import BaseExtractor, CSVExtractor
from pypel.transformers.Transformers import Transformer, BaseTransformer
from pypel.loaders.Loaders import Loader, BaseLoader
import warnings
from typing import List, Union, Optional
from pandas import DataFrame


class Process:
    """
    Wrapper around dedicated E(xtract)/T(ransform)/L(oad) classes.

    Parameters
    ----------
    :param extractor: Optional[pypel.extractors.Extractor]
        Extractor instance to use for extracting data. MUST be derived from pypel.extractors.BaseExtractor
    :param transformer: Union[pypel.BaseTransformer, type, List[pypel.transformers.BaseTransformers], None]
        instance, class or list of instances to use for transforming data.
        MUST be derived from pypel.Transformer, for lists, all elements of the list must inherit from pypel.Transformer.
        if list-like, will be for-in looped on, so mind the order.
    :param loader: Union[pypel.loaders.BaseLoader, type, None]
        Loader instance or class to use for loading data. MUST be derived from pypel.loaders.BaseLoader

    Examples
    --------
    Instanciate the default Process

    >>> import pypel
    >>> my_process = pypel.processes.Process()

    Instanciate a Process with a custom Extractor (same logic applies for custom Transformers/Loaders)

    >>> class MyExtractor(pypel.extractors.Extractor):
    ...       pass
    >>> my_extractor_instance = MyExtractor()
    >>> my_process2 = pypel.processes.Process(extractor=my_extractor_instance)
    """
    def __init__(self,
                 extractor: Optional[BaseExtractor] = None,
                 transformer: Union[BaseTransformer, type, List[BaseTransformer], None] = None,
                 loader: Union[Loader, type, None] = None):
        self.extractor = extractor if extractor is not None else CSVExtractor()
        self.transformer = transformer if transformer is not None else Transformer
        self.loader = loader if loader is not None else Loader
        try:
            assert isinstance(self.extractor, BaseExtractor)
        except AssertionError as e:
            raise ValueError("Bad extractor") from e
        try:
            try:
                if isinstance(self.transformer, list):
                    self.__multiple_transformers = True
                    self.__transformer_is_instanced = False
                    for t in self.transformer:
                        assert isinstance(t, BaseTransformer)
                else:
                    self.__multiple_transformers = False
                    assert isinstance(self.transformer(), BaseTransformer)
                    self.__transformer_is_instanced = False
            except TypeError:
                assert isinstance(self.transformer, BaseTransformer)
                self.__transformer_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad transformer") from e
        try:
            try:
                assert issubclass(self.loader, BaseLoader)
                self.__loader_is_instanced = False
            except TypeError:
                assert isinstance(self.loader, BaseLoader)
                self.__loader_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad loader argument") from e

    def process(self, file_path: Union[str, bytes, os.PathLike]) -> None:
        """
        Conveniance wrapper around Process.extract, Process.transform & Process.load. Relies on instanced E/T/L classes.

        :param file_path:
            path to the file to be extracted
        :return: None
        """
        self.load(self.transform(self.extract(file_path)))

    def extract(self, file_path: Union[str, bytes, os.PathLike], **kwargs) -> DataFrame:
        """
        Returns the `Dataframe` obtained from the extactor

        :param file_path: PathLike
            the file to extract from
        :param kwargs:
            extra optional keyword parameters for custom extractor instanciation
        :return: pandas.Dataframe
            the extracted Dataframe
        """
        return self.extractor.extract(file_path, **kwargs) # noqa

    def transform(self, dataframe: DataFrame, *args, **kwargs) -> DataFrame:
        """
        Return the transformed Dataframe

        :param dataframe: pandas.Dataframe
            the input Dataframe
        :param args:
            extra optional positional parameters for custom transformer instanciation
        :param kwargs:
            extra optional keyword parameters for custom transformer instanciation
        :return: pandas.Dataframe
            the transformed Dataframe
        """
        if self.__multiple_transformers:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced transformers receiving extra arguments !")
            result = dataframe
            for transformer in self.transformer:
                result = transformer.transform(result)
            return result
        elif self.__transformer_is_instanced:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced transformer receiving extra arguments !")
            return self.transformer.transform(dataframe)  # noqa
        else:
            return self.transformer(*args, **kwargs).transform(dataframe)

    def load(self, df, *args, **kwargs) -> None:
        """
        Loads the passed dataframe into the passed elasticsearch instance's indice es_indice.

        :param df: pd.Dataframe
            the dataframe to load
        :param args:
            optional positional parameters for custom loader instanciation
        :param kwargs:
            optional keyword parameters for custom loader instanciation
        :return:
        """
        if self.__loader_is_instanced:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced loader receiving extra arguments !")
            self.loader.load(df)
        else:
            self.loader(*args, **kwargs).load(df)

    def bulk(self,  file_list: List[str]) -> None:
        """
        Given a list of files, loads the files into the loader's indice.
            Only works for Process with instanced Extractors, Transformers and Loaders

        =======
        Example
        >>> process = Process(CSVExtractor(), Transformer(), Loader({"user": "", "pwd": ""}, "my_indice"))
        >>> myconf = ["covid_stats.csv", "relance.xls", "obscure_name.xlsx"]
        >>> process.bulk(myconf)
        Equivalent to
        >>> for i in myconf:
        ...     process.process(i)
        Example in the second format
        >>> my_second_conf = ["covid_stats_2020.csv", "covid_stats_2021.csv"]
        >>> process.bulk(my_second_conf)

        :param file_list: the list of files to be bulked into the loader's indice
        :return: None
        """
        try:
            assert (self.__transformer_is_instanced & self.__loader_is_instanced)
        except AssertionError:
            err = ""
            if not self.__transformer_is_instanced:
                err = "Transformer"
            if not self.__loader_is_instanced:
                if err:
                    err = f"{err}, Loader"
                else:
                    err = "Loader"
            raise ValueError(f"{err} not instanced")
        for file in file_list:
            self.process(file)
