from pypel.extractors.Extractor import Extractor
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader, BaseLoader
from elasticsearch import Elasticsearch
import warnings


class Process:
    """
    Wrapper around dedicated E(xtract)/T(ransform)/L(oad) classes.

    Parameters
    ----------
    :param extractor: pypel.Extractor
        instance or class to use for extracting data. MUST be derived from pypel.Extractor
    :param transformer: pypel.Transformer
        instance, class or list of instances to use for transforming data.
        MUST be derived from pypel.Transformer, for lists, all elements of the list must inherit from pypel.Transformer.
        if list-like, will be for-in looped on, so mind the order.

    :param loader: pypel.Loader
        class to use for loading data. MUST be derived from pypel.Loader

    Examples
    --------
    Instanciate the default Process

    >>> import pypel
    >>> my_process = Process()

    Instanciate a Process with a custom Extractor (same logic applies for custom Transformers/Loaders)

    >>> class MyExtractor(pypel.Extractor):
    ...       pass
    >>> my_extractor_instance = MyExtractor()
    >>> my_process2 = pypel.Process(extractor=my_extractor_instance)
    """
    def __init__(self,
                 extractor: Extractor = None,
                 transformer: Transformer or type = None,
                 loader: Loader or type = None):
        self.extractor = extractor if extractor is not None else Extractor()
        self.transformer = transformer if transformer is not None else Transformer
        self.loader = loader if loader is not None else Loader
        try:
            assert isinstance(self.extractor, Extractor)
        except AssertionError as e:
            raise ValueError("Bad extractor") from e
        try:
            try:
                if isinstance(self.transformer, list):
                    self.__multiple_transformers = True
                    self.__transformer_is_instanced = False
                    for t in self.transformer:
                        assert isinstance(t, Transformer)
                else:
                    self.__multiple_transformers = False
                    assert isinstance(self.transformer(), Transformer)
                    self.__transformer_is_instanced = False
            except TypeError:
                assert isinstance(self.transformer, Transformer)
                self.__transformer_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad transformer") from e
        try:
            try:
                assert isinstance(self.loader(None), BaseLoader) # noqa
                self.__loader_is_instanced = False
            except TypeError:
                assert isinstance(self.loader, BaseLoader)
                self.__loader_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad loader argument") from e

    def process(self, file_path, es_indice, es_instance=None):
        """
        Conveniance wrapper around Process.extract, Process.transform & Process.load

        :param file_path:
            path to the file to be extracted
        :param es_instance:
            elasticsearch connection to use for loading
        :param es_indice:
            the name of the elasticsearch indice in which data is to be loaded
        :return: None
        """
        self.load(self.transform(self.extract(file_path)), es_indice, es_instance=es_instance)

    def extract(self, file_path, **kwargs):
        """
        Returns the `Dataframe` obtained from the extactor

        :param file_path: PathLike
            the file to extract from
        :param args:
            extra optional positional parameters for custom extractor instanciation
        :param kwargs:
            extra optional keyword parameters for custom extractor instanciation
        :return: pandas.Dataframe
            the extracted Dataframe
        """
        return self.extractor.init_dataframe(file_path, **kwargs) # noqa

    def transform(self, dataframe, *args, **kwargs):
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

    def load(self, df, es_indice, es_instance=None, *args, **kwargs):
        """
        Loads the passed dataframe into the passed elasticsearch instance's indice es_indice.

        :param df: pd.Dataframe
            the dataframe to load
        :param es_indice:
            the elasticsearch indice in which to load
        :param es_instance:
            the elasticsearch instance in which to load
        :param args:
            optional positional parameters for custom loader instanciation
        :param kwargs:
            optional keyword parameters for custom loader instanciation
        :return:
        """
        if self.__loader_is_instanced:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced loader receiving extra arguments !")
            self.loader.load(df, es_indice) # noqa
        else:
            assert isinstance(es_instance, Elasticsearch)
            self.loader(es_instance, *args, **kwargs).load(df, es_indice)

    def bulk(self, file_indice_dic: dict[str: str]):
        """
        Given a dict of format {file1: indice1, file2: indice2} extract and transform all files before loading into the
            corresponding indice. Only works for Process with instanced Extractors, Transformers and Loaders

        =======
        Example
        >>> process = Process(Extractor(), Transformer(), Loader())
        >>> myconf = {"covid_stats.csv": "covid", "relance.xls": "relance", "obscure_name": "id7654"}
        >>> process.bulk(myconf)
        Equivalent to
        >>> for f, i in myconf:
        ...     process.process(f, i)

        :param file_indice_dic: the dictionnary of parameters to use for extracting and loading. Must be in the format
            {file1: indice1}
        :return: None
        """
        try:
            assert (self.__transformer_is_instanced & self.__extractor_is_instanced
                    & self.__loader_is_instanced)
        except AssertionError:
            err = ""
            if self.__extractor_is_instanced:
                err = "Extractor,"
            if self.__transformer_is_instanced:
                if err:
                    err = f"{err} Transformer,"
                else:
                    err = "Transformer"
            if self.__loader_is_instanced:
                if err:
                    err = f"{err} Loader,"
                else:
                    err = "Loader"
            raise ValueError(f"{err} are not instanced")
        for file, indice in file_indice_dic.items():
            self.process(file, indice)
