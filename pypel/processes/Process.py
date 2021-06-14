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
        instance to use for extracting data. MUST be derived from pypel.Extractor
    :param transformer: pypel.Transformer
        instance to use for transforming data. MUST be derived from pypel.Transformer
    :param loader: pypel.Loader
        instance to use for loading data. MUST be derived from pypel.Loader

    Examples
    --------
    Instanciate the default Process

    >>> import pypel
    >>> my_process = Process()

    Instanciate a Process with a custom Extractor (same logic applies for custom Transformers/Loaders)

    >>> class MyExtractor(pypel..extractors.Extractor):
    ...       def __init__(self):
    ...          super().__init__()
    >>> my_process = Process(extractor=MyExtractor)
    or
    >>> my_extractor_instance = MyExtractor(converter={"columns1": float})
    >>> my_process2 = Process(extractor=my_extractor_instance)
    """
    def __init__(self,
                 extractor: Extractor or type = None,
                 transformer: Transformer or type = None,
                 loader: Loader or type = None):
        self.extractor = extractor if extractor else Extractor
        self.transformer = transformer if transformer else Transformer
        self.loader = loader if loader else Loader
        try:
            assert isinstance(self.extractor(), Extractor)
            self.__extractor_is_instanced = False
        except TypeError:
            assert isinstance(self.extractor, Extractor)
            self.__extractor_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad extractor") from e
        try:
            assert isinstance(self.transformer(), Transformer)
            self.__transformer_is_instanced = False
            if isinstance(self.transformer, list):
                self.multiple_transformers = True
                for t in self.transformer:
                    assert isinstance(t, Transformer)
            else:
                self.multiple_transformers = False
                assert isinstance(self.transformer(), Transformer)
                self.__transformer_is_instancied = False
        except TypeError:
            assert isinstance(self.transformer, Transformer)
            self.__transformer_is_instanced = True
        except AssertionError as e:
            raise ValueError("Bad transformer") from e
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

    def extract(self, file_path, *args, **kwargs):
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
        if self.__extractor_is_instanced:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced extractor receiving extra arguments !")
            return self.extractor.init_dataframe(file_path=file_path) # noqa
        else:
            return self.extractor(*args, **kwargs).init_dataframe(file_path)

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
        if self.__transformer_is_instanced:
            if len(args) + len(kwargs) > 0:
                warnings.warn("Instanced transformer receiving extra arguments !")
            result = dataframe
            for transformer in self.transformer:
                result = transformer.transform(result)
            return result
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
