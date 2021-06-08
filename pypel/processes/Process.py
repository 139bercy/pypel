from pypel.extractors.Extractor import Extractor
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader, BaseLoader


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
    >>> my_process = pypel.Process()

    Instanciate a Process with a custom Extractor (same logic applies for custom Transformers/Loaders)

    >>> class MyExtractor(pypel.Extractor):
    ...       def __init__(self):
    ...          super().__init__()
    >>> my_process = pypel.Process(extractor=MyExtractor)
    or
    >>> my_extractor_instance = MyExtractor(converter={"columns1": float})
    >>> my_process2 = pypel.Process(extractor=my_extractor_instance)
    """
    def __init__(self,
                 extractor: Extractor or type = None,
                 transformer: Transformer or type = None,
                 loader: type = None):
        self.extractor = extractor if extractor else Extractor
        self.transformer = transformer if transformer else Transformer
        self.loader = loader if loader else Loader
        try:
            assert isinstance(self.extractor(), Extractor)
            self.__extractor_is_instancied = False
        except TypeError:
            assert isinstance(self.extractor, Extractor)
            self.__extractor_is_instancied = True
        except AssertionError as e:
            raise ValueError("Bad extractor") from e
        try:
            assert isinstance(self.transformer(), Transformer)
            self.__transformer_is_instancied = False
        except TypeError:
            assert isinstance(self.transformer, Transformer)
            self.__transformer_is_instancied = True
        except AssertionError as e:
            raise ValueError("Bad transformer") from e
        try:
            # TODO: add support for instanced loaders
            assert isinstance(self.loader("", ""), BaseLoader)
            self.__loader_is_instancied = False
        except AssertionError as e:
            raise ValueError("Bad loader argument") from e

    def process(self, file_path, es_instance, es_indice):
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
        self.load(self.transform(self.extract(file_path)), es_indice, es_instance)

    def extract(self, file_path, *args, **kwargs):
        """

        :param file_path:
        :param args:
        :param kwargs:
        :return:
        """
        if self.__extractor_is_instancied:
            return self.extractor.init_dataframe(file_path=file_path) # noqa
        else:
            return self.extractor(*args, **kwargs).init_dataframe(file_path)

    def transform(self, dataframe, *args, **kwargs):
        """

        :param dataframe:
        :param args:
        :param kwargs:
        :return:
        """
        if self.__transformer_is_instancied:
            return self.transformer.transform(dataframe=dataframe) # noqa
        else:
            return self.transformer(*args, **kwargs).transform(dataframe)

    def load(self, df, es_indice, es_instance, *args, **kwargs):
        """
        Loads the passed dataframe into the passed elasticsearch instance's indice es_indice.

        :param df: pd.Dataframe
            the dataframe to load
        :param es_indice:
            the elasticsearch indice in which to load
        :param es_instance:
            the elasticsearch instance in which to load
        :param args:
            optional parameters to pass to Loader
        :param kwargs:
            optional parameters to pass to Loader
        :return:
        """
        # instanced loaders not currently supported
        self.loader(es_indice, es_instance, *args, **kwargs).load(df)
