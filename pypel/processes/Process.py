from pypel.extractors.Extractor import Extractor
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader
import warnings


class Process:
    """
    Default process that only manages .csv and performs minimal modifications :
    - parse dates in columns `dates` according to format `dates_format`
    - strip columns in `strip`
    - uses pandas-type `converters` to read data
    - exports data just before sending it to Elasticsearch's bulk API if `backup` is True.
    """
    def __init__(self,
                 extractor=None,
                 transformer=None,
                 loader=None):
        self.extractor = extractor if extractor else Extractor
        self.transformer = transformer if transformer else Transformer
        self.loader = loader if loader else Loader
        try:
            assert isinstance(self.extractor, Extractor)
            self.__extractor_is_instancied = True
        except TypeError:
            assert isinstance(self.extractor(), Extractor)
            self.__extractor_is_instancied = False
        except AssertionError as e:
            warnings.warn("Invalid extractor !")
            raise ValueError(e)
        try:
            assert isinstance(self.transformer, Transformer)
            self.__transformer_is_instancied = True
        except TypeError:
            assert isinstance(self.transformer(), Transformer)
            self.__transformer_is_instancied = False
        except AssertionError as e:
            warnings.warn("Invalid transformer !")
            raise ValueError(e)
        try:
            # TODO: add support for instanced loaders
            assert isinstance(self.loader("", ""), Loader)
            self.__loader_is_instancied = False
        except AssertionError as e:
            warnings.warn("Invalid loader !")
            raise ValueError(e)

    def process(self, file_path):
        self.load(self.transform(self.extract(file_path)))

    def extract(self, file_path, *args, **kwargs):
        if self.__extractor_is_instancied:
            return self.extractor.init_dataframe(file_path=file_path)
        else:
            return self.extractor(*args, **kwargs).init_dataframe(file_path)

    def transform(self, dataframe, *args, **kwargs):
        if self.__transformer_is_instancied:
            return self.transformer.transform(dataframe=dataframe)
        else:
            return self.transformer(*args, **kwargs).transform(dataframe)

    def load(self, df, *args, **kwargs):
        # instanced loaders not currently supported
        self.loader(*args, **kwargs).load(df)
