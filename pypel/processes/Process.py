import abc
import logging.handlers
from pypel.extractors.Extractor import Extractor
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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

    def extract(self, file_path, *args, **kwargs):
        return self.extractor(*args, **kwargs).init_dataframe(file_path)

    def transform(self, dataframe, *args, **kwargs):
        return self.transformer(*args, **kwargs).transform(dataframe)

    def load(self, df, *args, **kwargs):
        self.loader(*args, **kwargs).load(df)
