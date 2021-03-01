import abc
import logging.handlers
from pypel.extractors.BaseExtractor import BaseExtractor
from pypel.transformers.BaseTransformer import BaseTransformer
from pypel.loaders.BaseLoader import BaseLoader


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Process(abc.ABC):
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
        self.extractor = extractor if extractor else BaseExtractor
        self.transformer = transformer if transformer else BaseTransformer
        self.loader = loader if loader else BaseLoader

    def extract(self, file_path, *args, **kwargs):
        return self.extractor(*args, **kwargs).init_dataframe(file_path)

    def transform(self, dataframe, *args, **kwargs):
        return self.transformer(dataframe, *args, **kwargs).transform()

    def load(self, *args, **kwargs):
        self.loader(*args, **kwargs)
