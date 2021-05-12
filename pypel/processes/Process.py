from pypel.extractors.Extractor import Extractor
from pypel.transformers.Transformer import Transformer
from pypel.loaders.Loader import Loader


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
        assert isinstance(self.extractor, Extractor)
        self.transformer = transformer if transformer else Transformer
        assert isinstance(self.transformer, Transformer)
        self.loader = loader if loader else Loader
        assert isinstance(self.loader, Loader)

    def process(self, file_path):
        self.load(self.transform(self.extract(file_path)))

    def extract(self, file_path, *args, **kwargs):
        return self.extractor(*args, **kwargs).init_dataframe(file_path)

    def transform(self, dataframe, *args, **kwargs):
        return self.transformer(*args, **kwargs).transform(dataframe)

    def load(self, df, *args, **kwargs):
        self.loader(*args, **kwargs).load(df)
