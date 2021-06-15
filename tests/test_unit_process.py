import pytest
from pypel import Extractor, Transformer, Loader, Process
from pandas import DataFrame


class Elasticsearch:
    pass


class LoaderTest(Loader):
    def load(self, dataframe, indice):
        pass


class ExtractorTest(Extractor):
    def init_dataframe(self, file_path, **kwargs):
        return {"0": file_path, **kwargs}


@pytest.fixture
def df():
    return DataFrame()


@pytest.fixture
def process():
    return Process(extractor=ExtractorTest(), transformer=Transformer(), loader=LoaderTest(Elasticsearch()))


class TestWarnings:
    def test_instanced_transformer_warns_if_addition_params_are_passed(self, process, df):
        with pytest.warns(UserWarning):
            process.transform(df, 0)
        with pytest.warns(UserWarning):
            process.transform(df, keyword_arg=0)

    def test_instanced_loader_warns_if_addition_params_are_passed(self, process, df):
        with pytest.warns(UserWarning):
            process.load(df, "fake", Elasticsearch(), 0)
        with pytest.warns(UserWarning):
            process.load(df, "fake", Elasticsearch(), keyword_arg=0)


class TestParameters:
    def test_extractor_calls_init_dataframe_with_extract_parameters(self, process):
        assert (process.extract("myfile") == {"0": "myfile"})
        assert (process.extract("myfile2", converters={"i convert": "to stuff"})
                == {"0": "myfile2", "converters": {"i convert": "to stuff"}})

class TestBulk:
    def test_bulk_crashes_if_extractor_not_instanced(self):
        process_bad_extractor = Process(transformer=Transformer(), loader=Loader(Elasticsearch(), None))
        with pytest.raises(ValueError):
            process_bad_extractor.bulk({"fake": "also_fake"})

    def test_bulk_crashes_if_transformer_not_instanced(self):
        process_bad_transformer = Process(extractor=Extractor(), loader=Loader(Elasticsearch(), None))
        with pytest.raises(ValueError):
            process_bad_transformer.bulk({"fake": "also_fake"})

    def test_bulk_crashes_if_loader_not_instanced(self):
        process_bad_loader = Process(transformer=Transformer(), extractor=Extractor())
        with pytest.raises(ValueError):
            process_bad_loader.bulk({"fake": "also_fake"})
