import pytest
from pypel.extractors import Extractor
from pypel.loaders import Loader
from pypel.transformers import Transformer
from pypel.processes import Process
from pandas import DataFrame


class LoaderTest(Loader):
    def load(self, dataframe):
        pass


class ExtractorTest(Extractor):
    def extract(self, file_path, **kwargs):
        return {"0": file_path, **kwargs}


@pytest.fixture
def df():
    return DataFrame()


@pytest.fixture
def process(es_conf):
    return Process(extractor=ExtractorTest(), transformer=Transformer(), loader=LoaderTest(es_conf, "dummy"))


class TestWarnings:
    def test_instanced_transformer_warns_if_addition_params_are_passed(self, process, df):
        with pytest.warns(UserWarning):
            process.transform(df, 0)
        with pytest.warns(UserWarning):
            process.transform(df, keyword_arg=0)

    def test_instanced_loader_warns_if_addition_params_are_passed(self, process, df):
        with pytest.warns(UserWarning):
            process.load(df, "fake", {}, 0)
        with pytest.warns(UserWarning):
            process.load(df, "fake", {}, keyword_arg=0)


class TestParameters:
    def test_extractor_calls_init_dataframe_with_extract_parameters(self, process):
        assert (process.extract("myfile") == {"0": "myfile"})
        assert (process.extract("myfile2", converters={"i convert": "to stuff"})
                == {"0": "myfile2", "converters": {"i convert": "to stuff"}})


class TestBulk:
    def test_bulk_crashes_if_transformer_not_instanced(self, es_conf, es_indice):
        process_bad_transformer = Process(loader=Loader(es_conf, es_indice))
        with pytest.raises(ValueError, match="Transformer not instanced"):
            process_bad_transformer.bulk([])

    def test_bulk_crashes_if_loader_not_instanced(self):
        process_bad_loader = Process(transformer=Transformer())
        with pytest.raises(ValueError, match="Loader not instanced"):
            process_bad_loader.bulk([])

    def test_bulk_crashes_if_both_not_instanced(self):
        process_bad_loader = Process()
        with pytest.raises(ValueError, match="Transformer, Loader not instanced"):
            process_bad_loader.bulk([])

    def test_bulk(self, es_conf, es_indice, mocker):
        mocker.patch('pypel.processes.Process.process')
        process = Process(transformer=Transformer(), loader=Loader(es_conf, es_indice))
        arg = ["file1", "file2", "file3"]
        process.bulk(arg)
        assert Process.process.call_count == 3
        calls = [mocker.call(file) for file in arg]
        print("mock_calls", Process.process.mock_calls)
        Process.process.assert_has_calls(calls)
