import pytest
import pypel
import elasticsearch
from tests.unit.test_Loader import Elasticsearch, LoaderTest


class Extractor2(pypel.Extractor):
    pass


class FakeExtractor:
    pass


def assert_process_called_with_indice1_to_file1(_, file, indice):
    assert file == "file1"
    assert indice == "indice1"


class TestProcessInstanciation:
    def test_default_instanciates(self):
        pypel.Process()

    def test_bad_type_args_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(extractor="") # noqa
        with pytest.raises(ValueError):
            pypel.Process(transformer="")
        with pytest.raises(ValueError):
            pypel.Process(loader="")  # noqa
        with pytest.raises(ValueError):
            pypel.Process(extractor=[]) # noqa

    def test_instance_from_bad_class_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(extractor=FakeExtractor()) # noqa

    def test_good_args_instanciates(self):
        pypel.Process(extractor=Extractor2())

    def test_list_of_transformers_instanciates(self):
        pypel.Process(transformer=[pypel.Transformer(), pypel.Transformer()])

    def test_list_of_uninstanced_transformers_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(transformer=[pypel.Transformer, pypel.Transformer])

    def test_list_of_wrong_type_fails(self):
        with pytest.raises(ValueError):
            pypel.Process(transformer=[0, 0])


class TestProcessMethods:
    def test_transform_instanced_no_args(self, df):
        process = pypel.Process(transformer=pypel.Transformer(date_format="", date_columns=[]))
        process.transform(df)

    def test_transform_multiple_transformers_extra_args(self, df):
        process = pypel.Process(transformer=[pypel.Transformer(date_format="", date_columns=[]),
                                             pypel.Transformer(date_format="", date_columns=[])])
        with pytest.warns(UserWarning):
            process.transform(df, "extra_argument")

    def test_load_class_loader_fails_if_no_valid_es_instance_is_passed(self, df):
        process = pypel.Process(loader=pypel.Loader)
        with pytest.raises(AssertionError):
            process.load(df, "indice", Elasticsearch())

    def test_load_class_loader_passes_if_valid_es_instance_is_passed(self, df):
        process = pypel.Process(loader=LoaderTest)
        process.load(df, "indice", elasticsearch.Elasticsearch())

    def test_load_instanced_loader_extra_args_warns(self, df):
        process = pypel.Process(loader=LoaderTest(Elasticsearch()))
        with pytest.warns(UserWarning):
            process.load(df, "indice", "extra_arg", extrakeyword="yes")

    def test_load_instanced_does_not_warn(self, df):
        process = pypel.Process(loader=LoaderTest(Elasticsearch()))
        process.load(df, "indice")

    def test_bulk_with_indice_to_list_format(self, monkeypatch):
        processed_dic = {}

        def mock_process_for_multiple_bulk(_, file, indice):
            processed_dic[file] = indice

        monkeypatch.setattr(pypel.Process, "process", mock_process_for_multiple_bulk)
        process = pypel.Process(transformer=pypel.Transformer(), loader=pypel.Loader(Elasticsearch()))
        process.bulk({"indice": ["file1", "file2"]})
        assert processed_dic == {"file1": "indice", "file2": "indice"}

    def test_single_bulk_with_file_indice_format(self, monkeypatch):
        process = pypel.Process(transformer=pypel.Transformer(), loader=pypel.Loader(Elasticsearch()))
        monkeypatch.setattr(pypel.Process, "process", assert_process_called_with_indice1_to_file1)
        process.bulk({"file1": "indice1"})

    def test_multiple_bulk_with_file_indice_format(self, monkeypatch):
        processed_dic = {}

        def mock_process_for_multiple_bulk(_, file, indice):
            processed_dic[file] = indice

        process = pypel.Process(transformer=pypel.Transformer(), loader=pypel.Loader(Elasticsearch()))
        monkeypatch.setattr(pypel.Process, "process", mock_process_for_multiple_bulk)
        process.bulk({"file1": "indice1", "file2": "indice2", "file3": "indice3"})
        assert processed_dic == {"file1": "indice1", "file2": "indice2", "file3": "indice3"}
