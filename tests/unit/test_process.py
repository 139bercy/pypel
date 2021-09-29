import pytest
import pypel
from tests.unit.test_Loader import LoaderTest


class Extractor2(pypel.extractors.Extractor):
    pass


class FakeExtractor:
    pass


def assert_process_called_with_es_indice_to_file1(_, file):
    assert file == "file1"


class TestProcessInstanciation:
    def test_default_instanciates(self):
        pypel.processes.Process()

    def test_bad_type_args_fails(self):
        with pytest.raises(ValueError):
            pypel.processes.Process(extractor="")
        with pytest.raises(ValueError):
            pypel.processes.Process(transformer="")
        with pytest.raises(ValueError):
            pypel.processes.Process(loader="")
        with pytest.raises(ValueError):
            pypel.processes.Process(extractor=[])

    def test_instance_from_bad_class_fails(self):
        with pytest.raises(ValueError):
            pypel.processes.Process(extractor=FakeExtractor())

    def test_good_args_instanciates(self):
        pypel.processes.Process(extractor=Extractor2())

    def test_list_of_transformers_instanciates(self):
        pypel.processes.Process(transformer=[pypel.transformers.Transformer(), pypel.transformers.Transformer()])

    def test_list_of_uninstanced_transformers_fails(self):
        with pytest.raises(ValueError):
            pypel.processes.Process(transformer=[pypel.transformers.Transformer, pypel.transformers.Transformer])

    def test_list_of_wrong_type_fails(self):
        with pytest.raises(ValueError):
            pypel.processes.Process(transformer=[0, 0])


class TestProcessMethods:
    def test_transform_instanced_no_args(self, df):
        process = pypel.processes.Process(transformer=pypel.transformers.Transformer(date_format="", date_columns=[]))
        process.transform(df)

    def test_transform_multiple_transformers_extra_args(self, df):
        process = pypel.processes.Process(transformer=[pypel.transformers.Transformer(date_format="", date_columns=[]),
                                          pypel.transformers.Transformer(date_format="", date_columns=[])])
        with pytest.warns(UserWarning):
            process.transform(df, "extra_argument")

    def test_load_class_loader_passes_if_valid_es_config_is_passed(self, df, es_conf, es_indice):
        process = pypel.processes.Process(loader=LoaderTest)
        process.load(df, es_conf, es_indice)

    def test_load_instanced_loader_extra_args_warns(self, df, es_conf, es_indice):
        process = pypel.processes.Process(loader=LoaderTest(es_conf, es_indice))
        with pytest.warns(UserWarning):
            process.load(df, "indice", "extra_arg", extrakeyword="yes")

    def test_load_instanced_does_not_warn(self, df, es_conf, es_indice):
        process = pypel.processes.Process(loader=LoaderTest(es_conf, es_indice))
        process.load(df)

    def test_bulk_with_two_files(self, monkeypatch, es_conf, es_indice):
        processed_dic = {}

        def mock_process_for_multiple_bulk(_, file):
            processed_dic[file] = es_indice

        monkeypatch.setattr(pypel.processes.Process, "process", mock_process_for_multiple_bulk)
        process = pypel.processes.Process(transformer=pypel.transformers.Transformer(),
                                          loader=pypel.loaders.Loader(es_conf, es_indice))
        process.bulk(["file1", "file2"])
        assert processed_dic == {"file1": "test_indice", "file2": "test_indice"}

    def test_single_bulk_with_single_file(self, monkeypatch, es_conf, es_indice):
        process = pypel.processes.Process(transformer=pypel.transformers.Transformer(),
                                          loader=pypel.loaders.Loader(es_conf, es_indice))
        monkeypatch.setattr(pypel.processes.Process, "process", assert_process_called_with_es_indice_to_file1)
        process.bulk(["file1"])
