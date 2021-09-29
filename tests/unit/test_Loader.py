import os
import tempfile
import pytest
import pypel.loaders.Loaders as loader
from pandas import DataFrame
import elasticsearch
import ssl


class LoaderTest(loader.Loader):
    def _bulk_into_elastic(self, actions):
        pass


@pytest.fixture
def es_instanciating_loader(es_conf, es_indice):
    return LoaderTest(es_conf, es_indice)


def mock_streaming_bulk_no_error(*args, **kwargs):
    for a in range(10):
        yield True, {"success": True}


def mock_streaming_bulk_some_errors(*args, **kwargs):
    for a in range(10):
        if a > 6:
            yield False, {"error": {"fake_reason": "fake_error"}}
        else:
            yield True, {"success": True}


def mock_ssl_context(cafile):
    return {"name": cafile}


class TestLoader:
    def test_loading_export_csv(self, es_conf, es_indice):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        with tempfile.TemporaryDirectory() as path:
            loader_ = LoaderTest(es_conf, es_indice, backup=True, path_to_export_folder=path)
            loader_.load(df)

    def test_loading_no_export(self, es_conf, es_indice):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        loader_ = LoaderTest(es_conf, es_indice)
        loader_.load(df)

    def test_loading_export_named_file(self, es_conf, es_indice):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        with tempfile.TemporaryDirectory() as path:
            loader_ = LoaderTest(es_conf, es_indice, backup=True, path_to_export_folder=path, name_export="name")
            loader_.load(df)

    def test_bad_folder_crashes(self, es_conf, es_indice):
        with pytest.raises(ValueError):
            loader.Loader(es_conf, es_indice, path_to_export_folder="/this_folder_does_not_exist", backup=True)

    def test_no_folder_crashes(self, es_conf, es_indice):
        with pytest.raises(ValueError):
            loader.Loader(es_conf, es_indice, backup=True)

    def test_good_folder(self, es_conf, es_indice):
        loader.Loader(es_conf, es_indice, path_to_export_folder="/", backup=True)

    def test_no_backup(self, es_conf, es_indice):
        loader.Loader(es_conf, es_indice)

    def test_bulk_into_elastic(self, monkeypatch, es_conf, es_indice):
        monkeypatch.setattr(loader.elasticsearch.helpers, "streaming_bulk", mock_streaming_bulk_no_error)
        loader.Loader(es_conf, es_indice)._bulk_into_elastic([])

    def test_bulk_into_elastic_warns_on_error(self, monkeypatch, es_conf, es_indice):
        monkeypatch.setattr(loader.elasticsearch.helpers, "streaming_bulk",
                            mock_streaming_bulk_some_errors)
        with pytest.warns(UserWarning):
            loader.Loader(es_conf, es_indice)._bulk_into_elastic([])


class TestCSVWriter:
    def test_asserts_writes_csv(self):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        with tempfile.TemporaryDirectory() as path:
            loader_ = loader.CSVWriter()
            file_name = "tmpcsv.csv"
            loader_.load(df, path + f"/{file_name}")
            assert file_name in os.listdir(path)


class TestGetESInstance:
    def test_no_authentication(self, monkeypatch, es_instanciating_loader):
        def assert_es_instanciation_called_with(hosts, http_auth, use_ssl=False, scheme=None, port=8080,
                                                ssl_context=None):
            assert hosts == "localhost"
            assert http_auth is None
        monkeypatch.setattr(elasticsearch, "Elasticsearch", assert_es_instanciation_called_with)
        es_instanciating_loader._instantiate_es({})

    def test_authentication_no_cafile(self, monkeypatch, es_instanciating_loader):
        def assert_es_instanciation_called_with(hosts, http_auth, use_ssl=False, scheme=None, port=8080,
                                                ssl_context=None):
            assert hosts == "localhost"
            assert http_auth == ("user", "my_pwd")

        monkeypatch.setattr(elasticsearch, "Elasticsearch", assert_es_instanciation_called_with)
        es_instanciating_loader._instantiate_es({"user": "user", "pwd": "my_pwd"})

    def test_cafile(self, monkeypatch, es_instanciating_loader):
        def assert_es_instanciation_called_with(hosts, http_auth, use_ssl=False, scheme=None, port=8080,
                                                ssl_context=None):
            assert hosts == "1.12.4.2"
            assert http_auth == ("elastic", "changeme")
            assert use_ssl is True
            assert scheme == "https"
            assert port == 8080
            assert ssl_context == {"name": "fake_cafile_content"}

        monkeypatch.setattr(elasticsearch, "Elasticsearch", assert_es_instanciation_called_with)
        monkeypatch.setattr(ssl, "create_default_context", mock_ssl_context)
        es_instanciating_loader._instantiate_es({"user": "elastic", "pwd": "changeme", "host": "1.12.4.2", "scheme": "https", "port": 8080,
                         "cafile": "fake_cafile_content"})
