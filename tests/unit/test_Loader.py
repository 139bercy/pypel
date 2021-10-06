import datetime
import logging
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

    def test_bulk_into_elastic_warns_on_error(self, monkeypatch, es_conf, es_indice, caplog):
        monkeypatch.setattr(loader.elasticsearch.helpers, "streaming_bulk",
                            mock_streaming_bulk_some_errors)
        with caplog.at_level(logging.DEBUG, logger="pypel.loaders.Loaders"):
            loader.Loader(es_conf, es_indice)._bulk_into_elastic([])
            assert ("pypel.loaders.Loaders", logging.WARNING, "3 errors detected") in caplog.record_tuples
            assert ("pypel.loaders.Loaders", logging.DEBUG,
                    "Error details : [{'error': {'fake_reason': 'fake_error'}}, "
                    "{'error': {'fake_reason': 'fake_error'}}, {'error': {'fake_reason': 'fake_error'}}]")\
                   in caplog.record_tuples

    def test_change_time_freq(self, es_conf, es_indice, df, monkeypatch):
        def assert_bulk_called_with(_, action):  # _ is placeholder for self
            y = datetime.datetime.now().strftime("_%Y")
            assert action == [{"_index": "test_indice" + y,
                               "_source": {"0": 0}},
                              {"_index": "test_indice" + y,
                               "_source": {"0": 1}},
                              {"_index": "test_indice" + y,
                               "_source": {"0": 2}}]

        loader_ = loader.Loader(es_conf, es_indice, time_freq="_%Y")
        monkeypatch.setattr(loader.Loader, "_bulk_into_elastic", assert_bulk_called_with)
        loader_.load(df)

    def test_default_time_freq(self, es_conf, es_indice, df, monkeypatch):
        def assert_bulk_called_with(_, action):  # _ is placeholder for self
            y = datetime.datetime.now().strftime("_%m_%Y")
            assert action == [{"_index": "test_indice" + y,
                               "_source": {"0": 0}},
                              {"_index": "test_indice" + y,
                               "_source": {"0": 1}},
                              {"_index": "test_indice" + y,
                               "_source": {"0": 2}}]

        loader_ = loader.Loader(es_conf, es_indice)
        monkeypatch.setattr(loader.Loader, "_bulk_into_elastic", assert_bulk_called_with)
        loader_.load(df)

    def test_es_indices_exists(self, es_conf, es_indice, df, monkeypatch):
        def mock_indices_exists(indice):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            return False

        loader_ = LoaderTest(es_conf, es_indice, overwrite=True)
        monkeypatch.setattr(loader_.es.indices, "exists", mock_indices_exists)
        loader_.load(df)

    def test_es_cat_count(self, es_conf, es_indice, df, monkeypatch):
        def mock_indices_exists(indice):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            return True

        def mock_cat_count(index, h):
            assert index == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            assert h == "count"
            return "00"

        loader_ = LoaderTest(es_conf, es_indice, overwrite=True)
        monkeypatch.setattr(loader_.es.indices, "exists", mock_indices_exists)
        monkeypatch.setattr(loader_.es.cat, "count", mock_cat_count)
        loader_.load(df)

    def test_recreate_indice(self, es_conf, es_indice, monkeypatch, df):
        def mock_indices_exists(indice):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            return True

        def mock_cat_count(index, h):
            assert index == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            assert h == "count"
            return "11"

        def mock_indices_get(indice):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            return {indice: {"mappings": {"key1": "val1"}, "key2": "val2"}}

        def mock_indices_delete(indice):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")

        def mock_indice_create(indice, body):
            assert indice == es_indice + datetime.datetime.now().strftime("_%m_%Y")
            assert body == {"mappings": {"key1": "val1"}}

        loader_ = LoaderTest(es_conf, es_indice, overwrite=True)
        monkeypatch.setattr(loader_.es.indices, "exists", mock_indices_exists)
        monkeypatch.setattr(loader_.es.cat, "count", mock_cat_count)
        monkeypatch.setattr(loader_.es.indices, "get", mock_indices_get)
        monkeypatch.setattr(loader_.es.indices, "delete", mock_indices_delete)
        monkeypatch.setattr(loader_.es.indices, "create", mock_indice_create)
        loader_.load(df)


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
