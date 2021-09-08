import elasticsearch
import pytest
import pypel.processes
import ssl
from pypel.main import get_args, select_process_from_config, process_from_config, get_es_instance


@pytest.fixture
def process_config():
    return {
        "Extractors": {
            "name": "pypel.extractors.Extractor"
        },
        "Transformers": [{
            "name": "pypel.transformers.Transformer"
        }],
        "Loaders": {
            "name": "pypel.loaders.Loader",
        },
        "indice": "my_indice"
    }


@pytest.fixture
def processes_config():
    return [{
                "name": "EXAMPLE",
                "Extractors": {
                    "name": "pypel.extractors.Extractor"
                },
                "Transformers": [
                    {
                        "name": "pypel.transformers.ColumnStripperTransformer"
                    }
                ],
                "Loaders": {
                    "name": "pypel.loaders.Loader"
                },
                "indice": "pypel_bulk"
            },
            {
                "name": "ANOTHER_EXAMPLE",
                "Extractors": {
                    "name": "pypel.extractors.Extractor"
                },
                "Transformers": [{
                    "name": "pypel.transformers.Transformer"
                }],
                "Loaders": {
                    "name": "pypel.loaders.Loader"
                },
                "indice": "pypel_bulk_2"
            }]


def mock_ssl_context(cafile):
    return {"name": cafile}


class TestGetArgs:
    def test_raises_if_file_not_specified(self):
        with pytest.raises(SystemExit):
            get_args([])

    def test_process(self):
        key = "process"
        expected = "MyProcess"
        actual = get_args(["-p", "MyProcess", "-f", "/f"]).__getattribute__(key)
        assert expected == actual

    def test_indice(self):
        key = "indice"
        expected = "my_indice"
        actual = get_args(["-i", "my_indice", "-f", "/f"]).__getattribute__(key)
        assert expected == actual

    def test_source_path(self):
        key = "source_path"
        expected = "/home/user/data.csv"
        actual = get_args(["-f", "/home/user/data.csv"]).__getattribute__(key)
        assert expected == actual

    def test_config_file(self):
        key = "config_file"
        expected = "/home/user/pypel/config_template.json"
        actual = get_args(["-c", "/home/user/pypel/config_template.json", "-f", "/f"]).__getattribute__(key)
        assert expected == actual

    def test_defaults(self):
        expected = {
            "source_path": "/home/user/data.csv",
            "config_file": "./conf/config.json",
            "process": "all",
            "indice": None}
        actual = get_args(["-f", "/home/user/data.csv"])
        for key in actual.__dict__:
            assert actual.__getattribute__(key) == expected[key]

    def test_all_params(self):
        expected = {
            "source_path": "/file",
            "config_file": "/home/user/pypel_conf.json",
            "process": "MYPROCESS",
            "indice": "indice"}
        actual = get_args(["-f", "/file", "-i", "indice", "-c", "/home/user/pypel_conf.json", "-p", "MYPROCESS"])
        for key in actual.__dict__:
            assert actual.__getattribute__(key) == expected[key]


class TestGetESInstance:
    def test_no_authentication(self, monkeypatch):
        def assert_es_instanciation_called_with(hosts, http_auth, use_ssl=False, scheme=None, port=8080,
                                                ssl_context=None):
            assert hosts == "localhost"
            assert http_auth is None
        monkeypatch.setattr(elasticsearch, "Elasticsearch", assert_es_instanciation_called_with)
        get_es_instance({})

    def test_authentication_no_cafile(self, monkeypatch):
        def assert_es_instanciation_called_with(hosts, http_auth, use_ssl=False, scheme=None, port=8080,
                                                ssl_context=None):
            assert hosts == "localhost"
            assert http_auth == ("user", "my_pwd")

        monkeypatch.setattr(elasticsearch, "Elasticsearch", assert_es_instanciation_called_with)
        get_es_instance({"user": "user", "pwd": "my_pwd"})

    def test_cafile(self, monkeypatch):
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
        get_es_instance({"user": "elastic", "pwd": "changeme", "host": "1.12.4.2", "scheme": "https", "port": 8080,
                         "cafile": "fake_cafile_content"})


class TestProcessFromConfig:
    def test_raises_if_non_existant_file(self, process_config, es_instance):
        with pytest.raises(FileNotFoundError, match="Could not find file /i_do_not_exist"):
            process_from_config(es_instance, process_config, "/i_do_not_exist", None)

    def test_single_file(self, monkeypatch, process_config, es_instance):
        def process_assert_called_with(_, file_path, indice, instance):
            assert file_path == "./tests/fake_data/test_init_df.csv"
            assert indice == "my_indice"
            assert instance is es_instance

        monkeypatch.setattr(pypel.processes.Process, "process", process_assert_called_with)
        process_from_config(es_instance, process_config, "./tests/fake_data/test_init_df.csv", None)

    def test_with_cl_indice(self, monkeypatch, process_config, es_instance):
        def process_assert_called_with(_, file_path, indice, instance):
            assert file_path == "./tests/fake_data/test_init_df.csv"
            assert indice == "indice"
            assert instance is es_instance

        monkeypatch.setattr(pypel.processes.Process, "process", process_assert_called_with)
        process_from_config(es_instance, process_config, "./tests/fake_data/test_init_df.csv", "indice")

    def test_directory(self, monkeypatch, process_config, es_instance):
        def bulk_assert_called_with(_, dic):
            assert dic == {
                "my_indice": ['./tests/fake_data/test_bad_filename$.csv',
                              './tests/fake_data/test_bad_filename$.xlsx',
                              './tests/fake_data/test_init_df.csv', './tests/fake_data/test_init_df.xls',
                              './tests/fake_data/test_init_df.xlsx']}

        monkeypatch.setattr(pypel.processes.Process, "bulk", bulk_assert_called_with)
        process_from_config(es_instance, process_config, "./tests/fake_data/", None)


class TestSelectProcessFromConfig:
    def test_raises_if_processes_not_found(self, es_instance):
        with pytest.raises(ValueError, match="key 'Processes' not found in the passed config, no idea what to do."):
            select_process_from_config(es_instance, None, None, None, None)

    def test_raises_if_processes_not_a_list(self, es_instance):
        with pytest.raises(ValueError,
                           match="Processes is not a list, please encapsulate Processes inside a list even if you only "
                                 "have a single Process"):
            select_process_from_config(es_instance, {}, None, None, None)

    def test_raises_if_process_not_in_processes(self, es_instance, processes_config):
        with pytest.raises(ValueError, match="process DOESNOTEXIST not found in the configuration file !"):
            select_process_from_config(es_instance, processes_config, "DOESNOTEXIST", "/file", None)

    def test_single_process(self, es_instance, monkeypatch, processes_config):
        def process_from_conf_assert_called_with(es, proc, file, indice):
            assert es == es_instance
            assert proc == processes_config[0]
            assert file == "/file"
            assert indice is None

        monkeypatch.setattr(pypel.main, "process_from_config", process_from_conf_assert_called_with)
        select_process_from_config(es_instance, processes_config, "EXAMPLE", "/file", None)

    def test_multiple_processes(self, es_instance, mocker, processes_config):
        mocker.patch('pypel.main.process_from_config')
        select_process_from_config(es_instance, processes_config, "all", "/file", None)
        assert pypel.main.process_from_config.call_count == 2
        #  TODO: fix the assert_has_calls logic
        # calls = [call(es_instance, proc, "/files", None) for proc in processes_config]
        # print("mock_calls", pypel.main.process_from_config.mock_calls)
        # pypel.main.process_from_config.assert_has_calls(calls)
