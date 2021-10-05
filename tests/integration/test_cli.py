import pytest
import pypel.processes
import os
from pypel.main import get_args, select_process_from_config, process_from_config


@pytest.fixture
def process_config(es_conf, es_indice):
    return {
        "Extractor": {
            "name": "pypel.extractors.Extractor"
        },
        "Transformers": [{
            "name": "pypel.transformers.Transformer"
        }],
        "Loader": {
            "name": "pypel.loaders.Loader",
            "es_conf": es_conf,
            "indice": es_indice
        }
    }


@pytest.fixture
def processes_config(es_conf):
    return [{
        "name": "EXAMPLE",
        "Extractor": {
            "name": "pypel.extractors.Extractor"
        },
        "Transformers": [
            {
                "name": "pypel.transformers.ColumnStripperTransformer"
            }
        ],
        "Loader": {
            "name": "pypel.loaders.Loader",
            "indice": "pypel_bulk",
            "es_conf": es_conf
        }
    },
        {
            "name": "ANOTHER_EXAMPLE",
            "Extractor": {
                "name": "pypel.extractors.Extractor"
            },
            "Transformers": [{
                "name": "pypel.transformers.Transformer"
            }],
            "Loader": {
                "name": "pypel.loaders.Loader",
                "indice": "pypel_bulk_2",
                "es_conf": es_conf
            }
        }]


class TestGetArgs:
    def test_raises_if_file_not_specified(self):
        with pytest.raises(SystemExit):
            get_args([])

    def test_process(self):
        key = "process"
        expected = "MyProcess"
        actual = get_args(["-p", "MyProcess", "-f", "/f"]).__getattribute__(key)
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
            "process": "all"}
        actual = get_args(["-f", "/home/user/data.csv"])
        for key in actual.__dict__:
            assert actual.__getattribute__(key) == expected[key]

    def test_all_params(self):
        expected = {
            "source_path": "/file",
            "config_file": "/home/user/pypel_conf.json",
            "process": "MYPROCESS"}
        actual = get_args(["-f", "/file", "-c", "/home/user/pypel_conf.json", "-p", "MYPROCESS"])
        for key in actual.__dict__:
            assert actual.__getattribute__(key) == expected[key]


class TestProcessFromConfig:
    def test_raises_if_non_existant_file(self, process_config):
        with pytest.raises(FileNotFoundError, match="Could not find file /i_do_not_exist"):
            process_from_config(process_config, "/i_do_not_exist")

    def test_single_file(self, monkeypatch, process_config):
        def process_assert_called_with(_, file_path):
            assert file_path == "./tests/fake_data/test_init_df.csv"

        monkeypatch.setattr(pypel.processes.Process, "process", process_assert_called_with)
        process_from_config(process_config, "./tests/fake_data/test_init_df.csv")

    def test_with_cl_indice(self, monkeypatch, process_config):
        def process_assert_called_with(_, file_path):  # _ is self here
            assert file_path == "./tests/fake_data/test_init_df.csv"

        monkeypatch.setattr(pypel.processes.Process, "process", process_assert_called_with)
        process_from_config(process_config, "./tests/fake_data/test_init_df.csv")

    def test_directory(self, monkeypatch, process_config):
        def bulk_assert_called_with(_, list_):  # _ is self here
            assert list_ == [os.path.join("./tests/fake_data/", file) for file in os.listdir("./tests/fake_data/")]

        monkeypatch.setattr(pypel.processes.Process, "bulk", bulk_assert_called_with)
        process_from_config(process_config, "./tests/fake_data/")


class TestSelectProcessFromConfig:
    def test_raises_if_processes_not_found(self):
        with pytest.raises(ValueError, match="key 'Processes' not found in the passed config, no idea what to do."):
            select_process_from_config(None, "", "")

    def test_raises_if_processes_not_a_list(self):
        with pytest.raises(ValueError,
                           match="Processes is not a list, please encapsulate Processes inside a list even if you only "
                                 "have a single Process"):
            select_process_from_config({}, "", "")

    def test_raises_if_process_not_in_processes(self, processes_config):
        with pytest.raises(ValueError, match="process DOESNOTEXIST not found in the configuration file !"):
            select_process_from_config(processes_config, "DOESNOTEXIST", "/file")

    def test_single_process(self, monkeypatch, processes_config):
        def process_from_conf_assert_called_with(proc, file):
            assert proc == processes_config[0]
            assert file == "/file"

        monkeypatch.setattr(pypel.main, "process_from_config", process_from_conf_assert_called_with)
        select_process_from_config(processes_config, "EXAMPLE", "/file")

    def test_multiple_processes(self, mocker, processes_config):
        mocker.patch('pypel.main.process_from_config')
        select_process_from_config(processes_config, "all", "/files")
        assert pypel.main.process_from_config.call_count == 2
        calls = [mocker.call(proc, "/files") for proc in processes_config]
        pypel.main.process_from_config.assert_has_calls(calls)
