import tempfile
import pytest
import pypel
from pandas import DataFrame


class Elasticsearch:
    pass


class LoaderTest(pypel.Loader):
    def _bulk_into_elastic(self, actions: list, indice: str):
        pass


class TestLoader:
    def test_loading_export_csv(self):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        with tempfile.TemporaryDirectory() as path:
            loader = LoaderTest(Elasticsearch(), backup=True, path_to_export_folder=path)
            loader.load(df, "indice")

    def test_loading_no_export(self):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        loader = LoaderTest(Elasticsearch())
        loader.load(df, "indice")

    def test_loading_export_named_file(self):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        with tempfile.TemporaryDirectory() as path:
            loader = LoaderTest(Elasticsearch(), backup=True, path_to_export_folder=path, name_export="name")
            loader.load(df, "indice")

    def test_bad_folder_crashes(self):
        with pytest.raises(ValueError):
            pypel.Loader(Elasticsearch(), path_to_export_folder="/this_folder_does_not_exist", backup=True)

    def test_no_folder_crashes(self):
        with pytest.raises(ValueError):
            pypel.Loader(Elasticsearch(), backup=True)

    def test_good_folder(self):
        pypel.Loader(Elasticsearch(), path_to_export_folder="/", backup=True)

    def test_no_backup(self):
        pypel.Loader(Elasticsearch())
