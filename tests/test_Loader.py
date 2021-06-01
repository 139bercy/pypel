import tempfile
import os
import pytest
import pypel
from pandas.testing import assert_frame_equal
from pandas import read_csv, DataFrame


class LoaderTest(pypel.Loader):
    def __init__(self):
        super().__init__("", "") # noqa

    def load(self, dataframe):
        self.export_csv(df=dataframe)

    def export_csv(self, df, sep: str = '|'):
        with tempfile.TemporaryDirectory() as path:
            if not self.name_export_file:
                name_file = "exported_data_" + str(self.indice) + self._get_date() + ".csv"
            else:
                name_file = self.name_export_file
            path_to_csv = os.path.join(path, name_file)
            df.to_csv(path_to_csv, sep=sep, index=False, mode='a')
            assert os.path.exists(path_to_csv)
            assert_frame_equal(df, read_csv(path_to_csv, sep='|'))


class TestLoader:
    def test_loading_export_csv(self):
        df = DataFrame(data=[[6, 6, 6, 6, 6],
                             [7, 7, 7, 7, 7],
                             [8, 8, 8, 8, 8],
                             [9, 9, 9, 9, 9]],
                       columns=["0", "1", "2", "3", "4"])
        loader = LoaderTest()
        loader.load(df)

    def test_bad_folder_instanciation_crashes(self):
        with pytest.raises(ValueError):
            pypel.Loader("", None, path_to_export_folder="/this_folder_does_not_exist", backup=True)

    def test_no_folder_instanciation_crashes(self):
        with pytest.raises(ValueError):
            pypel.Loader("", None, backup=True)

    def test_good_folder_instanciation(self):
        pypel.Loader("", None, path_to_export_folder="/", backup=True)

    def test_no_backup_instanciation(self):
        pypel.Loader("", None)
