import pytest
from pypel import Extractor
import pandas as pd
from pandas.testing import assert_frame_equal
import os


@pytest.fixture
def ex():
    return Extractor()


class TestExtractor:
    def test_raises_if_unsupported_file_extension(self, ex):
        with pytest.raises(ValueError):
            ex.init_dataframe("./setup.py")

    def test_init_dataframe_excel(self, ex):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_default = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                              [2, 2, 2, 2, 2],
                                              [3, 3, 3, 3, 3],
                                              [4, 4, 4, 4, 4],
                                              [5, 5, 5, 5, 5],
                                              [6, 6, 6, 6, 6],
                                              [7, 7, 7, 7, 7],
                                              [8, 8, 8, 8, 8],
                                              [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path)
        assert_frame_equal(expected_default, df)

    def test_init_dataframe_excel_no_logging(self, ex, disable_logs):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_default = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                              [2, 2, 2, 2, 2],
                                              [3, 3, 3, 3, 3],
                                              [4, 4, 4, 4, 4],
                                              [5, 5, 5, 5, 5],
                                              [6, 6, 6, 6, 6],
                                              [7, 7, 7, 7, 7],
                                              [8, 8, 8, 8, 8],
                                              [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path)
        assert_frame_equal(expected_default, df)

    def test_init_dataframe_excel_skiprows(self, ex):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_skip_5 = pd.DataFrame(data=[[6, 6, 6, 6, 6],
                                             [7, 7, 7, 7, 7],
                                             [8, 8, 8, 8, 8],
                                             [9, 9, 9, 9, 9]],
                                       columns=[0, 1, 2, 3, 4])
        df = ex.init_dataframe(path, skiprows=5, header=None)
        assert_frame_equal(expected_skip_5, df)

    def test_init_dataframe_excel_sheetname(self, ex):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_sheetname = pd.DataFrame(data=[[9]], columns=["a"])
        obtained_sheetname = ex.init_dataframe(path, sheet_name="TEST")
        assert_frame_equal(expected_sheetname, obtained_sheetname)

    def test_init_dataframe_excel_csv(self, ex):
        path_csv = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv")
        expected_csv = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path_csv)
        assert_frame_equal(expected_csv, df)

    def test_init_dataframe_excel_csv_no_logging(self, ex, disable_logs):
        path_csv = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv")
        expected_csv = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path_csv)
        assert_frame_equal(expected_csv, df)

    def test_init_datafram_xls(self, ex):
        path_xls = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xls")
        expected_xls = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path_xls)
        assert_frame_equal(expected_xls, df)

    def test_init_dataframe_skiprows_xls(self, ex):
        path_xls = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xls")
        expected_xls = pd.DataFrame(data=[[5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=[0, 1, 2, 3, 4])
        df = ex.init_dataframe(path_xls, skiprows=4, header=None)
        assert_frame_equal(expected_xls, df)

    def test_init_datafram_xls_no_logging(self, ex, disable_logs):
        path_xls = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xls")
        expected_xls = pd.DataFrame(data=[[1, 1, 1, 1, 1],
                                          [2, 2, 2, 2, 2],
                                          [3, 3, 3, 3, 3],
                                          [4, 4, 4, 4, 4],
                                          [5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=["a", "b", "c", "d", "e"])
        df = ex.init_dataframe(path_xls)
        assert_frame_equal(expected_xls, df)
