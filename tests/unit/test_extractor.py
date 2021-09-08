import pytest
from pypel.extractors import Extractor, CSVExtractor, XLSExtractor, XLSXExtractor
import pandas as pd
from pandas.testing import assert_frame_equal
import os


@pytest.fixture
def ex():
    return Extractor()


@pytest.fixture
def csv():
    return CSVExtractor()


@pytest.fixture
def xls():
    return XLSExtractor()


@pytest.fixture
def xlsx():
    return XLSXExtractor()


class TestExtractor:
    def test_raises_if_unsupported_file_extension(self, ex):
        with pytest.raises(ValueError):
            ex.extract("./setup.py")

    def test_extract_excel(self, ex):
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
        df = ex.extract(path)
        assert_frame_equal(expected_default, df)

    def test_extract_excel_no_logging(self, ex, disable_logs):
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
        df = ex.extract(path)
        assert_frame_equal(expected_default, df)

    def test_extract_excel_skiprows(self, ex):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_skip_5 = pd.DataFrame(data=[[6, 6, 6, 6, 6],
                                             [7, 7, 7, 7, 7],
                                             [8, 8, 8, 8, 8],
                                             [9, 9, 9, 9, 9]],
                                       columns=[0, 1, 2, 3, 4])
        df = ex.extract(path, skiprows=5, header=None)
        assert_frame_equal(expected_skip_5, df)

    def test_extract_excel_sheetname(self, ex):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_sheetname = pd.DataFrame(data=[[9]], columns=["a"])
        obtained_sheetname = ex.extract(path, sheet_name="TEST")
        assert_frame_equal(expected_sheetname, obtained_sheetname)

    def test_extract_excel_csv(self, ex):
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
        df = ex.extract(path_csv)
        assert_frame_equal(expected_csv, df)

    def test_extract_excel_csv_no_logging(self, ex, disable_logs):
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
        df = ex.extract(path_csv)
        assert_frame_equal(expected_csv, df)

    def test_extract_xls(self, ex):
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
        df = ex.extract(path_xls)
        assert_frame_equal(expected_xls, df)

    def test_extract_skiprows_xls(self, ex):
        path_xls = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xls")
        expected_xls = pd.DataFrame(data=[[5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=[0, 1, 2, 3, 4])
        df = ex.extract(path_xls, skiprows=4, header=None)
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
        df = ex.extract(path_xls)
        assert_frame_equal(expected_xls, df)


class TestCSVExtractor:
    def test_extract_csv(self, csv):
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
        df = csv.extract(path_csv)
        assert_frame_equal(expected_csv, df)

    def test_extract_csv_no_logging(self, csv, disable_logs):
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
        df = csv.extract(path_csv)
        assert_frame_equal(expected_csv, df)


class TestXLSXExtractor:
    def test_extract_xlsx(self, xlsx):
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
        df = xlsx.extract(path)
        assert_frame_equal(expected_default, df)

    def test_extract_xlsx_no_logging(self, xlsx, disable_logs):
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
        df = xlsx.extract(path)
        assert_frame_equal(expected_default, df)

    def test_extract_xlsx_skiprows(self, xlsx):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_skip_5 = pd.DataFrame(data=[[6, 6, 6, 6, 6],
                                             [7, 7, 7, 7, 7],
                                             [8, 8, 8, 8, 8],
                                             [9, 9, 9, 9, 9]],
                                       columns=[0, 1, 2, 3, 4])
        df = xlsx.extract(path, skiprows=5, header=None)
        assert_frame_equal(expected_skip_5, df)

    def test_extract_xlsx_sheetname(self, xlsx):
        path = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xlsx")
        expected_sheetname = pd.DataFrame(data=[[9]], columns=["a"])
        obtained_sheetname = xlsx.extract(path, sheet_name="TEST")
        assert_frame_equal(expected_sheetname, obtained_sheetname)

    def test_raises_if_badly_named_file(self, xlsx):
        with pytest.warns(UserWarning):
            xlsx.extract(os.path.join(os.getcwd(), "tests", "fake_data", "test_bad_filename$.xlsx"))


class TestXLSExtractor:
    def test_extract_xls(self, xls):
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
        df = xls.extract(path_xls)
        assert_frame_equal(expected_xls, df)

    def test_extract_skiprows_xls(self, xls):
        path_xls = os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.xls")
        expected_xls = pd.DataFrame(data=[[5, 5, 5, 5, 5],
                                          [6, 6, 6, 6, 6],
                                          [7, 7, 7, 7, 7],
                                          [8, 8, 8, 8, 8],
                                          [9, 9, 9, 9, 9]], columns=[0, 1, 2, 3, 4])
        df = xls.extract(path_xls, skiprows=4, header=None)
        assert_frame_equal(expected_xls, df)

    def test_init_datafram_xls_no_logging(self, xls, disable_logs):
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
        df = xls.extract(path_xls)
        assert_frame_equal(expected_xls, df)
