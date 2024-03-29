import pytest
from pypel.transformers import (Transformer, ColumnStripperTransformer, ColumnReplacerTransformer,
                                ContentReplacerTransformer, ColumnCapitaliserTransformer,
                                ColumnContenStripperTransformer, MergerTransformer,
                                NullValuesReplacerTransformer, DateParserTransformer, DateFormatterTransformer)
from pypel.extractors import Extractor
import os
from pandas import DataFrame, NA, NaT, to_datetime
from numpy import nan
from pandas.testing import assert_frame_equal


@pytest.fixture
def transformer():
    return Transformer()


@pytest.fixture
def merger():
    return MergerTransformer()


class RefExtractor(Extractor):
    def extract(self, *args, **kwargs) -> DataFrame:
        return DataFrame({
            "0": [0, 1, 2]})


class TestTransformer:
    def test_format_str_columns_warns_if_column_missing(self, df):
        transformer = Transformer(strip=["1"])
        with pytest.warns(UserWarning):
            transformer._format_str_columns(df)

    def test_format_dates_warns_if_date_format_omitted(self, df):
        transformer = Transformer(date_columns=[])
        with pytest.warns(UserWarning):
            transformer._format_dates(df)

    def test_format_dates_warns_if_columns_omitted(self, df):
        transformer = Transformer(date_format="")
        with pytest.warns(UserWarning):
            transformer._format_dates(df)

    def test_merge_referential_passing_dataframe(self, df, transformer):
        transformer.merge_referential(df, mergekey="0", referential=df)

    def test_merge_ref_passing_instanced_extractor(self, df, transformer):
        transformer.merge_referential(df, None, "0", extractor=RefExtractor())

    def test_merge_ref_crashes_if_extractor_param_not_a_valid_extractor_instance(self, df, transformer):
        with pytest.raises(AssertionError):
            transformer.merge_referential(df, None, None, extractor=1)
        with pytest.raises(AssertionError):
            transformer.merge_referential(df, None, None, extractor=Extractor)

    def test_merge_ref_crashes_if_ref_is_not_path_nor_str(self, df, transformer):
        with pytest.raises(ValueError):
            transformer.merge_referential(df, 0, "0")

    def test_merge_ref_with_default_extractor(self, transformer):
        df = Extractor().extract(os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv"))
        expected = df.copy()
        actual = transformer.merge_referential(df, os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv"))
        assert_frame_equal(expected, actual)


class TestAtomicTransformers:
    def test_column_stripper(self):
        tr = ColumnStripperTransformer()
        expected = DataFrame(data=[[0]], columns=["bad_column_name"])
        actual = tr.transform(DataFrame(data=[[0]], columns=[" bad_column_name "]))
        assert_frame_equal(expected, actual)

    def test_column_replacer(self):
        tr = ColumnReplacerTransformer()
        expected = DataFrame(data=[[0]], columns=["replaced"])
        actual = tr.transform(DataFrame(data=[[0]], columns=["original"]), column_replace_dict={
            "original": "replaced"})
        assert_frame_equal(expected, actual)

    def test_content_replacer(self):
        tr = ContentReplacerTransformer()
        expected = DataFrame(data=[["replaced"]], columns=[0])
        actual = tr.transform(DataFrame(data=[["original"]], columns=[0]), replace_dict={
            "original": "replaced"})
        assert_frame_equal(expected, actual)

    def test_column_capitaliser(self):
        tr = ColumnCapitaliserTransformer()
        expected = DataFrame(data=[[0]], columns=["Capitalized"])
        actual = tr.transform(DataFrame(data=[[0]], columns=["capitalized"]))
        assert_frame_equal(expected, actual)

    def test_null_values_replacer(self):
        tr = NullValuesReplacerTransformer()
        expected = DataFrame(data=[[None], [None], [None]], columns=[0])
        actual = tr.transform(DataFrame(data=[[NA], [NaT], [nan]], columns=[0]))
        assert_frame_equal(expected, actual)

    def test_date_parser(self):
        tr = DateParserTransformer()
        expected = DataFrame(data=[[to_datetime("13-01-1970")]], columns=["to_parse"])
        actual = tr.transform(DataFrame(data=[["13 01 1970"]], columns=["to_parse"]), ["to_parse"], "%d %m %Y")
        assert_frame_equal(expected, actual)

    def test_date_formatter(self):
        tr = DateFormatterTransformer()
        expected = DataFrame(data=[["1970-01-22"]], columns=["to_format"])
        actual = tr.transform(DataFrame(data=[[to_datetime("22 01 1970")]], columns=["to_format"]), ["to_format"])
        assert_frame_equal(expected, actual)

    def test_date_formatter_raises_if_non_datetimes_in_column(self):
        with pytest.raises(ValueError, match="Column to_format has non-datetime values, the columns to format must "
                                             "be datetimes. Please parse beforehands."):
            tr = DateFormatterTransformer()
            tr.transform(DataFrame(data=[["22 01 1970"]], columns=["to_format"]), ["to_format"])


class TestColumnContentStripper:
    def test_column_content_stripping(self):
        tr = ColumnContenStripperTransformer()
        expected = DataFrame(data=[["stripped"]], columns=["to_strip"])
        actual = tr.transform(DataFrame(data=[[" stripped "]], columns=["to_strip"]), columns_to_strip=["to_strip"])
        assert_frame_equal(expected, actual)

    def test_warns_if_column_not_in_df(self):
        with pytest.warns(UserWarning, match="No such column not_in_df in passed dataframe"):
            tr = ColumnContenStripperTransformer()
            tr.transform(DataFrame(data=[[" stripped "]], columns=["to_strip"]), columns_to_strip=["not_in_df"])

    def test_warns_if_column_not_strings(self):
        with pytest.warns(UserWarning, match="Column not_str is not of type `str`, cannot strip."):
            tr = ColumnContenStripperTransformer()
            tr.transform(DataFrame(data=[[0]], columns=["not_str"]), columns_to_strip=["not_str"])


class TestMerger:
    def test_merge_with_self(self, df, merger):
        expected = df.copy()
        actual = merger.transform(df, mergekey="0", ref=df)
        assert_frame_equal(expected, actual)
