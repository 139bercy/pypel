import pytest
from pypel.transformers import Transformer
from pypel.extractors import Extractor
import os
from pandas import DataFrame
from pandas.testing import assert_frame_equal


@pytest.fixture
def transformer():
    return Transformer()


class RefExtractor(Extractor):
    def extract(self, *args, **kwargs) -> DataFrame:
        return DataFrame({"0": [0, 1, 2]})


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
