import pytest
import pypel
from pypel import Transformer
import os
from pandas import DataFrame


@pytest.fixture
def transformer():
    return Transformer()


class RefExtractor(pypel.Extractor):
    def init_dataframe(self, *args, **kwargs):
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
            transformer.merge_referential(df, None, None, extractor=pypel.Extractor)

    def test_merge_ref_crashes_if_ref_is_not_path_nor_str(self, df, transformer):
        with pytest.raises(ValueError):
            transformer.merge_referential(df, 0, "0")

    def test_merge_ref_with_default_extractor(self, transformer):
        df = pypel.Extractor().init_dataframe(os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv"))
        transformer.merge_referential(df,
                                      os.path.join(os.getcwd(), "tests", "fake_data", "test_init_df.csv"))
