import os
from pypel.extractors.Extractors import Extractor
import warnings
from typing import List, Dict, Optional, Any, Union
from pandas import DataFrame, to_datetime, NA
import abc


class BaseTransformer:
    """Dummy class that all Transformers must inherit from."""

    @abc.abstractmethod
    def transform(self, *args, **kwargs) -> Any:
        """This method must be implemented"""


class BaseParser(BaseTransformer):
    @abc.abstractmethod
    def __init__(self, coerce=True):
        """Must be overridden and declare a `self.parsed` attribute"""
        self.coerce = coerce
        self.parsed = ""

    def _coerce(self, value):
        if self.coerce:
            return None
        else:
            raise ValueError(f"La valeur {value} n'est pas {self.parsed}valide")

    @abc.abstractmethod
    def transform(self, dataframe, columns_to_parse) -> DataFrame:
        """This method must be impemented"""


class Transformer(BaseTransformer):
    """
    Encapsulates all the data-transformation related logic. Massively relies on pandas.

    :param strip: list
        list of columns whose name should be stripped off
    :param column_replace: dict
        dictionnary of format {"old": "new"} where old is a regex matching the string to replace and new its replacement
        This is used to format column names.
    :param df_replace: dict
        dictionnary of format {"old": "new"} where old is a regex matching the string to replace and new its replacement
        This is use to replace the dataframe's contents.
    :param date_format:
        the format in which datetimes are to be. cf [this website](https://strftime.org/)
        or [the docs](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)
    :param date_columns:
        list of columns that are to be parsed as dates
    """

    def __init__(self,
                 strip: Optional[List[str]] = None,
                 column_replace: Optional[Dict[str, str]] = None,
                 df_replace: Optional[Dict[Any, Any]] = None,
                 date_format: Optional[str] = None,
                 date_columns: Optional[List[str]] = None):
        self.column_replace = {} if not column_replace else column_replace
        self.df_replace = {} if not df_replace else df_replace
        self.columns_to_strip = [] if not strip else strip
        self.date_format = date_format
        self.date_columns = date_columns

    def transform(self,
                  dataframe: DataFrame) -> DataFrame:
        """
        Transforms the passed dataframe.

        :param dataframe: pd.Dataframe
            the dataframe to transform
        :return:
        """
        df = dataframe.copy()
        self._format_str_columns(df)
        self._format_contents(df)
        self._format_dates(df)
        df = self._format_na(df)
        return df

    def _format_str_columns(self, df: DataFrame) -> DataFrame:
        """
        returns df with normalized column names, replacing using self.column_replace & applying str.upper()

        :param df: the dataframe to be normalized
        :return: the dataframe with normalized columns
        """
        df.columns = df.columns.astype(str).str.strip()
        df.columns = df.columns.to_series().replace(self.column_replace, regex=True).apply(str.upper)
        for column in self.columns_to_strip:
            try:
                df[column] = df[column].str.strip()
            except KeyError:
                warnings.warn(f"NO SUCH COLUMN {column} IN DATAFRAME PASSED")
        return df

    def _format_contents(self, df: DataFrame) -> DataFrame:
        """
        returns df with normalized contents, replacing every value using self.df_replace
        self.df_replace should be in the following format : {"value_to_replace": "replacement_value"}

        :param df: the dataframe to normalize
        :return: the normalized dataframe
        """
        df.replace(self.df_replace, regex=True, inplace=True)
        return df

    def _format_na(self, df: DataFrame) -> DataFrame:
        """
        returns df with replaced NaNs & NaTs in the passed dataframe by None

        :param df: the dataframe that is to be treated
        :return: the modified dataframe, with Nones instead of NaNs & NaTs
        """
        df = df.applymap(lambda x: None if x != x else x)
        return df

    def _format_dates(self, df: DataFrame, date_format: Optional[str] = None,
                      date_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Formats datetimes in columns date_columns from dataframe df according to format date_format, or self.date_format

        :param df: the dataframe to format
        :param date_format: the dateformat to use, let to None if you wish to use self.date_format
        :param date_columns: the list of column names containing datetimes
        :return: the modified dataframe with datetimes formatted
        """
        date_format = date_format if date_format else self.date_format
        cols = date_columns if date_columns else self.date_columns
        if cols is not None and date_format is not None:
            for col in cols:
                df[col] = df[col].dt.strftime(date_format)
        elif date_format is None and cols is not None:
            warnings.warn("Incorrect usage : date_format not specified as argument nor in Transformer's constructor")
        elif cols is None and date_format is not None:
            warnings.warn("Incorrect usage : date_columns not specified as argument nor in Transformer's constructor")
        else:
            warnings.warn("No date columns and no date format, assuming there is nothing to do.")
        return df

    def merge_referential(self,
                          df: DataFrame,
                          referential: Union[str, bytes, os.PathLike, DataFrame],
                          mergekey: Optional[Union[str, List[str]]] = None,
                          how: str = "inner",
                          extractor: Optional[Extractor] = None,
                          **kwargs) -> DataFrame:
        """
        Enrich passed dataframe by merging it with a referential, either passed as dataframe
            or by a path to extract from, and then return it. Additional keyword parameters are passed to the Extractor.

        :param df: the dataframe to enrich
        :param mergekey: the mergekeys the merge will be executed upon (pandas merge's on parameter)
        :param referential: the referential to merge with. Either a `Dataframe` a string, or `PathLike`
        :param extractor: the extractor to use for extracting the referential
        :param how: the mergetype e.g. `inner`, `outer` etc... equivalent to pandas.merge's `how` parameter.
        :return: pd.Dataframe: the enriched dataframe
        """
        if isinstance(referential, DataFrame):
            return df.merge(referential, how=how, on=mergekey)
        elif extractor is not None:
            assert isinstance(extractor, Extractor)
            return df.merge(extractor.extract(referential, **kwargs),
                            how=how,
                            on=mergekey)
        else:
            try:
                assert isinstance(referential, str) or isinstance(referential, os.PathLike)
            except AssertionError as e:
                raise ValueError("Pass a string or an os.PathLike object pointing to the referential !") from e
            return df.merge(Extractor()
                            .extract(referential, **kwargs),
                            how=how,
                            on=mergekey)


class ColumnStripperTransformer(BaseTransformer):
    """Strips column names, removing trailing and leading whitespaces."""

    def transform(self, df: DataFrame) -> DataFrame:
        return df.rename(columns=str.strip)


class ColumnReplacerTransformer(BaseTransformer):
    """Allows replacing column names."""

    def transform(self, df: DataFrame, column_replace_dict: Dict[str, str]) -> DataFrame:
        return df.rename(columns=column_replace_dict)


class ColumnCapitaliserTransformer(BaseTransformer):
    """Capitalizes column names."""

    def transform(self, df: DataFrame) -> DataFrame:
        return df.rename(columns=str.capitalize)


class ColumnContenStripperTransformer(BaseTransformer):
    """Strips/trims the contents of a column. Said column(s) must contain only str values."""

    def transform(self, df: DataFrame, columns_to_strip: List[str]) -> DataFrame:
        df_ = df.copy()
        for column in columns_to_strip:
            try:
                df_[column] = df_[column].str.strip()
            except KeyError:
                warnings.warn(f"No such column {column} in passed dataframe")
            except AttributeError:
                warnings.warn(f"Column {column} is not of type `str`, cannot strip.")
        return df_


class ContentReplacerTransformer(BaseTransformer):
    """Allows replacing contents of a column"""

    def transform(self, df: DataFrame, replace_dict: Dict[Any, Any]) -> DataFrame:
        return df.replace(replace_dict, regex=True)


class NullValuesReplacerTransformer(BaseTransformer):
    """Replaces NaNs, NaTs or similar values by None, because None is understood by elasticsearc where nans arent."""

    def transform(self, df: DataFrame) -> DataFrame:
        """
        We use x != x because we want to check for both NaN & NaT at the same time, also any value that do pass this
        check should probably be taken care of anyway.
        """
        return df.applymap(lambda x: None if x is NA or x != x else x)


class DateFormatterTransformer(BaseTransformer):
    """Allows changing the date format of specific datetime columns"""

    def transform(self, df: DataFrame, date_columns: List[str], date_format="%Y-%m-%d") -> DataFrame:
        """
        :param df: the dataframe to modify
        :param date_columns: the columns containing the datetime values to modify
        :param date_format: the desired dateformat. Defaults to yyyy-MM-dd
        :return: the modified dataframe
        """
        df_ = df.copy()
        for col in date_columns:
            try:
                df_[col] = df_[col].dt.strftime(date_format)
            except AttributeError as e:
                raise ValueError(f"Column {col} has non-datetime values, the columns to format must be datetimes. "
                                 f"Please parse beforehands.") from e
        return df_


class DateParserTransformer(BaseTransformer):
    """Converts passed columns' values to datetime objects. Date parsing is prefered at extraction."""

    def transform(self, df: DataFrame, date_columns: List[str], date_format: str = "%Y-%m-%d") -> DataFrame:
        """

        :param df: the dataframe containing the columns to parse
        :param date_columns: list of names of columns to parse
        :param date_format: the strftime format to parse from
        :return:
        """
        df_ = df.copy()
        for col in date_columns:
            df_[col] = to_datetime(df_[col], format=date_format)
        return df_


class MergerTransformer(BaseTransformer):
    def transform(self, df: DataFrame, ref: DataFrame, mergekey: Optional[Union[str, List[str]]] = None,
                  how: str = "inner") -> DataFrame:
        """
        Enrich passed dataframe by merging it with a referential passed as dataframe.
            Uses pandas.DataFrame.merge.

        :param df: the dataframe to enrich
        :param mergekey: the mergekeys the merge will be executed upon (pandas merge's on parameter)
        :param ref: the referential to merge with
        :param how: the mergetype e.g. `inner`, `outer` etc... equivalent to pandas.merge's `how` parameter.
        :return: pd.Dataframe: the enriched dataframe
        """
        return df.merge(ref, how=how, on=mergekey)


class CodeDepartementParserTransformer(BaseParser):
    def __init__(self, coerce=True):
        super().__init__(coerce)
        self.parsed = "un code département"

    def _code_departement(self, value: str):
        if value == str(None) or value == str(NA):
            self._coerce(value)
        else:
            try:
                if "2A" in value.upper():
                    return "2A"
                elif "2B" in value.upper():
                    return "2B"
                elif len(value) == 3 or len(value) == 2:
                    if 0 < int(value) <= 95:
                        return str(int(value))
                    elif 977 > int(value) > 970:
                        return value
                    else:
                        self._coerce(value)
                elif len(value) == 1 and int(value) > 0:
                    return "0" + value
                else:
                    self._coerce(value)
            except ValueError:
                self._coerce(value)

    def transform(self, dataframe: DataFrame, columns: str or List[str]) -> DataFrame:
        df = dataframe.copy()
        if isinstance(columns, list):
            for column in columns:
                df[column] = df[column].astype(str).apply(self._code_departement)
        else:
            df[columns] = df[columns].astype(str).apply(self._code_departement)
        return df
