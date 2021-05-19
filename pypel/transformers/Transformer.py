import os

from pypel.extractors.Extractor import Extractor
import pandas as pd
import warnings


class Transformer:
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
                 strip: list = None,
                 column_replace: dict = None,
                 df_replace: dict = None,
                 date_format: str = None,
                 date_columns: list = None,
                 referential=None):
        self.column_replace = {} if not column_replace else column_replace
        self.df_replace = {} if not df_replace else df_replace
        self.columns_to_strip = [] if not strip else strip
        self.date_format = date_format
        self.date_columns = date_columns

    def transform(self,
                  dataframe: pd.DataFrame):
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

    def _format_str_columns(self, df: pd.DataFrame):
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

    def _format_contents(self, df: pd.DataFrame):
        """
        returns df with normalized contents, replacing every value using self.df_replace
        self.df_replace should be in the following format : {"value_to_replace": "replacement_value"}

        :param df: the dataframe to normalize
        :return: the normalized dataframe
        """
        df.replace(self.df_replace, regex=True, inplace=True)
        return df

    def _format_na(self, df: pd.DataFrame):
        """
        returns df with replaced NaNs & NaTs in the passed dataframe by None

        :param df: the dataframe that is to be treated
        :return: the modified dataframe, with Nones instead of NaNs & NaTs
        """
        df = df.applymap(lambda x: None if x == "NaT" or x == "nan" else x)
        df = df.where(pd.notnull(df), None)
        return df

    def _format_dates(self, df: pd.DataFrame, date_format: str = None, date_columns: list = None):
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
                          df,
                          mergekey: str or list,
                          referential: str or os.PathLike or pd.DataFrame,
                          how="inner",
                          converters=None,
                          dates=False,
                          dates_format='%Y-%m-%d',
                          sheet_name=0,
                          skiprows=None):
        if isinstance(referential, pd.DataFrame):
            df.merge(referential, how=how, on=mergekey)
        else:
            return df.merge(Extractor(converters, dates, dates_format, sheet_name, skiprows).init_dataframe(referential),
                            how=how,
                            on=mergekey)
