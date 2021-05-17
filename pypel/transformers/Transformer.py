import pandas as pd
import warnings


class Transformer:
    def __init__(self,
                 strip: list = None,
                 column_replace: dict = None,
                 df_replace: dict = None,
                 date_format: str = None,
                 date_columns: list = None):
        self.column_replace = {} if not column_replace else column_replace
        self.df_replace = {} if not df_replace else df_replace
        self.columns_to_strip = [] if not strip else strip
        self.date_format = date_format
        self.date_columns = date_columns

    def transform(self,
                  dataframe: pd.DataFrame):
        df = dataframe.copy()
        self.format_str_columns(df)
        self.format_contents(df)
        self.format_dates(df)
        df = self.format_na(df)
        return df

    def format_str_columns(self, df: pd.DataFrame):
        """
        returns df_ with no accents in column names, column names all UPPERCASE and _ separated
        also drops column with all na values
        """
        df.columns = df.columns.astype(str).str.strip()
        df.columns = df.columns.to_series().replace(self.column_replace, regex=True).apply(str.upper)
        for column in self.columns_to_strip:
            try:
                df[column] = df[column].str.strip()
            except KeyError:
                warnings.warn(f"NO SUCH COLUMN {column} IN DATAFRAME PASSED")
        return df

    def format_contents(self, df: pd.DataFrame):
        df.replace(self.df_replace, regex=True, inplace=True)
        return df

    def format_na(self, df: pd.DataFrame):
        """
        fills missing values of self.df with None
        """
        df = df.applymap(lambda x: None if x == "NaT" or x == "nan" else x)
        df = df.where(pd.notnull(df), None)
        return df

    def format_dates(self, df: pd.DataFrame, date_format: str = None, date_columns: list = None):
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
