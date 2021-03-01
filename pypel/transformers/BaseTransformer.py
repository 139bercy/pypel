import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Transformer:
    def __init__(self,
                 df: pd.DataFrame,
                 strip: list = None,
                 column_replace: dict = None,
                 df_replace: dict = None,
                 date_format: str = None,
                 date_columns: list = None):
        self.column_replace = {} if not column_replace else column_replace
        self.df_replace = {} if not df_replace else df_replace
        self.columns_to_strip = [] if not strip else strip
        self.df = df.copy()
        self.date_format = date_format
        self.date_columns = date_columns

    def transform(self):
        self.format_dataframe_columns()
        self.format_dates()
        self.format_na()
        return self.df

    def format_dataframe_columns(self):
        """
        returns df_ with no accents in column names, column names all UPPERCASE and _ separated
        also drops column with all na values
        """
        self.df.columns = self.df.columns.str.strip()
        self.df.replace(self.df_replace, regex=True, inplace=True)
        self.df.columns = self.df.columns.to_series().replace(self.column_replace, regex=True).apply(str.upper)
        for column in self.columns_to_strip:
            try:
                self.df[column] = self.df[column].str.strip()
            except KeyError:
                logger.warning(f"NO SUCH COLUMN {column} IN DATAFRAME PASSED")

    def format_na(self):
        """
        fills missing values of self.df with None
        """
        self.df = self.df.applymap(lambda x: None if x == "NaT" or x == "nan" else x)
        self.df = self.df.where(pd.notnull(self.df), None)

    def format_dates(self, date_format: str = None, date_columns: list = None):
        df = date_format if date_format else self.date_format

        cols = date_columns if date_columns else self.date_columns
        if cols is None and df is None:
            logger.debug("No date columns and no date format, assuming there is nothing to do.")
            return
        elif df is None:
            logger.error("Incorrect usage : date_format not specified as argument nor in Transformer's constructor")
            return
        elif cols is None:
            logger.error("Incorrect usage : date_columns not specified as argument nor in Transformer's constructor")
            return
        for col in cols:
            self.df = self.df[col].dt.strftime(df)
