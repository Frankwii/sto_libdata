import pandas as pd

from pandas.io.sql import Series
from sqlalchemy.types import TypeEngine as SQLType

class DataFrameHandler:
    def __init__(self) -> None:
        ...

    def assert_normalized(self, df: pd.DataFrame) -> None:
        """
        Raises a NormalizationError if the given dataframe is not normalized.
        """
        # Not implemented yet

    def infer_SQL_types(self, df: pd.DataFrame) -> dict[str, SQLType]:

        return {str(col): self.infer_SQL_type(Series(df[col])) for col in df.columns}

    def infer_SQL_type(self, col: pd.Series | pd.DataFrame) -> SQLType:
        raise NotImplementedError("Pending")
