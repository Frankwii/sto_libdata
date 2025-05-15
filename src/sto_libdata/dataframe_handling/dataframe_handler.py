import pandas as pd

from pandas.io.sql import Series
from sqlalchemy import DATETIME, Boolean, Date, Float, Integer
from sqlalchemy.types import String, TypeEngine as SQLType


class PotentialCHAR(SQLType):
    ...

class UnknownType(SQLType):
    ...


class DataFrameHandler:
    def __init__(self) -> None:
        ...

    def assert_normalized(self, df: pd.DataFrame) -> None:
        """
        Raises a NormalizationError if the given dataframe is not normalized.
        """
        # Not implemented yet

    def infer_SQL_types(self, df: pd.DataFrame) -> dict[str, SQLType]:

        return {str(col_name): self.infer_SQL_type(Series(df[col_name]), col_name) for col_name in df.columns}

    def infer_by_name(self, column_name: str) -> SQLType:
        match [c for c in column_name.capitalize()]:
            case ["I", "D", "_", *c]:
                return Integer()
            case ["D", "S", "_", *c]:
                return String() # VARCHAR(MAX)
            case ["T", "X", "_", *c] | ["C", "O", "_", *c]:
                return PotentialCHAR()
            case ["S", "W", "_", *c]:
                return Boolean()
            case ["D", "A", "_", *c]:
                return Date()
            case ["T", "S", "_", *c]:
                return DATETIME()
            case [*c, "_", "E", "U", "R"] | [*c, "_", "U", "S", "D"]:
                return Float()
            case [*c, "C", "O", "U", "N", "T"]:
                return Integer()
            case _:
               return UnknownType()

    def infer_by_value(self, col: pd.Series | pd.DataFrame) -> SQLType:
        return UnknownType()

    def infer_SQL_type(self, col: pd.Series | pd.DataFrame, col_name: str) -> SQLType:
        type_by_naming_convention= self.infer_by_name(col_name)
        if not isinstance(type_by_naming_convention, UnknownType | PotentialCHAR):
            return type_by_naming_convention

        type_by_value= self.infer_by_value(col)
        if not isinstance(type_by_value, UnknownType):
            return type_by_value

        raise TypeError(f"Unable to infer type for column {col}")
