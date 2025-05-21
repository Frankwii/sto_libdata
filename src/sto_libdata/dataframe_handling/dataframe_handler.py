import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)
from pandas.io.sql import Series
from sqlalchemy import CHAR, DATE, DATETIME, Boolean, Date, Float, Integer
from sqlalchemy.types import String
from sqlalchemy.types import TypeEngine as SQLType


class PotentialCHAR(SQLType): ...
class PotentialDATE(SQLType): ...
class UnknownType(SQLType): ...


UndeterminedType = PotentialCHAR | PotentialDATE | UnknownType


class DataFrameHandler:
    def __init__(self) -> None: ...

    def assert_normalized(self, df: pd.DataFrame) -> None:
        """
        Raises a NormalizationError if the given dataframe is not normalized.
        """
        # Not implemented yet

    def infer_SQL_types(self, df: pd.DataFrame) -> dict[str, SQLType]:
        return {
            str(col_name): self.infer_SQL_type(Series(df[col_name]), col_name)
            for col_name in df.columns
        }

    def infer_SQL_type(self, col: pd.Series, col_name: str) -> SQLType:
        inferred_type = self.infer_by_name(col_name)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        inferred_type = self.infer_by_dtype(col)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        inferred_type = self.infer_by_value(inferred_type, col)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        raise TypeError(f"Unable to infer type for column {col}")

    def infer_by_name(self, column_name: str) -> SQLType:
        upper = column_name.upper()
        prefix = f"{upper}_"[:3]

        match prefix:
            case "ID_":
                return Integer()
            case "DS_":
                return String()  # VARCHAR(MAX)
            case "TX_" | "CO_":
                return PotentialCHAR()
            case "SW_":
                return Boolean()
            case "DA_":
                return DATE()
            case "TS_":
                return DATETIME()
            case _:
                pass

        suffix = upper[-4:]
        if suffix == "_EUR" or suffix == "_USD":
            return Float()
        elif upper[-5:] == "COUNT":
            return Integer()

        return UnknownType()

    def infer_by_dtype(self, col: pd.Series) -> SQLType:
        nonnull = pd.Series(col[col.isna() == False])

        if len(nonnull) == 0:
            raise ValueError(
                f"Trying to insert a completely null column!. Name: {col.name}"
            )

        if is_bool_dtype(nonnull):
            return Boolean()
        elif is_integer_dtype(nonnull):
            return Integer()
        elif is_float_dtype(nonnull):
            return Float()
        elif is_datetime64_dtype(nonnull):
            return PotentialDATE()
        elif is_string_dtype(nonnull):
            return PotentialCHAR()
        else:
            return UnknownType()

    def infer_by_value(
        self, inferred_dtype: UndeterminedType, col: pd.Series
    ) -> SQLType:
        if isinstance(inferred_dtype, PotentialDATE):
            return self.resolve_potential_date(col)
        return self.resolve_stringtype(col)

    def resolve_potential_date(self, col: pd.Series) -> SQLType:
        for time_unit in ["hour", "minute", "second"]:
            values = getattr(col.dt, time_unit)
            if not (values.isna() | values == 0).all():
                return DATETIME()

        return DATE()

    def resolve_stringtype(self, col: pd.Series) -> SQLType:
        as_strings = pd.Series(col[col.isna() == False].astype(str))

        lengths = as_strings.apply(len)

        m, M = int(lengths[lengths > 0].min()), int(lengths.max())

        if m == M:
            return CHAR(M)
        else:
            return String(2 * M)
