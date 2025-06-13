import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)
from pandas.io.sql import Series
from sqlalchemy import CHAR, DATE, DATETIME, Boolean, Float, Integer
from sqlalchemy.types import String
from sqlalchemy.types import TypeEngine as SQLType

from sto_libdata.exceptions.exceptions import NormalizationError
from concurrent.futures import ProcessPoolExecutor


class PotentialCHAR(SQLType): ...


class PotentialDATE(SQLType): ...


class UnknownType(SQLType): ...


UndeterminedType = PotentialCHAR | PotentialDATE | UnknownType

def _infer_column_type(t: tuple[str, Series]) -> tuple[str, SQLType]:
    return __infer_column_type(*t)


def __infer_column_type(name: str, column: Series) -> tuple[str, SQLType]:
    handler = DataFrameTypeHandler()

    return name, handler.infer_SQL_type(column, name)

class DataFrameTypeHandler:
    def __init__(self) -> None: ...

    def assert_normalized(
        self, df: pd.DataFrame, coltypes: dict[str, SQLType], table_name: str
    ) -> None:
        """
        Raises a NormalizationError if the given dataframe is not normalized.
        """
        string_column_normalization_data = (
            (
                colname,
                self.determine_string_column_normalization(
                    pd.Series(df[colname]), colname
                ),
            )
            for colname, coltype in coltypes.items()
            if isinstance(coltype, String)
        )

        non_normalized_data = [
            (colname, reason)
            for colname, (is_normalized, reason) in string_column_normalization_data
            if not is_normalized
        ]

        if len(non_normalized_data) >= 1:
            raise NormalizationError(table_name, non_normalized_data)

    def determine_string_column_normalization(
        self, col: pd.Series, name: str
    ) -> tuple[bool, str]:
        if len(col.unique()) != len(col):
            msg = (
                f"Column {name} is not normalized because it contains"
                " duplicate strings. Consider creating a separate table"
                " for this and using foreign keys to it, or if absolutely"
                " necessary, pass a flag to prevent normalization checking."
            )

            return False, msg

        return True, ""

    def infer_SQL_types(self, df: pd.DataFrame) -> dict[str, SQLType]:

        names_and_columns = [(name, Series(df[name])) for name in df.columns]

        with ProcessPoolExecutor() as executor:
            names_and_inferred_types = list(executor.map(
                _infer_column_type,
                names_and_columns
            ))

        return {name: inferred_type for (name, inferred_type) in names_and_inferred_types}

    def infer_SQL_type(self, col: pd.Series, col_name: str) -> SQLType:
        inferred_type = self.__infer_by_name(col_name)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        inferred_type = self.__infer_by_dtype(col)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        inferred_type = self.__infer_by_value(inferred_type, col)
        if not isinstance(inferred_type, UndeterminedType):
            return inferred_type

        raise TypeError(f"Unable to infer type for column {col}")

    def __infer_by_name(self, column_name: str) -> SQLType:
        upper = column_name.upper()
        if upper == "DATE":
            return DATE()

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

    def __infer_by_dtype(self, col: pd.Series) -> SQLType:
        nonnull = pd.Series(col[col.isna() == False]) # noqa: E712


        if len(nonnull) == 0:
            raise ValueError(
                f"Trying to insert a completely null column!. Name: {col.name}"
            )

        inferred_dtype = nonnull.infer_objects().dtype
        if is_bool_dtype(inferred_dtype):
            return Boolean()
        elif is_integer_dtype(inferred_dtype):
            return Integer()
        elif is_float_dtype(inferred_dtype):
            return Float()
        elif is_datetime64_dtype(inferred_dtype):
            return PotentialDATE()
        elif is_string_dtype(inferred_dtype):
            return PotentialCHAR()
        else:
            return UnknownType()

    def __infer_by_value(
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
        as_strings = pd.Series(col[col.isna() == False].astype(str)) # noqa: E712

        lengths = as_strings.apply(len)

        m, M = int(lengths[lengths > 0].min()), int(lengths.max())

        if m == M:
            return CHAR(M)
        else:
            return String(2 * M)
