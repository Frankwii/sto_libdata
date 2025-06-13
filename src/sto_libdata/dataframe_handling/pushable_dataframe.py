from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from typing import Any, Iterator, Literal, Optional, cast

import pandas as pd
from sqlalchemy import Column, Connection, ForeignKey, MetaData, Table
from sqlalchemy.types import TypeEngine as SQLType

from sto_libdata.dataframe_handling.dataframe_handler import DataFrameTypeHandler


class PushableDF:
    """An enriched dataframe with information regarding its representation in
    the database.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        table_name: str,
        coltypes: dict[str, SQLType],
        primary_key: str = "ID",
        foreign_keys: dict[str, ForeignKey] = {},
        constraints: dict[str, dict[str, Any]] = {},
    ) -> None:
        """
        Args:
            df: The dataframe.
            table_name: Name of the target table in the database. This should
                not include the name of the schema; just the table's.
            coltypes: A dictionary that maps all, some or none of `df`'s
                columns to the SQL types they should have in the database.
                If specified, said types should be SQLAlchemy types (check their
                docs: https://docs.sqlalchemy.org/en/21/core/type_basics.html).
                For those columns that are not specified, this class uses a
                custom heuristic to determine which SQLAlchemy type the columns
                should have (see `DataFrameHandler.infer_SQL_type` for this).
            primary_key: Which one of the columns in the table acts as the PK.
                Defaults to "ID". Currently, there is no support for multicolumn
                primary keys nor "autoincrement"-style features.
            foreign_keys: A dictionary mapping column names to sqlalchemy.
                ForeignKey objects. Tables and columns being pointed to should
                either already exist in the database or be created in a the same
                batch as the current dataframe.
            constraints: A dictionary mapping column names to a dictionary with
                the sqlalchemy constraints that column should have (check their
                docs: https://docs.sqlalchemy.org/en/20/core/constraints.html).
                This need not include the "primary_key" constraint as the class
                adds it automatically, but there won't be a problem if it's
                included.
        """
        self.__df = df
        self.__table_name = table_name
        self.__coltypes = self.__infer_remaining_coltypes(coltypes)
        assert primary_key in self.__coltypes.keys(), (
            "Specified PK not found among dataframe columns."
        )

        basedict = {name: {} for name in self.__coltypes.keys()}
        self.__constraints = basedict | constraints
        self.__constraints[primary_key]["primary_key"] = True
        self.__constraints[primary_key]["autoincrement"] = False
        self.__foreign_keys = foreign_keys

        self.__table: Optional[Table] = None

    def get_name(self) -> str:
        return self.__table_name

    def get_dataframe(self) -> pd.DataFrame:
        return self.__df

    def get_coltypes(self) -> dict[str, SQLType]:
        return self.__coltypes

    def __infer_remaining_coltypes(
        self, specified: dict[str, SQLType]
    ) -> dict[str, SQLType]:
        df_columns = set(str(col) for col in self.__df.columns)

        specified_columns = set(specified.keys())

        df_handler = DataFrameTypeHandler()

        return {
            col: specified[col]
            if col in specified_columns
            else df_handler.infer_SQL_type(self.__df[col], col)
            for col in df_columns
        }

    def _instantiate_underlying_table(self, metadata: MetaData) -> None:
        column_positional_constraints = {name: [] for name in self.__coltypes.keys()}

        for col, v in self.__foreign_keys.items():
            column_positional_constraints[col] = [v]

        columns = (
            Column(
                name,
                type_,
                *column_positional_constraints[name],
                **(self.__constraints[name]),
            )
            for name, type_ in self.__coltypes.items()
        )

        self.__table = Table(
            self.__table_name,
            metadata,
            *columns,
        )

    def get_underlying_table(self) -> Optional[Table]:
        return self.__table

    def get_foreign_keys(self) -> Iterator[str]:
        return (
            fk.target_fullname.rsplit(".", 1)[0].split(".", 1)[-1]
            for fk in self.__foreign_keys.values()
        )


class _PushableDataframes:
    def __init__(self, *pdfs: PushableDF) -> None:
        self.__pdfs = list(pdfs)
        self.__name_index = {pdf.get_name(): pdf for pdf in self.__pdfs}
        self.__names = set(self.__name_index.keys())
        self.__is_sorted = False

    def __get_relevant_fks(self, name: str) -> list[str]:
        return [
            referenced
            for referenced in self.__name_index[name].get_foreign_keys()
            if referenced in self.__names
        ]

    def __sort_insertably(self) -> None:
        """
        Performs a topological ordering of the table DAG.
        See https://en.wikipedia.org/wiki/Topological_sorting.

        This is needed so that tables with foreign keys pointing to others are
        inserted after the pointee.

        Nodes correspond to tables (actually, their names are the nodes) and
        there is an arrow from table A to table B if there is some column of
        A that is a foreign key to some column of B.

        This implementation is likely not assymptotically optimal but it is my
        own :D.
        """
        if self.__is_sorted:
            return
        # {name: referenced names}
        index = {}
        # {name: names that reference it}
        inverted_index = defaultdict(set)

        for name in self.__names:
            index[name] = self.__get_relevant_fks(name)
            for referenced in index[name]:
                inverted_index[referenced].add(name)

        roots = (name for name in self.__names if len(inverted_index[name]) == 0)

        depths = {name: 0 for name in self.__names}

        def assign_depths(name: str):
            for child in index[name]:
                if depths[child] < (d := (depths[name] + 1)):
                    depths[child] = d
                    assign_depths(child)
                elif depths[child] > len(self.__names):
                    raise ValueError("Cyclical foreign key references detected!")

        for root in roots:
            assign_depths(root)

        self.__pdfs.sort(key=lambda pdf: depths[pdf.get_name()], reverse=True)
        self.__is_sorted = True

    def __getitem__(self, idx: int) -> PushableDF:
        return self.__pdfs[idx]

    def __len__(self) -> int:
        return len(self.__pdfs)

    def insert(self, schema_name: str, con: Connection) -> None:
        self.__sort_insertably()
        for pdf in self.__pdfs:
            dtype = cast(None, pdf.get_coltypes())

            pdf.get_dataframe().to_sql(
                schema=schema_name,
                name=pdf.get_name(),
                con=con,
                if_exists="append",
                dtype=dtype,
                chunksize=1000,
                index=False,
            )


class _PushableDataframesWithMetadata:
    def __init__(self, metadata: MetaData, pdfs: _PushableDataframes) -> None:
        self.__pdfs = pdfs
        self.__name_index = {pdf.get_name(): pdf for pdf in self.__pdfs}
        self.__loaded_names = set(self.__name_index.keys())
        self.__metadata = metadata

        for pdf in self.__pdfs:
            pdf._instantiate_underlying_table(self.__metadata)

    def __load_foreign_key_metadata(self, con: Connection) -> None:
        pointee_table_names = chain.from_iterable(
            pdf.get_foreign_keys() for pdf in self.__pdfs
        )

        for pointee_name in pointee_table_names:
            if pointee_name not in self.__loaded_names:
                Table(pointee_name, self.__metadata, autoload_with=con)
                self.__loaded_names.add(pointee_name)

    def push(self, con: Connection) -> None:
        self.__load_foreign_key_metadata(con)
        tables = cast(list[Table], [pdf.get_underlying_table() for pdf in self.__pdfs])
        self.__metadata.create_all(con, tables=tables)


@dataclass
class PushConfig:
    """
    Options for pushing a dataframe into the database.

    Args:
        if_exists: One of "fail", "replace", "append".
            What to do should the table already exist in the database.
            By default, it fails.
        warn_not_normalized: Whether to use a custom heuristic (see
            `DataFrameHandler.assert_normalized`) to determine if the passed
            dataframe is properly normalized, and raise an exception if not.
            Defaults to True.
    """

    if_exists: Literal["fail", "replace", "append"] = "fail"
    fail_if_not_normalized: bool = True


PushableDFWithConfig = tuple[PushableDF, PushConfig]
