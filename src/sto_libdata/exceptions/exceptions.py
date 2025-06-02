from abc import ABC, abstractmethod
from textwrap import dedent
from typing import Iterable, Optional

from sqlalchemy import Table

class LibdataError(ABC, Exception):
    def __init__(self, errmsg: str, db_name: Optional[str] = None) -> None:
        prepend = "" if db_name is None else f"DATABASE: `{db_name}`\n"
        errmsg_with_db = f"{prepend}{self._prettify(errmsg)}"
        super().__init__(errmsg_with_db)

    @staticmethod
    def _prettify(errmsg: str) -> str:
        return dedent(errmsg)

class NonexistingSQLError(LibdataError):
    """
    To be raised when a DDL SQL entity (schema, table or column) does not exist.
    Not meant to be instantiated directly; use a concrete class from below.
    """
    def __init__(
            self,
            name: str,
            db_name: str
    ) -> None:
        errmsg=f"""
        Attempting to access {self.ddl_type} `{name}` but it does not exist.
        """
        super().__init__(errmsg, db_name)

    @property
    @abstractmethod
    def ddl_type(self) -> str:
        ...

class NonexistingSchemaError(NonexistingSQLError):
    """To be raised when a schema does not exist."""
    @property
    def ddl_type(self) -> str:
        return "schema"

class NonexistingTableError(NonexistingSQLError):
    """To be raised when a table does not exist."""
    @property
    def ddl_type(self) -> str:
        return "table"

class NonexistingColumnError(NonexistingSQLError):
    """To be raised when a column does not exist."""
    @property
    def ddl_type(self) -> str:
        return "column"

class TableOperationError(LibdataError):
    """
    To be raised when operations regarding tables go wrong.
    Class used just for grouping.
    """

class TableDuplicationError(TableOperationError):
    """To be raised when attempting to create a table that already exists"""
    def __init__(self, table_fullname: str):
        errstring = f"""
        Attempting to create table which already exists in the database: {table_fullname}
        """

        super().__init__(errstring)

class NormalizationError(TableOperationError):
    """To be raised when a table is not normalized."""
    def __init__(self, table_fullname: str, conflicting_columns: Iterable[tuple[str, str]]):

        errstring = f"""
        Attempting to create a non-normalized table {table_fullname}. Conflicting columns are the following:
        {"\n".join((f'{column_name}: {reason}' for column_name, reason in conflicting_columns))}
        """

        super().__init__(errstring)

class UnexpectedColumnsError(TableOperationError):
    """
    To be raised when a column is expected to exist in a table but it
    doesn't, or vice versa.
    """
    def __init__(self, table: Table, expected_columns: set[str], specified_columns: set[str]):
        missing_cols = specified_columns.difference(expected_columns)
        extra_cols = specified_columns.difference(expected_columns)

        errstring = f"""
        Wrong columns specified when trying to create {table}.
        Specified: {specified_columns}.
        Missing: {missing_cols}.
        Extra: {extra_cols}.
        """

        super().__init__(errstring)
