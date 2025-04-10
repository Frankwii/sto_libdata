from typing import Any, Optional
from sqlalchemy import Connection, CursorResult, Row
from textwrap import dedent

from database_connection import init_engine
from querybuilder import QueryBuilder, ParametrizedQuery
from sql_entities import SQLCharType, SQLColumn, SQLDateType, SQLDatetimeType, SQLIntType, SQLNcharType, SQLNvarcharType, SQLSchema, SQLTable, SQLType, SQLVarcharType

class ConnectionHandler():
    """
    Interface class for interacting with the database.
    """
    def __init__(self, connection: Connection):
        self.__con = connection
        self.__query_builder = QueryBuilder()

    def __execute(self, query: ParametrizedQuery) -> CursorResult:
        if not query.is_formattable():
            errmsg = dedent(f"""
                            Trying to execute parametrized query without specifying the parameters.
                            Underlying query:
                            {query.get_text()}
                            Parameters found:
                            {query.get_params()}
                            """.strip())
            raise ValueError(errmsg)

        text = query.get_text()
        params = query.get_params()

        return self.__con.execute(text, params)

    def __get_first_row(self, query: ParametrizedQuery) -> Optional[Row]:
        """Gets the first row retrieved when executing the given query, if any."""
        return self.__execute(query).first()

    def __get_one_entry(self, query: ParametrizedQuery) -> Optional[Any]:
        """
        Gets the first entry of the first row retrieved when executing the
        given query, if there is a first row.
        """
        first_row = self.__get_first_row(query)
        if first_row is None:
            return None
        return first_row[0]

    def __check_nonempty(self, query: ParametrizedQuery) -> bool:
        """
        Returns True if the result of executing the query was not empty,
        False otherwise. Discards the actual result.
        """
        return bool(self.__get_first_row(query))

    def check_table_existence(self, table: SQLTable) -> bool:
        """Checks whether table exists in the database."""
        query = self.__query_builder.check_table_existence(table)

        return self.__check_nonempty(query)

    def check_column_existence(self, column: SQLColumn) -> bool:
        """Checks whether column exists in the database."""
        query = self.__query_builder.check_column_existence(column)

        return self.__check_nonempty(query)

    def drop_table(self, table: SQLTable) -> None:
        """
        Drops the specified table, without checking whether it exists.

        Raises: # TODO: Find out which error type this is
            If the table doesn't exist.
        """
        query = self.__query_builder.drop_table(table)
        self.__execute(query)

    def __get_chartype_length(self, column: SQLColumn) -> str:
        """
        Gets the maximum length for a given column with a character-based type.
        """
        number = int(self.__get_one_entry(self.__query_builder.get_chartype_length(column)))

        return "MAX" if number==-1 else str(number)

    def __get_data_type_parameters(self, column: SQLColumn, data_type: str) -> list[str]:
        match data_type.upper():
            case "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR":
                return [self.__get_chartype_length(column)]
            case "INT" | "DATE" | "DATETIME" | "DATETIME2":
                return []
            case _:
                raise TypeError(f"SQLType for received data_type {data_type} has not been implemented yet.")

    def __initialize_sqltype(self, data_type: str, parameters: list[str]) -> SQLType:
        match data_type.upper():
            case "CHAR":
                return SQLCharType(*parameters)
            case "NCHAR":
                return SQLNcharType(*parameters)
            case "VARCHAR":
                return SQLVarcharType(*parameters)
            case "NVARCHAR":
                return SQLNvarcharType(*parameters)
            case "DATETIME" | "DATETIME2":
                return SQLDatetimeType(*parameters)
            case "INT":
                return SQLIntType()
            case "DATE":
                return SQLDateType()
            case _:
                raise TypeError(f"SQLType for received data_type {data_type} has not been implemented yet.")

    def get_column_type(self, column: SQLColumn) -> SQLType:
        """
        Gets the type of a given column. Doesn't check whether it exists first.
        """
        data_type_query = self.__query_builder.get_data_type(column)
        data_type = str(self.__get_one_entry(data_type_query))
        type_parameters = self.__get_data_type_parameters(column, data_type)
        return self.__initialize_sqltype(data_type, type_parameters)

    def enforce_not_nullable(self, column: SQLColumn, check_existence = True) -> None:
        """
        Forces the constraint NOT NULL onto the specified column.
        """
        if check_existence and not self.check_column_existence(column):
            raise ValueError(f"Column {column} doesn't exist")

        sql_type = self.get_column_type(column)
        query = self.__query_builder.add_not_null(column=column, sql_type=sql_type)

        self.__execute(query)

    def enforce_primary_key(
            self,
            column: SQLColumn,
            check_existence = True
        ) -> None:
        """
        Forces a column to be the primary key of its table.
        Checks whether it exists first depending on `check_existence`
        """
        if check_existence and not self.check_column_existence(column):
            raise ValueError(f"Column {column} doesn't exist")

        self.enforce_not_nullable(column, check_existence = False)
        query = self.__query_builder.annotate_primary_key(column)

        self.__execute(query)

    def enforce_foreign_key(
            self,
            from_column: SQLColumn,
            to_column: SQLColumn,
            enforce_primary_key: bool = False,
            check_existence: bool = False
            ) -> None:
        """
        Enforces that the column "from_column" is a FOREIGN KEY to "to_column".
        Optionally, also enforces that "to_column" is a primary key in its
        own table and whether both columns exists.
        """
        if check_existence:
            if not self.check_column_existence(from_column):
                raise ValueError(f"Column {from_column} doesn't exist")
            elif not self.check_column_existence(to_column):
                raise ValueError(f"Column {to_column} doesn't exist")

        if enforce_primary_key:
            self.enforce_primary_key(to_column, check_existence = False)

        query = self.__query_builder.annotate_foreign_key(from_column, to_column)

        self.__execute(query)

if __name__=="__main__":
    bsn = SQLSchema("BSN")
    dim_ccaa = SQLTable("DIM_CCAA", bsn)
    col = SQLColumn("FE_VIGOR_INI", dim_ccaa)

    engine = init_engine()
    handler = ConnectionHandler(engine.connect())

    print(handler.get_column_type(col))
