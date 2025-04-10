from sqlalchemy import TextClause, text
from sql_entities import SQLIntType, SQLSchema, SQLTable, SQLColumn, SQLType
import re
from textwrap import dedent

__all__ = ["QueryBuilder", "ParametrizedQuery"]

class ParametrizedQuery():
    """
    Data class for storing parametrized SQLServer queries,
    to be passed to sqlalchemy.Connection.execute.
    """
    def __init__(self, text: TextClause, params: dict[str, str]):
        self.__text = text
        self.__params = params
        self.__is_formatted = len(params) == 0

    def get_text(self) -> TextClause:
        """Getter of the underlying sqlalchemy.TextClause."""
        return self.__text

    def get_params(self) -> dict[str, str]:
        """Getter of the underlying param dictionary."""
        return self.__params

    def is_formattable(self) -> bool:
        """Whether all of the query's parameters have been specified."""
        return self.__is_formatted

    def format_params(self, params: dict[str, str]):
        """
        Assign values to all of the underlying parameters. Should be
        specified in a dictionary ('params') where keys are the parameters'
        names and values are to be substituted in the underlying query
        """
        assert set(params.keys()) == set(self.__params.keys())

        self.__params = params
        self.__is_formatted = True

class QueryBuilder():
    """
    Utility class for generating parametrized SQLServer queries.
    All public non-static methods return a ParametrizedQuery.
    """
    parameter_regex = re.compile(r"[^\\|\b]*:(\w+)\b")

    @staticmethod
    def format_query_string(query: str) -> str:
        x = dedent(query).strip()
        if not x.endswith(";"):
            x += ";"
    
        return x

    @staticmethod
    def detect_parameter_names(query: str) -> list[str]:
        return QueryBuilder.parameter_regex.findall(query)

    @staticmethod
    def initialize_params(query: str) -> dict[str, str]:
        keys = QueryBuilder.detect_parameter_names(query)

        return {k:"" for k in keys}

    def __process_query_string(self, query: str)->ParametrizedQuery:
        formatted = self.format_query_string(query)
        params = self.initialize_params(formatted)

        return ParametrizedQuery(text(formatted), params)

    def check_table_existence(self, table:SQLTable)->ParametrizedQuery:
        """Gets a nonempty result if the table exists, empty otherwise."""
        schema = table.get_schema_name()

        query = f"""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table.get_name()}'
        """

        return self.__process_query_string(query)

    def check_column_existence(self, column:SQLColumn)->ParametrizedQuery:
        """Gets a nonempty result if the column exists, empty otherwise."""
        query = self.__select_column_of_columns_metatable(column, "1")

        return self.__process_query_string(query)

    def drop_table(self, table:SQLTable)->ParametrizedQuery:
        """Drops a table. Doesn't enforce dropping cascades or constraints."""
        query = f"DROP TABLE {table}"

        return self.__process_query_string(query)

    def annotate_primary_key(self, column:SQLColumn)->ParametrizedQuery:
        """Annotates a given column as the primary key of its table."""

        table = column.get_parent()
        query = f"""
            ALTER TABLE {table} ADD PRIMARY KEY ({column.get_name()})
        """

        return self.__process_query_string(query)

    def add_not_null(self, column: SQLColumn, sql_type: SQLType) -> ParametrizedQuery:
        """
        Adds the NOT NULL constraint to a given column.
        Needs the type of the column for this
        """
        table = column.get_parent()

        query = f"""
            ALTER TABLE {table}
            ALTER COLUMN {column.get_name()} {sql_type} NOT NULL
        """

        return self.__process_query_string(query)

    def __select_column_of_columns_metatable(self, column: SQLColumn, meta_column: str) -> str:
        """SELECTs a single (meta_)column of INFORMATION_SCHEMA.COLUMNS for the specified column."""
        schema_name = column.get_schema_name()
        table_name = column.get_table_name()

        query = f"""
            SELECT {meta_column} FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
                AND TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = '{column.get_name()}'
        """
        return query

    def get_data_type(self, column: SQLColumn) -> ParametrizedQuery:
        """
        Gets the DATA_TYPE annotated in INFORMATION_SCHEMA.COLUMNS for the given column.
        """
        query = self.__select_column_of_columns_metatable(column, "DATA_TYPE")

        return self.__process_query_string(query)

    def get_chartype_length(self, column: SQLColumn) -> ParametrizedQuery:
        """
        Gets the CHARACTER_MAXIMUM_LENGTH annotated in INFORMATION_SCHEMA.COLUMNS for the given column.
        """
        query = self.__select_column_of_columns_metatable(column, "CHARACTER_MAXIMUM_LENGTH")

        return self.__process_query_string(query)

    def annotate_foreign_key(self, from_column: SQLColumn, to_column: SQLColumn) -> ParametrizedQuery:
        raise NotImplementedError()


if __name__ == "__main__":
    qb = QueryBuilder()

    schema = SQLSchema("BSN")
    table = SQLTable(name="DIM_CCAA", parent=schema)
    column = SQLColumn(name="ID", parent=table)

    print(qb.add_not_null(column, SQLIntType()).get_text())
    print(qb.annotate_primary_key(column).get_text())
