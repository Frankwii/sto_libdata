import pandas as pd
from sqlalchemy import Connection, MetaData, Table, inspect

from sto_libdata.dataframe_handling.dataframe_handler import DataFrameTypeHandler
from sto_libdata.dataframe_handling.pushable_dataframe import (
    PushableDF,
    PushableDFWithConfig,
    PushConfig,
    _PushableDataframes,
    _PushableDataframesWithMetadata,
)
from sto_libdata.exceptions.exceptions import (
    NonexistingSchemaError,
    TableDuplicationError,
)

_PushData = tuple[*PushableDFWithConfig, bool]


class ConnectionHandler:
    """
    Interface for interacting with a specific schema in a database.
    """

    def __init__(self, con: Connection, schema: str) -> None:
        self.__con = con
        self.__inspector = inspect(con)
        self.__schema_name = schema
        self.__metadata = MetaData(schema=self.__schema_name, quote_schema=True)
        if not self.__schema_exists(self.__metadata):
            raise NonexistingSchemaError(
                str(self.__metadata.schema), str(self.__con.engine.url.database)
            )

        self.__dataframe_handler = DataFrameTypeHandler()

    def __schema_exists(self, metadata: MetaData) -> bool:
        return self.__inspector.has_schema(str(metadata.schema))

    def __table_exists(self, table: str) -> bool:
        return self.__inspector.has_table(table_name=table, schema=self.__schema_name)

    def __column_exists(self, column: str, table: str) -> bool:
        return column in self.__inspector.get_columns(table, schema=self.__schema_name)

    def download_dataframe(self, table: str) -> pd.DataFrame:
        return pd.read_sql_table(
            table_name=table, con=self.__con, schema=self.__metadata.schema
        )

    def __resolve_table_creation(self, *pdf_data: _PushData) -> None:
        should_create = (
            df
            for df, config, exists in pdf_data
            if exists and config.if_exists == "replace" or not exists
        )

        pdfs = _PushableDataframes(*should_create)
        pdfm = _PushableDataframesWithMetadata(self.__metadata, pdfs)
        pdfm.push(self.__con)
        self.commit_changes()

    def __handle_if_exists(self, *pdf_data: _PushData) -> None:
        should_fail = [
            pdf
            for pdf, config, exists in pdf_data
            if exists and config.if_exists == "fail"
        ]

        if len(should_fail) > 0:
            raise TableDuplicationError(
                f"{self.__schema_name}.{should_fail[0].get_name()}"
            )

        should_drop = (
            pdf.get_name()
            for pdf, config, exists in pdf_data
            if exists and config.if_exists == "replace"
        )

        self.__drop_tables_no_check(*should_drop)

    def __resolve_normalization(self, *pdf_data: _PushData) -> None:
        should_fail_if_not_normalized = (
            df for df, config, _ in pdf_data if config.fail_if_not_normalized
        )

        for pdf in should_fail_if_not_normalized:
            self.__dataframe_handler.assert_normalized(
                pdf.get_dataframe(), pdf.get_coltypes(), pdf.get_name()
            )

    def push_tables(
        self,
        *pushable_dataframes: PushableDF | PushableDFWithConfig,
    ) -> None:
        """
        Uploads pushable dataframes into this handler's database,
        with safeguards regarding normalization and typing.

        Tries to infer types based on the column specified in the `coltypes`
        argument.

        Args:
            *pushable_dataframes: A variable number of PushableDataframes, with
                configuration or not. If no configutarion is specified, the
                default will be instantiated.
        Raises:
            NormalizationError: When `warn_not_normalized` is set to True in
                some of the PushableDataframes config and the dataframe does
                not pass the normalization check.

            TableDuplicationError: When `if_exists` is set to `fail` in some of
                the configs of the PushableDataframes and the corresponding
                table already existed in the database. Currently, this only
                warns about the first such table it finds.

        """
        # Add default config to dataframes that don't specify it
        pushable_dataframes_with_config = (
            x if isinstance(x, tuple) else (x, PushConfig())
            for x in pushable_dataframes
        )

        pushable_dataframes_data = [
            (pdf, config, self.__table_exists(pdf.get_name()))
            for pdf, config in pushable_dataframes_with_config
        ]

        self.__resolve_normalization(*pushable_dataframes_data)
        self.__handle_if_exists(*pushable_dataframes_data)
        self.__resolve_table_creation(*pushable_dataframes_data)

        pdfs = _PushableDataframes(*(pdf for pdf, _, _ in pushable_dataframes_data))

        pdfs.insert(self.__schema_name, self.__con)
        self.commit_changes()

    def __drop_tables_no_check(self, *table_name: str) -> None:
        tables = [
            Table(name, self.__metadata, autoload_with=self.__con)
            for name in table_name
        ]

        self.__metadata.drop_all(self.__con, tables, checkfirst=False)
        for table in tables:
            self.__metadata.remove(table)

    def drop_table(self, table_name: str) -> None:
        """Drops the specified table, checking first whether it exists."""
        if self.__table_exists(table_name):
            table = Table(table_name, self.__metadata, autoload_with=self.__con)

            table.drop(bind=self.__con, checkfirst=False)

            self.commit_changes()
            self.__metadata.remove(table)

    def commit_changes(self) -> None:
        """Persist changes made to the database."""
        self.__con.commit()
