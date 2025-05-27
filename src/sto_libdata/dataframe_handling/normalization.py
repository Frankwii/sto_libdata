from collections import defaultdict
from typing import cast

import pandas as pd
from sqlalchemy import ForeignKey

type DataFrame = pd.DataFrame


class NormalizationHandler:
    """An interface to normalize a single dataframe into separate tables."""

    def __init__(self, df: DataFrame, name: str) -> None:
        """
        Args:
            df: The dataframe to normalize. It will be transformed and referred
                to as the "main" dataframe in this instance.
            name: The name that the SQL table corresponding to the main
                dataframe should have in the database.
        """
        self.__orig_table_name = name
        self.__table_state: dict[str, DataFrame] = {name: df}
        self.__foreign_keys: dict[str, dict[str, ForeignKey]] = defaultdict(dict)

    def get_main_df(self) -> DataFrame:
        """Fetches the main dataframe in its current state."""
        return self.__table_state[self.__orig_table_name]

    def get_state(self) -> dict[str, DataFrame]:
        """Fetches all of the dataframes that have been extracted so far."""
        return self.__table_state

    def __extract_table(self, base: DataFrame, column_list: list[str]) -> DataFrame:
        """Extracts and deduplicates a list of columns from a `base` dataset.
        Does not mutate the base dataset.
        """
        new_table = base[column_list].drop_duplicates().copy()

        new_table.reset_index(drop=True, inplace=True)
        new_table["ID"] = new_table.index + 1

        return cast(DataFrame, new_table)

    def __replace_columns_by_fk(
        self,
        base: DataFrame,
        dim: DataFrame,
        join_columns: list[str],
        dim_name: str,
    ) -> DataFrame:
        """Replaces a set of columns in `base` by a foreign key to `dim`.

        This foreign key is computed by inner joining the two datasets on
        `column_list`. Therefore, names in `column_list` should be present on
        both dataframes.

        Does not mutate any of the datasets.
        """

        merged = base.merge(dim, on=join_columns, suffixes=("", f"_{dim_name}"))

        all_columns = set(merged.columns)
        dim_columns = set(dim.columns)

        columns_to_keep = list(all_columns - (dim_columns - {"ID"}))

        merged = cast(DataFrame, merged[columns_to_keep])

        return merged

    def extract_new_table(self, columns: set[str], new_table_name: str) -> None:
        """Extracts a new table from a set of columns of the main dataframe.

        This method takes a set of columns of the main dataframe and creates a
        separate dataframe with them without duplicate rows. These columns are
        replaced from the main dataframe by a single foreign key column to the
        newly created dataframe.

        Keeps track of the newly created foreign key relation by storing it in
        an internal attribute.

        Args:
            columns: The set of columns from which to extract a new dataframe.
            new_table_name: The name that the SQL table corresponding to the
                newly created dataframe should have in the database.
        """
        column_list = list(columns)

        current_df = self.get_main_df()

        new_table = self.__extract_table(current_df, column_list)
        substituted_table = self.__replace_columns_by_fk(
            current_df, new_table, column_list, new_table_name
        )

        self.__table_state[self.__orig_table_name] = substituted_table.copy()
        self.__table_state[f"DIM_{new_table_name}"] = new_table.copy()

        self.__foreign_keys[self.__orig_table_name][f"ID_{new_table_name}"] = (
            ForeignKey(f"DIM_{new_table_name}.ID")
        )
