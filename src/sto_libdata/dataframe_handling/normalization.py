from collections import defaultdict
from typing import Set, cast
import pandas as pd
from sqlalchemy import ForeignKey

type DataFrame = pd.DataFrame


class NormalizationHandler:
    def __init__(self, df: DataFrame, name: str) -> None:
        self.__orig_df = df
        self.__orig_table_name = name
        self.__table_state: dict[str, DataFrame] = {name: df}
        self.__foreign_keys: dict[str, dict[str, ForeignKey]] = defaultdict(dict)

    def get_main_df(self) -> DataFrame:
        return self.__table_state[self.__orig_table_name]

    def get_all(self) -> dict[str, DataFrame]:
        return self.__table_state

    def extract_new_table(self, columns: Set[str], new_table_name: str) -> None:
        list_columns = list(columns)
        new_table_info = self.__orig_df[list_columns].drop_duplicates().copy()

        fk_col_name = f"ID_{new_table_name}"
        new_table_name = f"DIM_{new_table_name}"

        new_table_info.reset_index(drop=True, inplace=True)
        new_table_info[fk_col_name] = new_table_info.index + 1

        current_df = self.get_main_df()

        merged_table = current_df.merge(new_table_info, on=list_columns)
        current_columns = set(merged_table.columns)

        columns_to_keep = list(current_columns - columns)

        merged_table = merged_table[columns_to_keep]

        new_table_info.rename(columns={fk_col_name: "ID"}, inplace=True, errors="raise")

        self.__table_state[self.__orig_table_name] = cast(DataFrame, merged_table.copy())
        self.__table_state[new_table_name] = cast(DataFrame, new_table_info.copy())

        self.__foreign_keys[self.__orig_table_name][fk_col_name] = ForeignKey(f"{new_table_name}.ID")
