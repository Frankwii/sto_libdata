import math
from itertools import chain, repeat
from typing import Iterable

import pandas as pd

from src.sto_libdata import NamedDataFrame
from src.sto_libdata.dataframe_handling.normalization import (
    NormalizationHandler,
)


def assert_dataframe_equality(df1: pd.DataFrame, df2: pd.DataFrame, name: str) -> None:
    assert (cols := set(df1.columns)) == set(df2.columns), (
        f"Columns in {name} don't coincide."
    )

    for col in cols:
        assert (df1[col] == df2[col]).all(), f"{col} has different values"


def assert_dataframe_dict_equality(
    d1: dict[str, pd.DataFrame], d2: dict[str, pd.DataFrame]
) -> None:
    assert set(d1.keys()) == set(d2.keys())

    for k in d1.keys():
        assert_dataframe_equality(d1[k], d2[k], k)


def get_not_normalized_dataframe():
    df = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "CO_INE": [1, 1, 1, 2, 5],
            "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
            "TX_ES": ["Juan", "Juan", "Juan", "Juan", "Juan otra vez"],
        }
    )

    return df


def test_table_extraction():
    df = get_not_normalized_dataframe()

    normhandler = NormalizationHandler(NamedDataFrame(df=df, name="MY_FAC_TABLE"))

    normhandler.extract_new_table("MY_FAC_TABLE", {"TX_ES"}, "DIM_NAME")

    expected_fac = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "CO_INE": [1, 1, 1, 2, 5],
            "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
            "ID_NAME": [1, 1, 1, 1, 2],
        }
    )

    expected_dim = pd.DataFrame({"ID": [1, 2], "TX_ES": ["Juan", "Juan otra vez"]})

    expected_output = {"MY_FAC_TABLE": expected_fac, "DIM_NAME": expected_dim}

    assert_dataframe_dict_equality(normhandler.get_state(), expected_output)


def expand_n_times(n: int, o: Iterable) -> Iterable:
    return chain.from_iterable(repeat(el, n) for el in o)


def get_dataframe_2() -> tuple[pd.DataFrame, pd.DataFrame]:
    ids = list(range(4 * 7))
    weekdays = [
        "Lunes",
        "Martes",
        "Miercoles",
        "Jueves",
        "Viernes",
        "Sábado",
        "Domingo",
    ]

    weekday_ids = list(range(1, 8))

    days = list(expand_n_times(4, weekdays))
    day_ids = list(expand_n_times(4, weekday_ids))

    primitive_countries_es = ["España", "Portugal", "Francia", "Alemania"]
    primitive_countries_ca = ["Espanya", "Portugal", "França", "Alemanya"]
    primitive_country_ids = [1, 2, 3, 4]

    countries_es = list(expand_n_times(7, primitive_countries_es))
    countries_ca = list(expand_n_times(7, primitive_countries_ca))
    country_ids = list(expand_n_times(7, primitive_country_ids))

    values = [math.sin(10 * i) for i in range(4 * 7)]

    raw_df = pd.DataFrame(
        {
            "ID": ids,
            "DAY": days,
            "COUNTRY_ES": countries_es,
            "COUNTRY_CA": countries_ca,
            "VAL": values,
        }
    )

    substituted_ids_df = pd.DataFrame(
        {"ID": ids, "ID_DAY": day_ids, "ID_COUNTRY": country_ids, "VAL": values}
    )

    return raw_df, substituted_ids_df


def test_advanced_normalization():
    raw_df, normalized_df = get_dataframe_2()

    weekday_df = pd.DataFrame(
        {
            "ID": list(range(1, 8)),
            "DAY": [
                "Lunes",
                "Martes",
                "Miercoles",
                "Jueves",
                "Viernes",
                "Sábado",
                "Domingo",
            ],
        }
    )

    country_df = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4],
            "COUNTRY_ES": ["España", "Portugal", "Francia", "Alemania"],
            "COUNTRY_CA": ["Espanya", "Portugal", "França", "Alemanya"],
        }
    )

    expected_dataframes = {
        "FAC_TABLE": normalized_df,
        "DIM_DAY": weekday_df,
        "DIM_COUNTRY": country_df,
    }

    normalization_handler = NormalizationHandler(
        NamedDataFrame(df=raw_df, name="FAC_TABLE")
    )

    normalization_handler.extract_new_table(
        "FAC_TABLE", {"COUNTRY_ES", "COUNTRY_CA"}, "DIM_COUNTRY"
    )

    normalization_handler.extract_new_table("FAC_TABLE", {"DAY"}, "DIM_DAY")

    output_dataframes = normalization_handler.get_state()

    assert_dataframe_dict_equality(output_dataframes, expected_dataframes)


def get_dataframe_3() -> tuple[pd.DataFrame, pd.DataFrame]:
    ids = list(range(4 * 7))
    weekdays = [
        "Lunes",
        "Martes",
        "Miercoles",
        "Jueves",
        "Viernes",
        "Sábado",
        "Domingo",
    ]

    weekday_ids = list(range(1, 8))

    days = list(expand_n_times(4, weekdays))
    day_ids = list(expand_n_times(4, weekday_ids))

    primitive_countries_es = ["España", "Portugal", "Francia", "Alemania"]
    primitive_countries_ca = ["Espanya", "Portugal", "França", "Alemanya"]
    primitive_country_ids = [1, 2, 3, 4]

    countries_es = list(expand_n_times(7, primitive_countries_es))
    countries_ca = list(expand_n_times(7, primitive_countries_ca))
    country_ids = list(expand_n_times(7, primitive_country_ids))

    values = [math.sin(10 * i) for i in range(4 * 7)]

    raw_df = pd.DataFrame(
        {
            "ID": ids,
            "DIA": days,
            "PAIS_ES": countries_es,
            "PAIS_CA": countries_ca,
            "VAL": values,
        }
    )

    substituted_ids_df = pd.DataFrame(
        {"ID": ids, "ID_DAY": day_ids, "ID_COUNTRY": country_ids, "VAL": values}
    )

    return raw_df, substituted_ids_df


def test_renaming():
    raw_df, normalized_df = get_dataframe_3()

    weekday_df = pd.DataFrame(
        {
            "ID": list(range(1, 8)),
            "DAY": [
                "Lunes",
                "Martes",
                "Miercoles",
                "Jueves",
                "Viernes",
                "Sábado",
                "Domingo",
            ],
        }
    )

    country_df = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4],
            "COUNTRY_ES": ["España", "Portugal", "Francia", "Alemania"],
            "COUNTRY_CA": ["Espanya", "Portugal", "França", "Alemanya"],
        }
    )

    expected_dataframes = {
        "FAC_TABLE": normalized_df,
        "DIM_DAY": weekday_df,
        "DIM_COUNTRY": country_df,
    }

    normalization_handler = NormalizationHandler(
        NamedDataFrame(df=raw_df, name="FAC_TABLE")
    )

    normalization_handler.extract_new_table(
        "FAC_TABLE", {"PAIS_ES", "PAIS_CA"}, "DIM_PAIS"
    )

    normalization_handler.extract_new_table("FAC_TABLE", {"DIA"}, "DIM_DIA")

    normalization_handler.rename_table("DIM_PAIS", "DIM_COUNTRY")
    normalization_handler.rename_column("DIM_COUNTRY", "PAIS_ES", "COUNTRY_ES")
    normalization_handler.rename_column("DIM_COUNTRY", "PAIS_CA", "COUNTRY_CA")

    normalization_handler.rename_table("DIM_DIA", "DIM_DAY")
    normalization_handler.rename_column("DIM_DAY", "DIA", "DAY")

    normalization_handler.rename_column("FAC_TABLE", "ID_PAIS", "ID_COUNTRY")
    normalization_handler.rename_column("FAC_TABLE", "ID_DIA", "ID_DAY")

    output_dataframes = normalization_handler.get_state()

    assert_dataframe_dict_equality(output_dataframes, expected_dataframes)


def test_pushable_dataframe_extraction():
    raw_df, _ = get_dataframe_3()

    normalization_handler = NormalizationHandler(
        NamedDataFrame(df=raw_df, name="FAC_TABLE")
    )

    normalization_handler.extract_new_table(
        "FAC_TABLE", {"PAIS_ES", "PAIS_CA"}, "DIM_PAIS"
    )

    normalization_handler.extract_new_table("FAC_TABLE", {"DIA"}, "DIM_DIA")

    normalization_handler.rename_table("DIM_PAIS", "DIM_COUNTRY")
    normalization_handler.rename_column("DIM_COUNTRY", "PAIS_ES", "TX_ES")
    normalization_handler.rename_column("DIM_COUNTRY", "PAIS_CA", "TX_CA")

    normalization_handler.rename_table("DIM_DIA", "DIM_DAY")
    normalization_handler.rename_column("DIM_DAY", "DIA", "TX_ES")

    normalization_handler.rename_column("FAC_TABLE", "ID_PAIS", "ID_COUNTRY")
    normalization_handler.rename_column("FAC_TABLE", "ID_DIA", "ID_DAY")

    fks = normalization_handler.get_foreign_keys()
    pdfs = normalization_handler.to_pushable_dataframes()

    expected_tables = {"DIM_COUNTRY", "DIM_DAY", "FAC_TABLE"}

    assert set(pdf.get_name() for pdf in pdfs) == expected_tables

    for pdf in pdfs:
        print("TABLE NAME:")
        print(pdf.get_name())

        print("COLTYPES:")
        print(pdf.get_coltypes())

        print("FOREIGN KEYS:")
        print(fks[pdf.get_name()])

        print("DATAFRAME:")
        print(pdf.get_dataframe())
