import pandas as pd

from src.sto_libdata.dataframe_handling.dataframe_handler import DataFrameHandler
from src.sto_libdata.dataframe_handling.pushable_dataframe import PushableDF
from src.sto_libdata.exceptions.exceptions import NormalizationError

def test_string_duplication1():
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "TX_ES": ["Juan", "Nico", "Álex", "Frank", "Nuevos"]
    })

    handler = DataFrameHandler()
    coltypes = handler.infer_SQL_types(df)

    name = "MY_DF"


    pdf = PushableDF(
        df,
        name,
        coltypes
    )

    handler.assert_normalized(pdf)


def test_string_duplication2():
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "TX_ES": ["Juan", "Juan", "Juan", "Juan", "Juan otra vez"]
    })

    handler = DataFrameHandler()
    coltypes = handler.infer_SQL_types(df)

    name = "MY_DF"


    pdf = PushableDF(
        df,
        name,
        coltypes
    )

    try:
        handler.assert_normalized(pdf)
        raise AssertionError
    except NormalizationError as e:
        assert "TX_ES" in str(e)
