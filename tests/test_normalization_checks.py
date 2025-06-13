import pytest
import pandas as pd

from sto_libdata.dataframe_handling.dataframe_handler import DataFrameTypeHandler
from sto_libdata.dataframe_handling.pushable_dataframe import PushableDF
from sto_libdata.exceptions.exceptions import NormalizationError

def test_string_duplication1():
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "TX_ES": ["Juan", "Nico", "Álex", "Frank", "Nuevos"]
    })

    handler = DataFrameTypeHandler()
    coltypes = handler.infer_SQL_types(df)

    name = "MY_DF"


    pdf = PushableDF(
        df,
        name,
        coltypes
    )


    handler.assert_normalized(pdf.get_dataframe(), pdf.get_coltypes(), pdf.get_name())


def test_string_duplication2():
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "TX_ES": ["Juan", "Juan", "Juan", "Juan", "Juan otra vez"]
    })

    handler = DataFrameTypeHandler()
    coltypes = handler.infer_SQL_types(df)

    name = "MY_DF"

    pdf = PushableDF(
        df,
        name,
        coltypes
    )

    with pytest.raises(NormalizationError) as excinfo:
        handler.assert_normalized(pdf.get_dataframe(), pdf.get_coltypes(), pdf.get_name())

    assert "TX_ES" in str(excinfo.value)
