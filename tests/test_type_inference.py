import pandas as pd
import numpy as np
from sqlalchemy import CHAR, DATE, DATETIME, Boolean, Float, Integer, String
from sto_libdata.dataframe_handling.dataframe_handler import DataFrameTypeHandler


def assert_type_comparison(type_dict1, type_dict2):
    # It's impossible to check these types for equality directly
    # https://stackoverflow.com/questions/34787794/sqlalchemy-column-type-comparison

    assert set(type_dict1.keys()) == set(type_dict2.keys())
    for key in type_dict1.keys():
        assert repr(type_dict1[key]) == repr(type_dict2[key]), str(key)


def test_name_inference():
    handler = DataFrameTypeHandler()


    mock_df = pd.DataFrame({
        "ID": [1, 2, 3, 4],
        "TX_ES": ["Álex", "Juan", "Nico", "Frank"],
        "DS_ES": ["Data architect", "Juan", "Nico", "Frank"],
        "CO_ID": ["031", "472", "100", "036"]
    })

    expected_types = {
        "ID": Integer(),
        "TX_ES": String(10),
        "DS_ES": String(),
        "CO_ID": CHAR(3)
    }

    inferred_types = handler.infer_SQL_types(mock_df)

    assert_type_comparison(expected_types, inferred_types)

def test_dtype_inference():
    handler = DataFrameTypeHandler()
    mock_df = pd.DataFrame({
        "ID": [1, 2, 3, 4],
        "BOOL_NULL": [True, True, False, None],
        "BAD_NAME": [True, True, False, True],
        "BAD_NAME2": [3, 5, 6, 10],
        "BAD_NAME3": ["aaa", "bbb", "ccc", None],
        "BAD_NAME4": [12.4, None, None, None],
    })

    expected_types = {
        "ID": Integer(),
        "BOOL_NULL": Boolean(),
        "BAD_NAME": Boolean(),
        "BAD_NAME2": Integer(),
        "BAD_NAME3": CHAR(3),
        "BAD_NAME4": Float(),
    }

    inferred_types = handler.infer_SQL_types(mock_df)
    
    assert_type_comparison(expected_types, inferred_types)


def test_value_inference():
    handler = DataFrameTypeHandler()
    mock_df = pd.DataFrame({
        "BAD_NAME1": ["Port", "Esp", "ALEMANIA"],
        "BAD_NAME2": [np.datetime64("2000-01-01"), np.datetime64("2001-01-01"), np.datetime64("2002-01-01")],
        "BAD_NAME3": [np.datetime64("2000-01-01 00:00:01"), np.datetime64("2000-01-01 00:01:00"), np.datetime64("2000-01-01 01:00:00")],
    })

    expected_types = {
        "BAD_NAME1": String(16),
        "BAD_NAME2": DATE(),
        "BAD_NAME3": DATETIME(),
    }

    inferred_types = handler.infer_SQL_types(mock_df)
    
    assert_type_comparison(expected_types, inferred_types)


