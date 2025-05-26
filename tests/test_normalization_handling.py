import pandas as pd

from src.sto_libdata.dataframe_handling.normalization import NormalizationHandler


def assert_dataframe_equality(df1: pd.DataFrame, df2: pd.DataFrame) -> None:

    assert (cols:=set(df1.columns))==set(df2.columns)

    for col in cols:
        assert (df1[col]==df2[col]).all(), f"{col} has different values"

def assert_dataframe_dict_equality(d1: dict[str, pd.DataFrame], d2: dict[str, pd.DataFrame]) -> None:
    assert set(d1.keys()) == set(d2.keys())

    for k in d1.keys():
        assert_dataframe_equality(d1[k], d2[k])

def get_not_normalized_dataframe():
    df = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "TX_ES": ["Juan", "Juan", "Juan", "Juan", "Juan otra vez"]
    })

    return df


def test_table_extraction():
    df = get_not_normalized_dataframe()

    normhandler = NormalizationHandler(df, "MY_FAC_TABLE")

    normhandler.extract_new_table({"TX_ES"}, "NAME")

    expected_fac = pd.DataFrame({
        "ID": [1, 2, 3, 4, 5],
        "CO_INE": [1, 1, 1, 2, 5],
        "VAL": [0.3, 0.3, 0.3, 0.3, 0.3],
        "ID_NAME": [1, 1, 1, 1, 2]
    })

    expected_dim = pd.DataFrame({
        "ID": [1, 2],
        "TX_ES": ["Juan", "Juan otra vez"]
    })

    expected_output = {
        "MY_FAC_TABLE": expected_fac,
        "DIM_NAME": expected_dim
    }

    assert_dataframe_dict_equality(normhandler.get_all(), expected_output)
