import pandas as pd
from typing import NamedTuple

type DataFrame = pd.DataFrame

class NamedDataFrame(NamedTuple):
    """
    Args:
        df: The dataframe to normalize. It will be transformed and referred
            to as the "main" dataframe in this instance.
        name: The name that the SQL table corresponding to the main
            dataframe should have in the database.
    """
    name: str
    df: DataFrame

