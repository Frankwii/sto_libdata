import pandas as pd
from sqlalchemy import ForeignKey, Integer, String
from sto_libdata import PushConfig, PushableDF, ConnectionHandler, init_engine


df_test1 = pd.DataFrame({"ID": [1,2,3], "FK": [3,2,2], "NAME": ["Uno", "Dos", "Tres"]})

coltypes1 = {"ID": Integer, "FK": Integer, "NAME": String}

df_test2 = pd.DataFrame({"ID": [1,2,3], "NAME": ["One", "Two", "Three"]})

coltypes2 = {"ID": Integer, "NAME": String}

pdf1 = PushableDF(df_test1, "TMP_TEST5", coltypes1, foreign_keys={"FK" : ForeignKey("TMP_TEST4.ID")})
pdf2 = PushableDF(df_test2, "TMP_TEST4", coltypes2)

conf1 = (pdf1, PushConfig(if_exists="fail"))
conf2 = (pdf2, PushConfig(if_exists="fail"))

engine = init_engine()

handler = ConnectionHandler(engine.connect(), "BSN")

handler.push_tables(pdf1)
