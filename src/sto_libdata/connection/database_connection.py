from dotenv import load_dotenv
import os
from sqlalchemy import Engine, create_engine

def _init_engine(host, database, user, password)->Engine:

    driver="ODBC+DRIVER+18+for+SQL+Server"

    connection_string_sqlalchemy = f"mssql+pyodbc://{user}:{password}@{host}/{database}?driver={driver}"

    return create_engine(connection_string_sqlalchemy)

def init_engine()->Engine:
    load_dotenv()

    host = os.getenv('DB_HOST')
    database = os.getenv('DB_DTBS')

    user = os.getenv('DB_USER')
    password = os.getenv('DB_PSWD')

    return _init_engine(host, database, user, password)

