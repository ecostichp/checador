from typing import Any
import pandas as pd
from pandas._typing import AstypeArg
from sqlalchemy import (
    create_engine,
    text,
)
from .utils import path_from_dropbox
from ._settings import SELECTED_DATABASE

# Se define la ruta para los datos en Dropbox
_db_file = f'{SELECTED_DATABASE}.db'
_db_file_path_str = path_from_dropbox(_db_file)

# Se crea el objeto engine para trabajarlo con los metodos de pandas
engine = create_engine(f'sqlite:///{_db_file_path_str}')

def save_on_database(data: pd.DataFrame, table_name: str) -> None:

    # Se abre la conexión a la base de datos
    with engine.connect() as conn, conn.begin():
        # Se guardan los datos del DataFrame en la tabla
        data.to_sql(
            table_name,
            index= False,
            con= conn,
            if_exists= 'replace',
        )

    # Se cierra la conexión
    engine.dispose()

def load_from_database(table_name: str, dtype: AstypeArg = {}) -> pd.DataFrame:

    # Se abre la conexión a la base de datos
    with engine.connect() as conn, conn.begin():
        # Se carga la tabla en un DataFrame
        data = pd.read_sql_query(f'SELECT * FROM {table_name}', conn, dtype= dtype)

    # Se cierra la conexión
    engine.dispose()

    return data

def execute_query(query: str, /, commit: bool = False) -> Any:

    # Se compila el código provisto
    compiled_query = text(query)

    # Se abre la conexión a la base de datos
    with engine.connect() as conn, conn.begin():

        # Ejecución en la base de datos
        result = conn.execute(compiled_query)

        # Si se especificó commit, se realiza éste
        if commit:
            conn.commit()

    return result

def get_value(table_name: str, column: str, condition: str) -> Any:

    # Construcción del query
    query = (
        f"""
        SELECT
            {column}
        FROM {table_name}
        WHERE {condition}
        ;
        """
    )

    # Obtención del valor
    [ ( value, ) ] = execute_query(query)

    return value
