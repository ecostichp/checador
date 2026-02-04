import pandas as pd
from sqlalchemy import create_engine
from .utils import (
    PROJECT_NAME,
    path_from_dropbox,
)

# Se define la ruta para los datos en Dropbox
_db_file = f'{PROJECT_NAME}.db'
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

def load_from_database(table_name: str) -> pd.DataFrame:

    # Se abre la conexión a la base de datos
    with engine.connect() as conn, conn.begin():
        # Se carga la tabla en un DataFrame
        data = pd.read_sql_table(table_name, conn)

    # Se cierra la conexión
    engine.dispose()

    return data
