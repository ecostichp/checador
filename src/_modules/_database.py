import pandas as pd
from typing import Literal
from .._core import (
    _CoreRegistryProcessing,
    _Interface_Database,
)
from .._typing import (
    _T,
    ConnFunction,
)
from ..sql import engine

class _Database(_Interface_Database):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def load_data_from_query(
        self,
        query: str,
    ) -> pd.DataFrame:

        # Función para obtención de los datos
        fn: ConnFunction[pd.DataFrame] = lambda conn: pd.read_sql_query(query, conn)
        # Obtención de los datos
        data = self._execute_on_connection(fn)

        return data

    def save_in_database(
        self,
        data: pd.DataFrame,
        table_name: str,
        if_exists: Literal['fail', 'replace', 'append'],
    ) -> None:

        # Creación de función
        save_fn: ConnFunction[pd.DataFrame] = (
            lambda conn: (
                data
                .to_sql(
                    table_name,
                    index= False,
                    con= conn,
                    if_exists= if_exists,
                )
            )
        )

        # Se guarda el DataFrame
        self._execute_on_connection(save_fn)

    def _execute_on_connection(
        self,
        fn: ConnFunction[_T]
    ) -> _T:

        # Se abre la conexión a la base de datos
        with engine.connect() as conn, conn.begin():
            # Ejecución de la función provista
            result = fn(conn)

        return result
