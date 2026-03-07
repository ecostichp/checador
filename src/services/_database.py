import pandas as pd
from typing import Literal
from ..contracts.services import _Contract_Database
from ..typing.callables import ConnFunction
from ..typing.generics import _T
from ..sql import engine

class _Database(_Contract_Database):

    def load_data_from_query(
        self,
        query: str,
    ) -> pd.DataFrame:

        # Función para obtención de los datos
        load_data: ConnFunction[pd.DataFrame] = lambda conn: pd.read_sql_query(query, conn)
        # Obtención de los datos
        data = self._execute_on_connection(load_data)

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
