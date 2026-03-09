from datetime import (
    datetime,
    timedelta,
)
import pandas as pd
from typing import Literal
from ..contracts.services import _Contract_Database
from ..settings import DATABASE
from ..typing.callables import ConnFunction
from ..typing.generics import _T
from ..typing.misc import RecordsLastDates
from ..sql import (
    execute_query,
    get_value,
    engine,
)
from ..constants import ARGS
from ..templates.queries import QUERY

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

    def get_records_last_date_saved(
        self,
        warehouse_name: str,
    ) -> datetime:

            # Obtención de la última fecha de actualización de los datos
            last_date_saved = (
                datetime.fromisoformat(
                    get_value(
                        DATABASE.TABLE.LAST_UPDATE_DATES,
                        'date',
                        f"name = '{warehouse_name}'",
                    )
                )
                # Se incrementa 1 segundo para evitar obtener nuevamente el último resultado ya guardado
                + timedelta(seconds= 1)
            )

            return last_date_saved

    def update_last_update_dates(
        self,
        max_dates: RecordsLastDates,
    ) -> None:

        # Iteración por cada par almacén/valor
        for ( warehouse_i, max_found_datetime ) in max_dates:
            # Construcción del query
            query = (
                QUERY.UPDATE_LAST_UPDATE_IN_RECORDS
                .format(**{
                    ARGS.TABLE_NAME: DATABASE.TABLE.LAST_UPDATE_DATES,
                    ARGS.DATE: max_found_datetime,
                    ARGS.DEVICE_NAME: warehouse_i,
                })
            )

            # Ejecución del comando
            execute_query(query, commit= True)

    def _execute_on_connection(
        self,
        fn: ConnFunction[_T]
    ) -> _T:

        # Se abre la conexión a la base de datos
        with engine.connect() as conn, conn.begin():
            # Ejecución de la función provista
            result = fn(conn)

        return result
