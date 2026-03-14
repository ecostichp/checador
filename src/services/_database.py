from datetime import (
    date,
    datetime,
    timedelta,
)
import pandas as pd
from typing import Literal
from ..constants import (
    COLUMN,
    COMMON_ARGS,
)
from ..contracts.services import _Contract_Database
from ..settings import DATABASE
from ..templates.queries import QUERY
from ..typing.callables import ConnFunction
from ..typing.generics import _T
from ..typing.misc import RecordsLastDates
from ..sql import (
    execute_query,
    get_value,
    load_from_database,
    engine,
)

class _Database(_Contract_Database):

    def load_assistance_records(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:

        # Construcción del query para leer la tabla
        query = (
            QUERY.GET_RECORDS_IN_DATE_RANGE
            .format(
                **{
                    COMMON_ARGS.TABLE_NAME: DATABASE.TABLE.ASSISTANCE_RECORDS,
                    COMMON_ARGS.REGISTRY_TIME: COLUMN.REGISTRY_TIME,
                    COMMON_ARGS.START_DATE: start_date,
                    COMMON_ARGS.END_DATE: end_date,
                }
            )
        )

        # Obtención de los datos desde la tabla de la base de datos
        data = self.load_data_from_query(query)

        return data

    def load_holidays(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        holidays = load_from_database(
            DATABASE.TABLE.HOLIDAYS,
            {
                COLUMN.HOLIDAY_NAME: 'string[python]',
                COLUMN.HOLIDAY_DATE: 'datetime64[ns]',
            }
        )

        return holidays

    def load_schedules(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        schedules = load_from_database(
            DATABASE.TABLE.SCHEDULES,
            # Conversión de tipos de dato ya que SQLite no soporta INTERVAL
            {
                COLUMN.WEEKDAY: 'uint8',
                COLUMN.START_SCHEDULE: 'timedelta64[ns]',
                COLUMN.END_SCHEDULE: 'timedelta64[ns]',
            }
        )

        return schedules

    def load_schedule_offsets(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        schedule_offsets = load_from_database(
            DATABASE.TABLE.SCHEDULE_OFFSETS,
            # Conversión de tipos de dato ya que SQLite no soporta INTERVAL
            {
                COLUMN.USER_ID: 'uint16',
                COLUMN.WEEKDAY: 'uint8',
                COLUMN.START_OFFSET: 'timedelta64[ns]',
                COLUMN.END_OFFSET: 'timedelta64[ns]',
            }
        )

        return schedule_offsets

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
                    COMMON_ARGS.TABLE_NAME: DATABASE.TABLE.LAST_UPDATE_DATES,
                    COMMON_ARGS.DATE: max_found_datetime,
                    COMMON_ARGS.DEVICE_NAME: warehouse_i,
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
