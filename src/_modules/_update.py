import pandas as pd
from datetime import datetime
from attendance_registry import Assistance
from .._constants import (
    COLUMN,
    DATABASE,
    WAREHOUSES,
)
from .._data import DEVICE_SERIAL_NUMBER
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Update,
)
from .._templates import QUERY
from .._typing import (
    ColumnAssignation,
    Devices,
    SeriesApply,
)
from ..sql import (
    execute_query,
    get_value,
)

class _Update(_Interface_Update):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

        # Creación de instancia
        self._registry = Assistance[Devices](
            {
                'csl': DEVICE_SERIAL_NUMBER.CSL,
                'sjc': DEVICE_SERIAL_NUMBER.SJC,
            }
        )

        # Actualización de registros
        self._update_records()

    def _update_records(
        self,
    ) -> None:

        # Inicialización de lista de DataFrames de datos obtenidos desde la API
        all_data_from_api: list[pd.DataFrame] = []
        # Inicialización de última fecha y hora de registros por almacén obtenidos desde la API
        records_last_dates: list[tuple[str, str]] = []

        for warehouse_i in WAREHOUSES:
            # Obtención de la última fecha de actualización de los datos
            last_date_saved = datetime.fromisoformat(
                get_value(
                    DATABASE.TABLE.LAST_UPDATE_DATES,
                    'date',
                    f"name = '{warehouse_i}'",
                )
            )
            # Obtención de los datos desde la API
            data_i = self._get_from_api(last_date_saved, warehouse_i)

            # Si existen datos obtenidos del dispositivo desde la API...
            if len(data_i):
                # Obtención de la fecha más actual de los nuevos datos del dispositivo
                max_found_datetime = self.get_datetime_from_last_recent_record(data_i)
                # Se añade el valor junto con el nombre del almacén para guardarse
                records_last_dates.append( (warehouse_i, max_found_datetime) )

                # Se añaden éstos a los datos totales
                all_data_from_api.append(data_i)

        # Si existen datos obtenidos desde la API...
        if all_data_from_api:
            # Construcción de los datos a guardar
            data_to_save = (
                # Concatenación de todos los DataFrames
                pd.concat(all_data_from_api)
                # Se ordenan los datos por fecha y hora de registro
                .sort_values(COLUMN.REGISTRY_TIME)
            )
            # Se guardan los datos en la base de datos
            self._save_on_database(data_to_save)
            # Actualización de fechas guardadas
            self._update_last_update_dates(records_last_dates)

    def _update_last_update_dates(
        self,
        max_dates: list[tuple[str, str]],
    ) -> None:

        # Iteración por cada par almacén/valor
        for ( warehouse_i, max_found_datetime ) in max_dates:
            # Construcción del query
            query = (
                QUERY.UPDATE_LAST_UPDATE_IN_RECORDS
                .format(**{
                    'table_name': DATABASE.TABLE.LAST_UPDATE_DATES,
                    'date': max_found_datetime,
                    'name': warehouse_i,
                })
            )

            # Ejecución del comando
            execute_query(query, commit= True)

    def get_datetime_from_last_recent_record(
        self,
        data: pd.DataFrame,
    ) -> str:

        # Obtención del valor más alto en fecha
        max_found_value = str(data[COLUMN.REGISTRY_TIME].max())

        return max_found_value

    def _get_from_api(
        self,
        last_date_saved: datetime,
        device: str,
    ) -> pd.DataFrame:

        # Definición de fecha y hora de inicio y final
        start_date = last_date_saved
        last_date = datetime.now()

        # Definición de rango de fecha y hora para búsqueda
        date_range = (start_date, last_date)

        # Obtención de los datos desde la API
        data = self._registry.get_daily_attendance(date_range, device)

        return data

    def _save_on_database(
        self,
        data: pd.DataFrame,
    ) -> None:

        # Conversión de fecha a código
        to_code: SeriesApply[str] = (
            lambda value: (
                value
                .replace('-', '')
                .replace(' ', '')
                .replace(':', '')
            )
        )

        # Asignación de columna de ID
        id_assignation: ColumnAssignation = {
            COLUMN.ID: (
                lambda df: (
                    (
                        df[COLUMN.DEVICE]
                        .astype(str)
                    ) + (
                        df[COLUMN.REGISTRY_TIME]
                        .astype(str)
                        .apply(to_code)
                    )
                )
            )
        }

        data_to_save = (
            data
            # Reasignación de nombres de columnas
            .rename(
                columns= {
                    'user_id': COLUMN.USER_ID,
                    'name': COLUMN.NAME,
                    'time': COLUMN.REGISTRY_TIME,
                    'status': COLUMN.REGISTRY_TYPE,
                    'device': COLUMN.DEVICE,
                }
            )
            # Asignación de columna de ID
            .assign(**id_assignation)
            # Asignación de tipos de dato
            .pipe(self._main._processing.assign_dtypes)
            # Selección de columnas
            [[
                COLUMN.ID,
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.REGISTRY_TIME,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
            ]]
        )

        # Se guardan los datos en la tabla de la base de datos local
        self._save_records(data_to_save)

    def _save_records(
        self,
        records: pd.DataFrame,
    ) -> None:

        # Se guardan los registros en la base de datos
        self._main._database.save_in_database(
            records,
            DATABASE.TABLE.ASSISTANCE_RECORDS,
            'append',
        )
