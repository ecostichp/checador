import re
import pandas as pd
from datetime import (
    date,
    datetime,
    timedelta,
)
from attendance_registry import Assistance
from .._constants import (
    COLUMN,
    DATABASE,
    WAREHOUSES,
)
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Update,
)
from .._data import DEVICE_SERIAL_NUMBER
from .._templates import QUERY
from .._typing import (
    ColumnAssignation,
    Devices,
    SeriesApply,
)
from ..sql import execute_query

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

    def _get_last_date_saved(
        self,
    ) -> date:

        # Patrón para encontrar fecha
        find_date_pattern = r'\w{3}(\d{8})'
        # Patrón para encontrar valores de fecha
        find_date_values_pattern = r'(\d{4})(\d{2})(\d{2})'
        # Patrón para dividir valores de fecha
        delimit_date_values_pattern = r'\1-\2-\3'

        # Construcción de query para lectura de datos
        query = (
            QUERY.GET_EXISTING_LAST_DATE
            .format(
                **{
                    'id_column': COLUMN.ID,
                    'table_name': DATABASE.TABLE.ASSISTANCE_RECORDS,
                    'time_column': COLUMN.REGISTRY_TIME,
                }
            )
        )

        # Obtención de los datos
        data = self._main._database.load_data_from_query(query)

        # Lectura de registro
        last_saved_id: str = (
            data
            .at[0, COLUMN.ID]
        )

        # Obtención de un valor para usarse como fecha
        string_date = (
            re.sub(
                find_date_values_pattern,
                delimit_date_values_pattern,
                (
                    re
                    .match(find_date_pattern, last_saved_id)
                    .group(1)
                )
            )
        )

        # Obtención del valor en formato fecha
        last_date_saved =  date.fromisoformat(string_date)

        return last_date_saved

    def _get_records(
        self,
    ) -> pd.DataFrame:

        # Construcción de las fechas de rango de inicio en fin en cadena de texto
        string_start_date = str( self._last_date_saved + timedelta(days= 1) )
        string_end_date = str( self._main._date.most_recent_available_date )

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

        return (
            # Obtención de los registros desde la API de HikVision
            self._registry.get_daily_attendance(
                (string_start_date, string_end_date)
            )
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

    def _save_records(
        self,
        records: pd.DataFrame,
    ) -> None:

        # Actualización de hora de última actualización
        self._set_last_update(records)

        # Se guardan los registros en la base de datos
        self._main._database.save_in_database(
            records,
            DATABASE.TABLE.ASSISTANCE_RECORDS,
            'append',
        )

    def _update_records(
        self,
    ) -> None:

        # Lectura del último día de actualización en la base de datos
        self._last_date_saved = self._get_last_date_saved()

        # Si existen fechas cuyos registros no han sido guardados...
        if self._last_date_saved < self._main._date.most_recent_available_date:
            # Obtención de los registros desde la API de HikVision
            records = self._get_records()
            # Se guardan los datos en la tabla de la base de datos local
            self._save_records(records)

            print('Se guardaron los datos')

    def _set_last_update(
        self,
        data: pd.DataFrame,
    ) -> None:

        # Función para obtennción de fecha más reciente de último registro en dispositivo por almacén
        def _get_last_date_on_warehouse(warehouse_name: str) -> datetime:
            return (
                data.pipe(
                    lambda df_: (
                        # Filtro por los registros que pertenecen al almacén
                        df_[df_[COLUMN.DEVICE] == warehouse_name]
                    )
                )
                # Selección de la columna de fecha de registro en el dispositivo
                [COLUMN.REGISTRY_TIME]
                # Obtención del valor de fecha más reciente
                .max()
            )

        # Se actualiza la fecha de última hora de actualización por cada almacén
        for warehouse_i in WAREHOUSES:
            # Construcción del query
            query = (
                QUERY.UPDATE_LAST_UPDATE_IN_RECORDS
                .format(**{
                    'table_name': DATABASE.TABLE.LAST_UPDATE_DATES,
                    'date': _get_last_date_on_warehouse(warehouse_i),
                    'name': warehouse_i,
                })
            )

            # Ejecución del comando
            execute_query(query, commit= True)
