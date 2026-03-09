import pandas as pd
from ..constants import (
    COLUMN,
    DATABASE,
    WAREHOUSES,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Update,
)
from ..typing import ColumnAssignation
from ..typing.callables import SeriesApply
from ..typing.misc import RecordsLastDates

class _Update(_Interface_Update):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

        # Actualización de registros
        self._update_records()

    def _update_records(
        self,
    ) -> None:

        # Inicialización de lista de datos a guardar en la base de datos
        all_data: list[pd.DataFrame] = []
        # Valores de fechas a actualizar
        date_values_to_update: RecordsLastDates = []

        # Iteración por cada almacén
        for warehouse_i in WAREHOUSES:
            # Obtención del valor de última fecha de actualización
            last_date_saved = self._main._services.database.get_records_last_date_saved(warehouse_i)

            # Obtención de los datos obtenidos desde la API
            data_to_save = self._main._services.attendance.get_warehouse_records_from_api(
                warehouse_i,
                last_date_saved,
            )

            # Si existen datos a guardar...
            if data_to_save:
                # Se añade el DataFrame de datos
                all_data.append(data_to_save.data)
                # Se añade la fecha a actualizar en el registro de almacén
                date_values_to_update.append(
                    (data_to_save.warehouse_name, data_to_save.max_found_datetime)
                )

        # Si existen datos a guardar en la base de datos...
        if all_data:
            # Obtención del DataFrame total de datos
            all_data_to_save = (
                # Concatenación de DataFrames
                pd.concat(all_data)
                # Se ordenan los datos por fecha y hora de registro
                .sort_values(COLUMN.REGISTRY_TIME)
            )

            # Se guardan los datos en la base de datos
            self._save_on_database(all_data_to_save)
            # Actualización de fechas guardadas
            self._main._services.database.update_last_update_dates(date_values_to_update)

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
        self._main._services.database.save_in_database(
            records,
            DATABASE.TABLE.ASSISTANCE_RECORDS,
            'append',
        )
