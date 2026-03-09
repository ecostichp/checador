from datetime import datetime
import pandas as pd
from attendance_registry import Assistance
from ..constants import COLUMN
from ..contracts.services import _Contract_Attendance
from ..domain_data import DEVICE_SERIAL_NUMBER
from ..resources import DataToSave
from ..typing import (
    ColumnAssignation,
    DataTypeOrNone,
)
from ..typing.callables import SeriesApply
from ..typing.literals import Devices

class _Attendance(_Contract_Attendance):

    def __init__(
        self,
    ) -> None:

        # Creación de instancia
        self._registry = Assistance[Devices](
            {
                'csl': DEVICE_SERIAL_NUMBER.CSL,
                'sjc': DEVICE_SERIAL_NUMBER.SJC,
            },
        )

    def get_warehouse_records_from_api(
        self,
        warehouse_name: str,
        last_date_saved: datetime,
    ) -> DataTypeOrNone[DataToSave]:

        # Obtención de los datos desde la API
        data_i = self._get_from_api(last_date_saved, warehouse_name)

        # Si existen datos obtenidos del dispositivo desde la API...
        if len(data_i):
            # Obtención de la fecha más actual de los nuevos datos del dispositivo
            max_found_datetime = self._get_datetime_from_last_recent_record(data_i)
            # Procesamiento de los datos
            processed_data = self._process_data_to_save(data_i)
            # Construcción del objeto de datos a guardar
            data_to_save = DataToSave(
                data= processed_data,
                max_found_datetime= max_found_datetime,
                warehouse_name= warehouse_name,
            )

            return data_to_save

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

    def _get_datetime_from_last_recent_record(
        self,
        data: pd.DataFrame,
    ) -> str:

        # Obtención del valor más alto en fecha
        max_found_value = str(data[COLUMN.REGISTRY_TIME].max())

        return max_found_value

    def _process_data_to_save(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

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

        return data_to_save
