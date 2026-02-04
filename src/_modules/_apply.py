from datetime import datetime
import pandas as pd
from .._constants import COLUMN
from .._core import (
    _CoreRegistryProcessing,
    _Interface_Apply,
)
from .._data import HOLIDAYS
from .._typing import UserID

class _Apply(_Interface_Apply):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def count_rest_days(
        self,
        record_row: pd.Series,
    ) -> int:

        # Obtención de los valores
        user_id: UserID = record_row[COLUMN.USER_ID]
        date_start: datetime = record_row[COLUMN.PERMISSION_START]
        date_end: datetime = record_row[COLUMN.PERMISSION_END]

        # Obtención de los datos de días de descanso
        rest_days = self._main._get_user_rest_days(user_id)

        # Obtención del total de días
        total = (
            # Creación de rango de fecha
            pd.date_range(date_start, date_end)
            # Obtención del día de la semana en número
            .weekday
            # Validación de si se encuentra en los días de descanso
            .isin(rest_days)
            # Suma de los días encontrados
            .sum()
        )

        return total

    def count_holidays(
        self,
        record_row: pd.Series,
    ) -> int:

        # Obtención de valores
        date_start: datetime = record_row[COLUMN.PERMISSION_START]
        date_end: datetime = record_row[COLUMN.PERMISSION_END]

        return (
            # Creación del rango de fecha
            pd.date_range(date_start, date_end)
            # Se converten los valores a cadena de texto
            .astype('string[python]')
            # Validación de días encontrados en festivos
            .isin(HOLIDAYS)
            # Suma de los días encontrados
            .sum()
        )

    def count_vacation_days(
        self,
        record_row: pd.Series,
    ) -> int:

        # Obtención de valores
        date_start: datetime = record_row[COLUMN.PERMISSION_START]
        date_end: datetime = record_row[COLUMN.PERMISSION_END]
        rest_days: int = record_row[COLUMN.REST_DAYS]
        holidays: int = record_row[COLUMN.HOLIDAYS]

        # Creación de rango de fecha
        date_range = pd.date_range(date_start, date_end)

        # Obtención de total de vacaciones
        total = len(date_range) - rest_days - holidays

        return total
