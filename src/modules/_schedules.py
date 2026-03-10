import pandas as pd
from ..constants import COLUMN
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Schedules,
)
from ..domain_data import (
    REST_DAYS,
    USER_DEFAULT_REST_DAYS,
)
from ..typing.aliases import UserID
from ..typing.interfaces import HorizontalSeries
from ..typing.literals import NumericWeekday

class _Schedules(_Interface_Schedules):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def count_rest_days(
        self,
        record_row: HorizontalSeries,
    ) -> int:

        # Obtención de los valores
        user_id = record_row[COLUMN.USER_ID]
        date_start = record_row[COLUMN.PERMISSION_START]
        date_end = record_row[COLUMN.PERMISSION_END]

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
        record_row: HorizontalSeries,
    ) -> int:

        # Obtención de valores
        date_start = record_row[COLUMN.PERMISSION_START]
        date_end = record_row[COLUMN.PERMISSION_END]

        return (
            # Creación del rango de fecha
            pd.date_range(date_start, date_end)
            # # Validación de días encontrados en festivos
            .isin(self._main._data.holidays[COLUMN.HOLIDAY_DATE])
            # Suma de los días encontrados
            .sum()
        )

    def count_vacation_days(
        self,
        record_row: HorizontalSeries,
    ) -> int:

        # Obtención de valores
        date_start = record_row[COLUMN.PERMISSION_START]
        date_end = record_row[COLUMN.PERMISSION_END]
        rest_days = record_row[COLUMN.REST_DAYS_COUNT]
        holidays = record_row[COLUMN.HOLIDAYS_COUNT]

        # Creación de rango de fecha
        date_range = pd.date_range(date_start, date_end)

        # Obtención de total de vacaciones
        total = len(date_range) - rest_days - holidays

        return total

    def _get_user_rest_days(
        self,
        user_id: UserID,
    ) -> list[NumericWeekday]:
        """
        ### Obtención de días de descanso
        Este método obtiene los días de decanso de un usuario en base a su ID provista.
        En caso de no encontrarse un dato especificado para este usuario se utiliza un
        valor prestablecido.
        """

        # Obtención de los datos de días de descanso
        rest_days = REST_DAYS.get(user_id, USER_DEFAULT_REST_DAYS)

        return rest_days
