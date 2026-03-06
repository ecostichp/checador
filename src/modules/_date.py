from datetime import (
    date,
    timedelta,
)
from ..domain_data import WEEK_PERIOD_END
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Date,
)
from ..settings import CONFIG

class _Date(_Interface_Date):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

        # Obtención de la fecha del día de hoy
        self.today = CONFIG.TODAY

        # Inicialización de valores de fechas
        self.most_recent_available_date = self.today - timedelta(days= 1)
        self.current_year = self.most_recent_available_date.year
        self.current_month = self.most_recent_available_date.month
        ( self.month_start_date, self.month_end_date ) = self._compute_month_first_and_last_day()

    def get_week_last_day(
        self,
        date_value: date,
    ) -> date:
        """
        ### último día de la semana laboral
        Este método obtiene y retorna el último día de la semana laboral en curso.
        """

        # Obtención del día numérico de la semana
        weekday = date_value.weekday()

        # Si el día de la semana está por debajo o en el día de término...
        if weekday <= WEEK_PERIOD_END:
            # Asignación de desfase para cálculo
            offset = 0
        # Si el día de la semana está por encima del día de término...
        else:
            # Asignación de desfase para cálculo
            offset = 7

        # Obtención de diferencia de días
        days_difference = WEEK_PERIOD_END - weekday

        # Cálculo del último día de la semana
        last_day = date_value + timedelta(days= days_difference + offset)

        return last_day

    def _compute_month_first_and_last_day(
        self,
    ) -> tuple[date, date]:

        # Si el mes actual no es Diciembre...
        if self.current_month < 12:
            # Se utiliza el año actual
            year = self.current_year
            # Se utiliza el mes siguiente al actual
            month = self.current_month + 1
        # Si el mes actual es Diciembre
        else:
            # Se utiliza el año siguiente al actual
            year = self.current_year + 1
            # Se utiliza el mes de Enero
            month = 1

        # Construcción de la fecha del primer día del mes actual
        month_start_day = date(self.current_year, self.current_month, 1)
        # Construcción de la fecha del último día del mes actual
        month_end_date = date(year, month, 1) - timedelta(days= 1)

        return (month_start_day, month_end_date)
