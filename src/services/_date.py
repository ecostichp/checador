from datetime import (
    date,
    timedelta,
)
from ..contracts.services import _Contract_Date
from ..domain_data import WEEK_PERIOD_END
from ..settings import CONFIG

class DateService(_Contract_Date):

    def __init__(
        self,
    ) -> None:

        # Obtención de la fecha del día de hoy
        self.today = CONFIG.TODAY

        # Inicialización de valores de fechas
        self.most_recent_available_date = self.today - timedelta(days= 1)
        self.current_year = self.most_recent_available_date.year
        self.current_month = self.most_recent_available_date.month
        ( self.month_start_date, self.month_end_date ) = self._compute_month_start_and_end_dates()
        ( self.first_week_start_date, self.first_week_end_date ) = self._compute_first_week_start_and_end_dates()

    def _compute_month_start_and_end_dates(
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

    def _compute_first_week_start_and_end_dates(
        self,
    ) -> tuple[date, date]:

        # Obtención del día numérico de la semana
        weekday = self.month_start_date.weekday()

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
        first_week_end_date = self.month_start_date + timedelta(days= days_difference + offset)

        # Obtención del primer día del ciclo semanal
        first_week_start_date = first_week_end_date - timedelta(days= 6)

        return (first_week_start_date, first_week_end_date)
