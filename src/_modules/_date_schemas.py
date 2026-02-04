from datetime import (
    date,
    timedelta,
)
from typing import Iterator
from .._core import (
    _CoreRegistryProcessing,
    _Interface_DateSchemas,
)
from .._data import WEEK_PERIOD_END
from .._modules import _DateSchema
from .._settings import DATE_LIMITS
from .._templates import SCHEMA

class _DateSchemas(_Interface_DateSchemas):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de la instancia principal
        self._main = main

        # Obtención del día de hoy
        self._today = self._main._date.today
        self._most_recent_available_date = self._main._date.most_recent_available_date

        # Inicialización de esquemas
        self._initialize_schemas()

    def min_date(
        self,
    ) -> date:

        # Obtención de las fechas de inicio de todos los esquemas creados
        start_dates = [schema_i.start_date for schema_i in self._schemas]
        # Obtención de la fecha más inicial
        min_start_date = min(start_dates)

        return min_start_date

    def max_date(
        self,
    ) -> date:

        # Obtención de las fechas de término de todos los esquemas creados
        end_dates = [schema_i.end_date for schema_i in self._schemas]
        # Obtención de la fecha más final
        max_end_date = max(end_dates)

        return max_end_date

    def __iter__(
        self,
    ) -> Iterator[_DateSchema]:

        # Iteración por cada esquema creado
        for schema in self._schemas:
            yield schema

    def __repr__(
        self,
    ) -> str:

        # Obtención de las representaciones de los esquemas individuales
        schemas = [ schema for schema in self ]
        # Construcción de la representación del objeto
        repr_ = f'DateSchemas({schemas})'

        return repr_

    def _initialize_schemas(
        self,
    ) -> None:

        # Inicialización de lista de esquemas
        self._schemas: list[_DateSchema] = []
        # Inicialización de los esquemas quincenales
        self._initialize_biweekly_schemas()
        # Inicialización de los esquemas semanales
        self._initialize_weekly_schemas()

    def _initialize_biweekly_schemas(
        self,
    ) -> None:
        """
        ### Inicialización de esquemas quincenales
        Este método inicializa los esquemas quincenales que apliquen para el día en
        curso.
        """

        # Obtención del esquema de la primera quincena
        self._schemas.append( self._first_half() )

        # Si el día es 16 en adelante...
        if self._most_recent_available_date.day >= DATE_LIMITS.SECOND_HALF_MONTH_START:
            # Obtención del esquema de la segunda quincena
            self._schemas.append( self._second_half() )

    def _initialize_weekly_schemas(
        self,
    ) -> None:
        """
        ### Inicialización de esquemas semanales
        Este método inicializa los esquemas semanales que apliquen para el día en
        curso.
        """

        # Obtención de la fecha del último día del ciclo semanal
        week_last_day = self._get_week_last_day()
        # Obtención de la fecha del primer día del ciclo semanal
        week_first_day = week_last_day - timedelta(days= 6)

        # Inicialización de valor de semanas previas
        previous_weeks = 0

        # Obtención de cantidad de semanas completas en el mes
        complete_weeks_in_current_month = week_first_day.day // 7
        # Evaluación de si existe una semana que cruza desde el mes anterior a al mes actual
        current_month_has_started_week = ( self._today - timedelta(days= complete_weeks_in_current_month) ).day > 0
        # Obtención de cantidad de semanas completas en el mes
        weeks_in_current_month = complete_weeks_in_current_month + int( current_month_has_started_week )

        # Si ya existen semanas completas en el mes y se requieren semanas anteriores a la actual...
        if weeks_in_current_month and (week_first_day.month == self._most_recent_available_date.month):
            # Se agregan éstas desde n - x hasta n - 1
            for i in range(weeks_in_current_month):
                # Obtención del esquema de la semana n - i
                schema_i = self._get_n_minus_x_week(weeks_in_current_month, i)
                # Incremento en contador de semanas previas
                previous_weeks += 1
                # Se añade el esquema
                self._schemas.append(schema_i)

        # Si el día de hoy es distinto al inicio de las semanas...
        if self._most_recent_available_date.weekday() != WEEK_PERIOD_END:
            # Obtención del esquema de la semana actual
            self._schemas.append( self._current_week(previous_weeks + 1) )

    def _first_half(
        self,
    ) -> _DateSchema:
        """
        ### Primera quincena
        Construcción del esquema de tiempo que representa la primera quincena del mes
        en curso.
        """

        # Construcción de la fecha de inicio de la primera quincena
        start_date = date(
            self._most_recent_available_date.year,
            self._most_recent_available_date.month,
            1,
        )
        # Construcción de la fecha de término de la primera quincena
        end_date = date(
            self._most_recent_available_date.year,
            self._most_recent_available_date.month,
            DATE_LIMITS.FIRST_HALF_MONTH_END,
        )

        # Creación del esquema
        schema = _DateSchema('biweekly', start_date, end_date, SCHEMA.BIWEEKLY.format(**{'n': 1}))

        return schema

    def _second_half(
        self,
    ) -> _DateSchema:
        """
        ### Segunda quincena
        Construcción del esquema de tiempo que representa la Segunda quincena del mes
        en curso.
        """

        # Construcción de la fecha de inicio de la primera quincena
        start_date = date(
            self._most_recent_available_date.year,
            self._most_recent_available_date.month,
            DATE_LIMITS.SECOND_HALF_MONTH_START,
        )
        # Construcción de la fecha de término de la primera quincena
        end_date = self._get_month_last_day()

        # Creación del esquema
        schema = _DateSchema('biweekly', start_date, end_date, SCHEMA.BIWEEKLY.format(**{'n': 2}))

        return schema

    def _current_week(
        self,
        n: int
    ) -> _DateSchema:
        """
        ### Semana actual
        Construcción del esquema de tiempo que representa la semana actual del mes en
        curso.
        """

        # Fin de la semana
        end_date = self._get_week_last_day()
        # Inicio de la semana
        start_date = end_date - timedelta(days= 6)

        # Creación del esquema
        schema = _DateSchema('weekly', start_date, end_date, SCHEMA.WEEKLY.format(**{'n': n}))

        return schema

    def _get_n_minus_x_week(
        self,
        weeks_in_current_month: int,
        n: int,
    ) -> _DateSchema:
        """
        ### Semana n - x
        Construcción de un esquema de tiempo que representa una eneava semana anterior
        a la semana actual del mes en curso.
        """

        # Fin de la semana
        end_date = self._get_week_last_day() - timedelta(days= 7 * (weeks_in_current_month - n))
        # Inicio de la semana
        start_date = end_date - timedelta(days= 6)

        # Creación del esquema
        schema = _DateSchema('weekly', start_date, end_date, SCHEMA.WEEKLY.format(**{'n': n + 1}))

        return schema

    def _get_month_last_day(
        self,
    ) -> date:
        """
        ### último día del mes
        Este método obtiene y retorna el último día del mes en curso.
        """

        # Si el mes es el último del año...
        if self._most_recent_available_date.month == 12:
            # Se usa el año siguiente
            year = self._most_recent_available_date.year + 1
            # Se usa el mes de Enero
            month = 1
        # Si el mes no es el último del año...
        else:
            # Se usa el año actual
            year = self._most_recent_available_date.year
            # Se usa el siguiente mes al actual
            month = self._most_recent_available_date.month + 1

        # Se usa el primer día del mes
        day = 1

        # Obtención del día anterior de la fecha definida
        month_last_day = date(year, month, day) -  timedelta(days= 1)

        return month_last_day

    def _get_week_last_day(
        self,
    ) -> date:
        """
        ### último día de la semana laboral
        Este método obtiene y retorna el último día de la semana laboral en curso.
        """

        # Obtención del día numérico de la semana
        weekday = self._most_recent_available_date.weekday()

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
        last_day = self._most_recent_available_date + timedelta(days= days_difference + offset)

        return last_day
