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

        # Obtención del año y mes actuales
        current_year = self._main._date.current_year
        current_month = self._main._date.current_month

        # Cálculo del primer día del mes
        month_first_day = date(current_year, current_month, 1)
        # Obtención del último día del ciclo semanal en base al primer día del mes
        week_last_date = self._get_week_last_day(month_first_day)
        # Obtención del primer día del ciclo semanal
        week_first_date = week_last_date - timedelta(days= 6)

        # Asignación de fechas de inicio y final
        start_date = week_first_date
        end_date = week_last_date

        # Creación de esquemas semanales
        self._create_weekly_schemas(start_date, end_date)

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

    def _create_weekly_schemas(
        self,
        start_date: date,
        end_date: date,
    ) -> None:

        # Inicialización de contador de semanas
        weeks_counter = 1

        # Creación cíclica de esquemas
        while True:
            # Creación del esquema
            schema_i = _DateSchema('weekly', start_date, end_date, SCHEMA.WEEKLY.format(**{'n': weeks_counter}))
            # Se añade el esquema
            self._schemas.append(schema_i)
            # Si la fecha actual está dentro del esquema i...
            if self._most_recent_available_date in schema_i:
                # Se finaliza el ciclo
                break
            # Si la fecha actual no está dentro del esquema i...
            else:
                # Incremento de la variable i
                weeks_counter += weeks_counter
                # Uso de nuevas variables de día
                start_date += timedelta(days= 7)
                end_date += timedelta(days= 7)
