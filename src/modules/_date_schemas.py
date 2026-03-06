from datetime import (
    date,
    timedelta,
)
from typing import Iterator
from ..constants import ARGS
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_DateSchemas,
)
from ..resources import _DateSchema
from ..settings import CONFIG
from ..templates import SCHEMA

class _DateSchemas(_Interface_DateSchemas):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de la instancia principal
        self._main = main

        # Obtención del día de hoy
        self._today = self._main._services.date.today
        self._most_recent_available_date = self._main._services.date.most_recent_available_date

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

        # Evaluación de si el segundo período quincenal es el actual
        second_half_is_current = (
            self._most_recent_available_date.day >= CONFIG.DATE_LIMITS.SECOND_HALF_MONTH_START
        )

        # Obtención del esquema de la primera quincena
        schema = self._first_half(not second_half_is_current)
        self._schemas.append(schema)

        if second_half_is_current:
            # Obtención del esquema de la segunda quincena
            schema = self._second_half()
            self._schemas.append(schema)

    def _initialize_weekly_schemas(
        self,
    ) -> None:

        # Obtención del inicio del primer ciclo semanal del mes
        week_first_date = self._main._services.date.first_week_start_date
        # Obtención del fin del primer ciclo semanal del mes
        first_week_end_date = self._main._services.date.first_week_end_date

        # Creación de esquemas semanales
        self._create_weekly_schemas(week_first_date, first_week_end_date)

    def _first_half(
        self,
        current: bool = False,
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
            CONFIG.DATE_LIMITS.FIRST_HALF_MONTH_END,
        )

        # Creación del esquema
        schema = _DateSchema(
            frequency= 'biweekly',
            start_date= start_date,
            end_date= end_date,
            name= SCHEMA.BIWEEKLY.format(**{ARGS.N: 1}),
            current= current,
        )

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
            CONFIG.DATE_LIMITS.SECOND_HALF_MONTH_START,
        )
        # Construcción de la fecha de término de la primera quincena
        end_date = self._get_month_last_day()

        # Creación del esquema
        schema = _DateSchema(
            frequency= 'biweekly',
            start_date= start_date,
            end_date= end_date,
            name= SCHEMA.BIWEEKLY.format(**{ARGS.N: 2}),
            current= True,
        )

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

    def _create_weekly_schemas(
        self,
        week_first_date: date,
        week_last_date: date,
    ) -> None:

        # Obtención de las fechas de inicio y final a usar
        start_date = week_first_date
        end_date = week_last_date

        # Inicialización de contador de semanas
        weeks_counter = 1

        # Creación cíclica de esquemas
        while True:
            # Evaluación de si la semana i es la actual
            week_i_is_current = (
                self._most_recent_available_date >= start_date
                and self._most_recent_available_date <= end_date
            )
            # Creación del esquema
            schema_i = _DateSchema(
                frequency= 'weekly',
                start_date= start_date,
                end_date= end_date,
                name= SCHEMA.WEEKLY.format(**{ARGS.N: weeks_counter}),
                current= week_i_is_current,
            )
            # Se añade el esquema
            self._schemas.append(schema_i)
            # Si la fecha actual está dentro del esquema i...
            if week_i_is_current:
                # Se finaliza el ciclo
                break
            # Si la fecha actual no está dentro del esquema i...
            else:
                # Incremento de la variable i
                weeks_counter += 1
                # Uso de nuevas variables de día
                start_date += timedelta(days= 7)
                end_date += timedelta(days= 7)
