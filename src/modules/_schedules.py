import pandas as pd
import numpy as np
from ..constants import COLUMN
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Schedules,
)
from ..domain_data import (
    REST_DAYS,
    USER_DEFAULT_REST_DAYS,
)
from ..resources import _DateSchema
from ..typing import (
    ColumnAssignation,
    DataFramePipe,
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

    def vacation_days(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:

        def fn(data: pd.DataFrame):

            return (
                data
                # Se filtran los registros que entran dentro del rango de fechas
                .pipe( self.get_permissions_in_date_range(schema) )
                # Se conservan los registros que entren dentro del rango de fecha actual
                .pipe( self.cut_justifications_date_ranges(schema) )
                # Obtención de días de vacaciones desde incidencias
                .pipe(self._main._pipes.total_vacation_days)
                # Agrupamiento por IDs de usuario
                .groupby(COLUMN.USER_ID)
                # Suma de los días de vacaciones
                .agg({
                    COLUMN.VACATION_DAYS_COUNT: 'sum',
                })
                # Reseteo de índice
                .reset_index()
                # Unión con el DataFrame de usuarios
                .pipe(
                    lambda df: (
                        pd.merge(
                            left= (
                                self._main.data.users
                                # Filtro por fechas
                                .pipe( lambda df: df[ df[COLUMN.PAY_FREQUENCY] == schema.frequency ] )
                                # Selección de columnas
                                [[
                                    COLUMN.USER_ID,
                                    COLUMN.NAME,
                                ]]
                            ),
                            right= df,
                            on= COLUMN.USER_ID,
                            how= 'left',
                        )
                    )
                )
                # Reemplazo de todos los valores np.NaN
                .replace({ COLUMN.VACATION_DAYS_COUNT: {np.nan: 0} })
                # Conversión del tipo de dato
                .astype({ COLUMN.VACATION_DAYS_COUNT: 'uint8' })
            )

        return fn

    def get_permissions_in_date_range(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:

        # Obtención de fechas de inicio y fin
        start_date = schema.start_date
        end_date = schema.end_date

        # Función para determinar si la fecha de inicio está dentro del rango
        _start_date_in_range: DataFramePipe = lambda df: ( 
            ( df[COLUMN.PERMISSION_START].dt.date >= start_date )
            & ( df[COLUMN.PERMISSION_START].dt.date <= end_date )
        )

        # Función para determinar si la fecha de término está dentro del rango
        _end_date_in_range: DataFramePipe = lambda df: (
            ( df[COLUMN.PERMISSION_END].dt.date <= end_date )
            & ( df[COLUMN.PERMISSION_END].dt.date >= start_date )
        )

        # Función para determinar si las fechas cruzan el rango desde afuera
        _range_segment_in_range: DataFramePipe = lambda df: (
            ( df[COLUMN.PERMISSION_START].dt.date < start_date )
            & ( df[COLUMN.PERMISSION_END].dt.date > end_date )
        )

        # Función que evalúa si un registro entra dentro del rango de fecha proporcionado
        def fn(data: pd.DataFrame) -> pd.DataFrame:
            return (
                data
                [
                    (
                        _start_date_in_range(data)
                        | _end_date_in_range(data)
                        | _range_segment_in_range(data)
                    )
                ]
            )

        return fn

    def cut_justifications_date_ranges(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:

        """
        ### Cortar rangos
        Este método fabrica una función que trunca las fechas que
        se desbordan fuera del rango de fechas provisto.

        Para que se considere que un registro cruza el rango de fecha, su inicio o
        término de fecha deben encontrarse dentro del rango de fecha del esquema de
        tiempo provisto.

        Ejemplo de entrada:
        >>> data # DataFrame
        >>> #         start         end
        >>> # 1  2026-01-01  2026-01-15
        >>> # 2  2026-01-03  2026-01-25
        >>> # 3  2026-01-10  2026-01-16
        >>> # 4  2026-01-13  2026-01-25
        
        Se truncan los registros, por ejemplo, con el rango provisto entre
        `datetime(2026, 1, 10)` y `datetime(2026, 1, 20)`:
        >>> #         start         end
        >>> # 1  2026-01-10  2026-01-15
        >>> # 2  2026-01-10  2026-01-20
        >>> # 3  2026-01-10  2026-01-16
        >>> # 4  2026-01-13  2026-01-20
        """

        # Obtención de fechas de inicio y fin
        start_date = schema.start_date
        end_date = schema.end_date

        # Columna temporal
        _START_DATE = '_start_date'
        _END_DATE = '_end_date'

        # Función para truncar fechas de inicio y fin que se desborden de los rangos provistos
        cut_ranges : ColumnAssignation = {
            COLUMN.PERMISSION_START: (
                lambda df: (
                    pd.to_datetime(
                        df[_START_DATE]
                        .where(
                                df[COLUMN.PERMISSION_START].dt.date < start_date,
                                df[COLUMN.PERMISSION_START]
                        )
                    )
                )
            ),
            COLUMN.PERMISSION_END: (
                lambda df: (
                    pd.to_datetime(
                        df[_END_DATE]
                        .where(
                                df[COLUMN.PERMISSION_END].dt.date > end_date,
                                df[COLUMN.PERMISSION_END]
                        )
                    )
                )
            ),
        }

        def fn(
            data: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                data
                # Columnas temporales de referencia
                .assign( **{_START_DATE: start_date, _END_DATE:end_date} )
                # Ajuste de fecha inicial en registros cuya fecha inicial se desborda del rango actual
                .assign( **cut_ranges )
                # Se conservan las columnas originales
                [data.columns]
            )

        return fn

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
