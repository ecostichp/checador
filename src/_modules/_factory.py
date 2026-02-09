from datetime import date
import pandas as pd
import numpy as np
from .._constants import COLUMN
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Factory,
)
from .._resources import _DateSchema
from .._rules import (
    GLOBAL_FILTERS,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from .._typing import (
    ColumnAssignation,
    DataFramePipe,
    PermissionTypeOption,
    ValidityOptions,
)

class _Factory(_Interface_Factory):

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
                .pipe( self.get_records_in_range(schema.start_date, schema.end_date) )
                # Se conservan los registros que entren dentro del rango de fecha actual
                .pipe( self._cut_ranges(schema.start_date, schema.end_date) )
                # Obtención de días de vacaciones desde incidencias
                .pipe(self._main._pipes.total_vacation_days)
                # Agrupamiento por IDs de usuario
                .groupby(COLUMN.USER_ID)
                # Suma de los días de vacaciones
                .agg({
                    COLUMN.VACATION_DAYS: 'sum',
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
                .replace({ COLUMN.VACATION_DAYS: {np.nan: 0} })
                # Conversión del tipo de dato
                .astype({ COLUMN.VACATION_DAYS: 'uint8' })
            )

        return fn

    def get_permissions_summary(
        self,
        perm_type: PermissionTypeOption,
        schema: _DateSchema,
    ) -> DataFramePipe:

        def fn(data: pd.DataFrame) -> pd.DataFrame:

            # Categorías asignadas
            assigned_categories = self._main._processing.categories[perm_type]
            # Función de cálculo de rango de fechas
            assigned_range_diff = self._main._processing.range_diff[perm_type]
            # Asignación de tipo de dato en la columna
            assigned_dtype = self._main._processing.dtypes[perm_type]

            return (
                data
                # Se renombran los tipos de permiso
                .pipe(self._main._pipes.rename_permission_types)
                # Se filtran los registros que entran dentro del rango de fechas
                .pipe( self.get_records_in_range(schema.start_date, schema.end_date) )
                # Se cortan los rangos de fechas para contar desde la fecha inicial del rango asignado
                .pipe( self._cut_ranges(schema.start_date, schema.end_date) )
                # Reasgnación de categorías para evitar pérdida de información en pivoteos de DataFrane
                .pipe( self._reassign_registry_type_categories(assigned_categories) )
                # Cálculo de diferencia en rango de fechas
                .assign(**assigned_range_diff)
                # Agrupamiento por nombre y tipo de registro
                .groupby(
                    [
                        COLUMN.NAME,
                        COLUMN.PERMISSION_TYPE,
                    ],
                    observed= False,
                )
                # Suma de las diferencias en rangos de fechas
                .agg({
                    self._main._processing._DIFF: 'sum',
                })
                # Se recupera el tipo de dato de las sumas
                .astype( {self._main._processing._DIFF: assigned_dtype} )
                # Reseteo de índice
                .reset_index()
                # Pivoteo de tabla para mostrar sumas por usuario y tipo de permiso
                .pivot_table(
                    index= COLUMN.NAME,
                    columns= COLUMN.PERMISSION_TYPE,
                    values= self._main._processing._DIFF,
                    observed= False,
                )
                # Se recupera el tipo de dato en las columnas de suma
                .astype( {col: assigned_dtype for col in assigned_categories} )
                # Reseteo de índice
                .reset_index()
                # Obtención de las IDs de usuarios
                .pipe( self._main._pipes.get_user_id )
            )

        return fn

    def filter_by_validity(
        self,
        by: ValidityOptions,
    ) -> DataFramePipe:

        # Columna temporal
        _ALL_VALID = '_all_valid'

        # Funciones para asignar el filtro de los registros
        filter_: dict[ValidityOptions, DataFramePipe] = {
            'valid': lambda df: df[ df[_ALL_VALID] ],
            'invalid': lambda df: df[ ~df[_ALL_VALID] ],
        }

        def fn(data: pd.DataFrame) -> pd.DataFrame:

            # Inicialización de función que une con AND todas las validaciones
            def validations_union(df: pd.DataFrame) -> pd.DataFrame:
                # Iteración por cada nombre de validación
                for validation_name in VALIDATIONS_PER_DAY_AND_USER_ID.keys():
                    # Unión de las columnas con AND
                    df[_ALL_VALID] &= df[validation_name]

                return df

            return (
                data
                # Creación de columna booleana inicializada en verdadera
                .assign(**{_ALL_VALID: True})
                # Unión de las validaciones
                .pipe(validations_union)
                # Se filtra el DataFrame únicamente por los datos especificados
                .pipe( filter_[by] )
                # Filtros globales
                .pipe(GLOBAL_FILTERS)
                # Se descarta la columna temporal
                [data.columns]
            )

        return fn

    def get_records_in_range(
        self,
        start_date: date,
        end_date: date,
    ) -> DataFramePipe:

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
        record_in_range: DataFramePipe = (
            lambda df: (
                df[
                    (
                        _start_date_in_range(df)
                        | _end_date_in_range(df)
                        | _range_segment_in_range(df)
                    )
                ]
            )
        )

        return record_in_range

    def _reassign_registry_type_categories(
        self,
        categories: list[str],
    ) -> DataFramePipe:
        """
        ### Reasignar categorías de tipo de registro
        Este método fabrica una función que reasigna las categorías de tipos de
        registro en base a los valores disponibles en el DataFrame provisto. De esta
        manera las agrupaciones y pivoteos de tabla se mantienen consistentes.

        :param categories list[str]: Lista completa de categorías de tipo de registro.
        """

        # Función para establecer las categorías disponibles en la columna
        set_categories: ColumnAssignation = {
            COLUMN.PERMISSION_TYPE: (
                lambda df: (
                    # Se usa la columna de tipo de permiso
                    df[COLUMN.PERMISSION_TYPE]
                    # Se asignan las categorías provistas
                    .cat.set_categories(categories)
                )
            )
        }

        def fn(data: pd.DataFrame) -> pd.DataFrame:

            return (
                data
                # Se filtran los registros por las categorías provistas
                .pipe(
                    lambda df: (
                        df[ df[COLUMN.PERMISSION_TYPE].isin(categories) ]
                    )
                )
                # Se asignan las categorías provistas
                .assign(**set_categories)
            )

        return fn

    def _cut_ranges(
        self,
        start_date: date,
        end_date: date,
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
