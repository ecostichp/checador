import pandas as pd
from ..constants import (
    COLUMN,
    REGISTRY_TYPE,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Factory,
)
from ..resources import _DateSchema
from ..rules import (
    GLOBAL_FILTERS,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from ..typing import DataFramePipe
from ..typing.literals import (
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
                .pipe( self._main._schedules.get_permissions_in_date_range(schema) )
                # Se cortan los rangos de fechas para contar desde la fecha inicial del rango asignado
                .pipe( self._main._schedules.cut_justifications_date_ranges(schema) )
                # Reasgnación de categorías para evitar pérdida de información en pivoteos de DataFrane
                .pipe( self._main._transformation.reassign_registry_type_categories(assigned_categories) )
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
        /,
        by: ValidityOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:

        # Función para conservar los registros de entrada del día en curso
        def filter_records(data: pd.DataFrame) -> pd.Series:

            # Condiciones para filtrar por validez de integridad de datos completos
            filter_validity: dict[ValidityOptions, pd.Series] = {
                'valid': data[COLUMN.IS_CLOSED_CORRECT],
                'invalid': ~data[COLUMN.IS_CLOSED_CORRECT],
            }

            # Construcción de condición
            validated_condition = filter_validity[by]

            # Validación de si los registros no son anulados
            is_not_null = data[COLUMN.REGISTRY_TYPE] != REGISTRY_TYPE.NULL

            # Si se especificó que se incluyeran los registros de inicio de jornada del día en curso...
            if keep_today_check_in:
                # Construcción de validación de registros
                is_current_day_check_in = data[COLUMN.IS_CURRENT_DAY_CHECKIN] == keep_today_check_in
                # Se añade la validación de si es registro de inicio de jornada en el día en curso
                validated_condition |= is_current_day_check_in
            # Si se especificó que no se incluyeran los registros de inicio de jornada del día en curso...
            else:
                # Validación de si la fecha de registros es distinta al día en curso
                date_is_not_current_day = data[COLUMN.DATE].dt.date != self._main._services.date.today
                # Se añade la validación de si la fecha de registros es distinta al día en curso
                validated_condition &= date_is_not_current_day

            # Se añade la validación de registro no anulado
            validated_condition &= is_not_null

            return (
                data[validated_condition]
            )

        def evaluate_pivot_validations(data: pd.DataFrame) -> pd.DataFrame:

            # Inicialización de función que une con AND todas las validaciones
            def validations_union(df: pd.DataFrame) -> pd.DataFrame:
                # Iteración por cada nombre de validación
                for validation_name in VALIDATIONS_PER_DAY_AND_USER_ID.keys():
                    # Unión de las columnas con AND
                    df[COLUMN.IS_CLOSED_CORRECT] &= df[validation_name]

                return df

            return (
                data
                # Creación de columna booleana inicializada en verdadera
                .assign(**{COLUMN.IS_CLOSED_CORRECT: True})
                # Unión de las validaciones
                .pipe(validations_union)
                # Se filtra el DataFrame únicamente por los datos especificados
                .pipe(filter_records)
                # Filtros globales
                .pipe(GLOBAL_FILTERS)
            )

        return evaluate_pivot_validations
