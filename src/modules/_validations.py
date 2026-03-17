import pandas as pd
from ..constants import (
    COLUMN,
    REGISTRY_TYPE,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Validations,
)
from ..rules import (
    GLOBAL_FILTERS,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from ..typing import DataFramePipe
from ..typing.literals import ViewOptions

class _Validations(_Interface_Validations):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def filter_for_view(
        self,
        /,
        view: ViewOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:

        # Función para conservar los registros de entrada del día en curso
        def filter_records(data: pd.DataFrame) -> pd.Series:

            # Condiciones para filtrar por validez de integridad de datos completos
            filter_validity: dict[ViewOptions, pd.Series] = {
                # Filtro por los datos cuyo día sea cerrado y correcto
                'report': data[COLUMN.IS_CLOSED_CORRECT],
                # Filtro por los datos cuyo día sea cerrado e incorrecto y además que el tipo de registro sea diferente a [null]
                'verifications': ~data[COLUMN.IS_CLOSED_CORRECT] & (data[COLUMN.REGISTRY_TYPE] != REGISTRY_TYPE.NULL),
            }

            # Construcción de condición
            validated_condition = filter_validity[view]

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
