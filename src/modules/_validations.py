import pandas as pd
from ..constants import (
    COLUMN,
    REGISTRY_TYPE,
    VALIDATION,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Validations,
)
from ..core import pipeline_hub
from ..rules import (
    GLOBAL_FILTERS,
    PIPELINE,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from ..settings import REPORT
from ..templates.messages import (
    COMMON_ARGS,
    MESSAGE,
)
from ..typing import DataFramePipe
from ..typing.literals import ViewOptions
from ..typing.misc import DataTypeOrNone

class _Validations(_Interface_Validations):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def check_integrity(
        self,
    ) -> DataTypeOrNone[pd.DataFrame]:

        # Función para filtrar por registros inválidos
        filter_by_invalid_fn = self.filter_by_validity(view= 'verifications')

        validations = (
            self._main._validations.base_records_for_report()
            # Se descartan todos los registros de entrada del día en curso
            .pipe(lambda df: df[ ~df[COLUMN.IS_CURRENT_DAY_CHECKIN] ])
            # Se descartan todos los registros que contengan alguna validación no aprobada
            .pipe(filter_by_invalid_fn)
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.TIME,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.IS_DUPLICATED,
                COLUMN.IS_CORRECTION,
                VALIDATION.COMPLETE,
                VALIDATION.BREAK_PAIRS,
                VALIDATION.UNIQUE_START_AND_END,
                COLUMN.IS_CURRENT_DAY_CHECKIN,
            ]]
        )

        # Si existen registros a validar...
        if len(validations):
            # Se genera el archivo Excel con los registros a validar
            validations.to_excel(f'{REPORT.VERIFICATION.NAME}.xlsx', index= False)
            # Se indica al usuario que hay registros que requieren ser corregidos
            print(MESSAGE.RECORDS_TO_FIX_WERE_FOUND)
            print(
                MESSAGE.HINT_VALIDATIONS
                .format(
                    **{
                        COMMON_ARGS.VALIDATIONS_ATTRIBUTE: 'to_verify'
                    }
                )
            )
            # Se retorna el DataFrame
            return validations

        # Si no existen registros a validar...
        else:
            # Se indica que todo está correcto
            print(MESSAGE.ALL_OK)

    def filter_by_validity(
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

    def records_for_report(
        self,
    ) -> pd.DataFrame:

        # Obtención de registros base para reporte
        base_records_for_report = self.base_records_for_report()
        # Función para filtrar por registros válidos
        filter_by_valid_fn = self.filter_by_validity(view= 'report', keep_today_check_in= True)
        # Filtro por registros válidos
        filtered_records = filter_by_valid_fn(base_records_for_report)
        # Evaluación de fechas y horas de registro
        records_for_report = pipeline_hub.run_pipe_flow(filtered_records, PIPELINE.EVALUATE_REGISTRY_TIMES)

        return records_for_report

    def base_records_for_report(
        self,
    ) -> pd.DataFrame:

        # Obtención de registros del dispositivo
        records = self._main.data.records

        # Obtención de registros corregidos
        corrected_records = pipeline_hub.run_pipe_flow(records, PIPELINE.CORRECT_RECORDS)
        # Validación de registros
        validated_records = pipeline_hub.run_pipe_flow(corrected_records, PIPELINE.VALIDATE_RECORDS)

        # Obtención de los registros completos para reportes
        base_records_for_report = (
            pd.merge(
                left= validated_records,
                right= corrected_records,
                left_on= COLUMN.USER_AND_DATE_INDEX,
                right_on= COLUMN.USER_AND_DATE_INDEX,
                how= 'left',
            )
            # Se ordenan los datos por fecha y hora de registro
            .sort_values(COLUMN.REGISTRY_TIME)
        )

        # Se etiquetan las entradas del día en curso
        base_records_for_report = pipeline_hub.run_pipe_flow(base_records_for_report, PIPELINE.VALIDATE_TODAY_CHECKIN)

        return base_records_for_report
