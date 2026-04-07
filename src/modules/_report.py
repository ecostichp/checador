from typing import Callable
import pandas as pd
import numpy as np
from datetime import timedelta
from ..constants import (
    COLUMN,
    PERMISSION_NAME,
    TIME_DELTA_ON_ZERO,
    VALIDATION,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Report,
)
from ..resources import _DateSchema
from ..settings import REPORT
from ..typing import (
    ColumnAssignation,
    DataFramePipe,
)
from ..typing.aliases import DatetimeStr
from ..typing.literals import PermissionTypeOption
from ..core import pipeline_hub
from ..rules import PIPELINE

class _Report(_Interface_Report):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def generate(
        self,
    ) -> None:

        # Construcción de una fecha en cadena de texto
        string_date: DatetimeStr = (
            self._main._services.date.most_recent_available_date
            .__str__()
            .replace('-', '')
        )

        # Construcción del nombre del archivo de Excel
        file_name = f'{string_date}_{REPORT.SUMMARY.NAME}.xlsx'

        # Reportes a exportar
        reports_to_export: dict[str, pd.DataFrame] = {
            # Usuarios
            REPORT.SUMMARY.SHEET.USERS: self._main._data.users,
            # Datos completos verificados
            REPORT.SUMMARY.SHEET.COMPLETE: self.complete_general_summary(),
            # Historial de incidencias
            REPORT.SUMMARY.SHEET.MONTHLY_JUSTIFICATIONS: self._main._data.justifications,
            # Resumen de acumulados
            REPORT.SUMMARY.SHEET.CUMMULATED_SUMMARY: self.lunch_summary(),
            # Incidencias
            REPORT.SUMMARY.SHEET.JUSTIFICATIONS: self.justfications_summmary(),
        }

        # Inicio de generación del archivo
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            # Iteración por cada reporte a exportar
            for ( sheet_name, report ) in reports_to_export.items():
                # Exportación de reporte
                (
                    report
                    .to_excel(
                        writer,
                        sheet_name= sheet_name,
                        index= False,
                    )
                )

    def complete_general_summary(
        self,
    ) -> pd.DataFrame:

        # Obtención de los registros base para reporte
        records_for_report = self._main._records_for_report
        # Procesamiento por medio de pipes
        data = pipeline_hub.run_pipe_flow(records_for_report, PIPELINE.CUSTOMIZED_OUTPUT)

        return data

    def justifications(
        self,
    ) -> pd.DataFrame:

        # Obtención del historial de incidencias
        justifications = self._main._data.justifications
        # Procesamiento por medio de pipes
        data = pipeline_hub.run_pipe_flow(justifications, PIPELINE.JUSTIFICATIONS_HISTORY)

        return (
            data
        )

    def holidays_summary(
        self,
    ) -> pd.DataFrame:

        # Obtención de los datos
        users = self._main._data.users
        justifications = self._main._data.justifications

        # Procesamiento por medio de pipes
        available_holidays_per_employee = pipeline_hub.run_pipe_flow(users, PIPELINE.GET_AVAILABLE_HOLIDAYS)
        justification_counts = pipeline_hub.run_pipe_flow(justifications, PIPELINE.COUNT_HOLIDAYS_ON_JUSTIFICATIONS)

        # Se unen los cómputos con los registros de empleados
        d = (
            available_holidays_per_employee
            .merge(
                right= justification_counts,
                on= COLUMN.USER_ID,
                how= 'left',
            )
        )

        # Procesamiento por medio de pipe
        summary = pipeline_hub.run_pipe_flow(d, PIPELINE.HOLIDAYS_SUMMARY)

        return summary

    def lunch_summary(
        self,
    ) -> pd.DataFrame:

        return self._reports_by_schemas( self._cummulated_summary )

    def justfications_summmary(
        self,
    ) -> pd.DataFrame:

        return self._reports_by_schemas( self._justification_counts )

    def _cummulated_summary(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Resumen de acumulados
        Este método genera resúmenes de acumulados por usuario basado en el esquema de
        tiempo provisto y obtiene:
        - Minutos en entradas tardías.
        - Minutos acumulados en salidas anticipadas.
        - Minutos en excedentes de hora de comida.

        Posteriormente los une en un mismo DataFrame y retorna el resultado.

        :param schema _DateSchema: Esquema de tiempo para usar como criterio.
        """

        # Creación de mapeo con datos calculados
        reports: dict[str, pd.DataFrame] = {
            COLUMN.LATE_TIME: self._late_start(schema),
            COLUMN.EARLY_TIME: self._early_end(schema),
            COLUMN.EXCEEDING_LUNCH_TIME: self._lunch_time(schema),
            COLUMN.WORKED_DAYS: self._worked_days(schema),
        }

        # Función para unir los reportes
        def merge_reports(data: pd.DataFrame) -> pd.DataFrame:

            # Iteración por cada par <llave, valor>
            for ( column_name, report ) in reports.items():

                # Se unen todos los reportes en el mismo DataFrame
                data = (
                    data
                    .merge(
                        right= (
                            report
                            # Selección de columnas
                            [[COLUMN.USER_ID, column_name]]
                        ),
                        on= 'user_id',
                        how= 'left',
                    )
                    .replace({
                        column_name: {np.nan: timedelta()}
                    })
                )

            return data

        return (
            # Se usan los datos de usuarios
            self._main.data.users
            # Segmentación de usuarios
            .pipe(lambda df: df[ df[COLUMN.PAY_FREQUENCY] == schema.frequency ])
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
            ]]
            # Se unen los reportes
            .pipe(merge_reports)
            # Ejecución dentro de una función para utilizar el estado desde aquí
            .pipe( self._assign_schema_name(schema) )
        )

    def _justification_counts(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:

        return (
            self._main.data.justifications
            # Se renombran los tipos de permiso
            .pipe(self._main._pipes.rename_permission_types)
            # Conteo de vacaciones dentro de las justificaciones
            .pipe( self._main._schedules.vacation_days(schema) )
            # Obtención de los conteos de justificaciones distintas a vacaciones
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= self._justifications_summary(schema),
                        on= [COLUMN.USER_ID, COLUMN.NAME],
                    )
                )
            )
            # Ejecución dentro de una función para utilizar el estado desde aquí
            .pipe( self._assign_schema_name(schema) )
        )

    def _late_start(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Entradas tardías
        Este método genera el resumen de minutos acumulados en entradas tardías por
        usuario en base a las fechas del esquema de tiempo provisto.

        :param schema _DateSchema: Esquema de tiempo para usar como criterio.
        """

        return (
            # Obtención de los registros
            self._records_into_schema(schema)
            # Registros de entradas tardías con minutos acumulados
            .groupby(COLUMN.NAME, observed= True)
            .agg({
                COLUMN.USER_ID: 'first',
                COLUMN.LATE_TIME: 'sum',
            })
            # Se filtran todos los resultados que no tengan tiempo de entrada tardía
            .pipe(lambda df: df[ df[COLUMN.LATE_TIME] != TIME_DELTA_ON_ZERO ])
            # Ordernamiento de mayor a menor
            .sort_values(
                COLUMN.LATE_TIME,
                ascending= False,
            )
            # Reseteo de índice
            .reset_index()
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.LATE_TIME,
            ]]
        )

    def _early_end(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Salidas anticipadas
        Este método genera el resumen de minutos acumulados en salidas anticipadas por
        usuario en base a las fechas del esquema de tiempo provisto.

        :param schema _DateSchema: Esquema de tiempo para usar como criterio.
        """

        return (
            # Obtención de los registros
            self._records_into_schema(schema)
            # Registros de salidas anticipadas con minutos acumulados
            .groupby(COLUMN.NAME, observed= True)
            .agg({
                COLUMN.USER_ID: 'first',
                COLUMN.EARLY_TIME: 'sum',
            })
            # Se filtran todos los resultados que no tengan tiempo de salida anticipada
            .pipe(lambda df: df[df[COLUMN.EARLY_TIME] != TIME_DELTA_ON_ZERO])
            # Ordenamiento por valores
            .sort_values(
                COLUMN.EARLY_TIME,
                ascending= False,
            )
            # Reseteo de índice
            .reset_index()
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.EARLY_TIME,
            ]]
        )

    def _lunch_time(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Excedentes en tiempo de comida
        Este método genera el resumen de minutos acumulados en excedentes en tiempo de
        comida por usuario en base a las fechas del esquema de tiempo provisto.

        :param schema _DateSchema: Esquema de tiempo para usar como criterio.
        """

        return (
            # Obtención de los registros
            self._records_into_schema(schema)
            # Minutos extras en tiempo de comida
            .pipe(self._main._pipes.get_exceeding_lunch_time)
            # Agrupación por nombres de usuario para obtención de sumas
            .groupby(COLUMN.NAME, observed= True,)
            .agg({
                COLUMN.USER_ID: 'first',
                COLUMN.EXCEEDING_LUNCH_TIME: 'sum',
            })
            # Se filtran todos los resultados que no tengan tiempo excedente en tiempo de comida
            .pipe(lambda df: df[ df[COLUMN.EXCEEDING_LUNCH_TIME] > TIME_DELTA_ON_ZERO ])
            # Reseteo de índice
            .reset_index()
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.EXCEEDING_LUNCH_TIME,
            ]]
        )

    def _worked_days(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Días laborados
        Este método genera el conteo de días laborados en base a las fechas del esquema
        de tiempo provisto.

        :param schema _DateSchema: Esquema de tiempo para usar como criterio.
        """

        # Función para obtención de validación de día completo de booleano a unsigned int 8
        validation_complete_as_int: ColumnAssignation = {
            COLUMN.WORKED_DAYS: (
                lambda df: (
                    df[VALIDATION.COMPLETE].astype('uint8')
                )
            )
        }

        return (
            # Obtención de los registros
            self._records_into_schema(schema)
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.DATE,
                VALIDATION.COMPLETE,
            ]]
            # Agrupamiento por ID de usuario y fecha
            .groupby([COLUMN.USER_ID, COLUMN.DATE])
            .agg({
                VALIDATION.COMPLETE: 'first',
            })
            # Asignación de columna numérica de validación de día completo
            .assign(**validation_complete_as_int)
            # Agrupamiento por ID de usuario
            .groupby(COLUMN.USER_ID)
            .agg({
                COLUMN.WORKED_DAYS: 'count',
            })
            # Reseteo de índice
            .reset_index()
        )

    def _justifications_summary(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:
        """
        ### Resumen de justificaciones
        Este método crea los reportes de permisos de tipo días y tiempo otorgados a los
        usuarios seleccionados dentro del esquema de tiempo proporcionado y los une
        para retornar un DataFrame completo.

        :param schema _DateSchema: Esquema de tiempo.
        """

        # Función para contar los permisos de días en el resumen
        permission_counts: ColumnAssignation = {
            COLUMN.INCAPACITIES_COUNT: (
                lambda df: (
                    df
                    # Selección de columnas
                    [[
                        PERMISSION_NAME.SICK_GENERAL,
                        PERMISSION_NAME.WORK_RISK,
                        PERMISSION_NAME.MATERNITY,
                    ]]
                    # Suma de los valores en el eje horizontal
                    .sum(axis= 1)
                )
            ),
            COLUMN.ABSENCES_COUNT: (
                lambda df: (
                    df
                    # Selección de columnas
                    [[
                        PERMISSION_NAME.UNJUSTIFIED_ABSENCE,
                        PERMISSION_NAME.UNPAID_EXTRA_ABSENCE,
                    ]]
                    # Suma de los valores en el eje horizontal
                    .sum(axis= 1)
                )
            ),
        }

        return (
            self._main.data.justifications
            # Obtención del resumen de permisos de tipo día
            .pipe( self._get_permissions_summary('days', schema) )
            # Se une el DataFrame con el resultado de...
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= (
                            self._main.data.justifications
                            # Obtención del resumen de permisos de tipo tiempo
                            .pipe( self._get_permissions_summary('time', schema) )
                        ),
                        on= [COLUMN.USER_ID, COLUMN.NAME],
                        how= 'outer',
                    )
                )
            )
            # Conteo de permisos de días en el resumen
            .assign(**permission_counts)
        )

    def _records_into_schema(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:

        return (
            # Obtención de registros para reportes
            self._main._validations.records_for_report()
            # Filtro por las fechas provistas en el esquema de tiempo
            .pipe(
                lambda df: df[
                    ( df[COLUMN.REGISTRY_TIME].dt.date >= schema.start_date )
                    & ( df[COLUMN.REGISTRY_TIME].dt.date <= schema.end_date )
                ]
            )
        )

    def _assign_schema_name(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:

        # Lambda de asignación de nombre de esquema
        assign_schema_name: DataFramePipe = (
            lambda df: (
                df
                # Se asigna una columna para capturar el esquema actual
                .assign(**{COLUMN.SCHEMA: schema.name})
                # Reordenamiento de columnas
                [ [COLUMN.SCHEMA] + df.columns.tolist() ]
            )
        )

        return assign_schema_name

    def _reports_by_schemas(
        self,
        fn: Callable[[_DateSchema], None],
    ) -> pd.DataFrame:

        # Construcción de DataFrame a partir de concatenaciones
        data = (
            pd.concat(
                [
                    fn(schema_i) for schema_i
                    in self._main._schemas
                ]
            )
        )

        return data

    def _get_permissions_summary(
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
