from typing import Callable
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
from ..constants import (
    COLUMN,
    PERMISSION_NAME,
    REGISTRY_TYPE,
    TIME_DELTA_ON_ZERO,
    VACATION_DAYS_PER_YEAR,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Report,
)
from ..mapping import PERMISSION_TYPE_REASSIGNATION_NAMES
from ..resources import _DateSchema
from ..settings import REPORT
from ..typing import (
    ColumnAssignation,
    DataFramePipe,
)
from ..typing.interfaces import HorizontalSeries
from ..typing.aliases import DatetimeStr
from ..typing.callables import SeriesApply
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

        # Asignación de columna para obtención de días restantes a tomar
        remaining_days_fn: ColumnAssignation = {
            COLUMN.REMAINING_VACATION_DAYS: (
                lambda df: df[COLUMN.AVAILABLE_VACATION_DAYS] - df[COLUMN.VACATION_DAYS_TAKEN]
            ),
        }

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

        return (
            summary
            .merge(
                self._get_total_vacation_days(),
                'left',
                COLUMN.USER_ID,
            )
            .merge(
                self._get_vacation_days_taken(),
                'left',
                COLUMN.USER_ID,
            )
            # Reemplazo de valores nulos en conteos de días de vacaciones
            .replace({
                COLUMN.AVAILABLE_VACATION_DAYS: {np.nan: 0},
                COLUMN.VACATION_DAYS_TAKEN: {np.nan: 0},
            })
            # Asignación de días restantes de vacaciones a tomar
            .assign(**remaining_days_fn)
            # Asignación de tipos de dato
            .pipe(self._main._processing.assign_dtypes)
            # Seleeción de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.WAREHOUSE,
                COLUMN.PAY_FREQUENCY,
                COLUMN.JOB,
                COLUMN.HIRE_DATE,
                COLUMN.AVAILABLE_HOLIDAYS,
                PERMISSION_NAME.HOLIDAY_ABSENCE,
                PERMISSION_NAME.HOLIDAY_COMPENSATION,
                COLUMN.REMAINING_HOLIDAYS,
                COLUMN.YEAR_VALIDITY_DATE,
                COLUMN.REMAINING_VACATION_DAYS,
            ]]
        )

    def lunch_summary(
        self,
    ) -> pd.DataFrame:

        return self._reports_by_schemas( self._cummulated_summary )

    def justfications_summmary(
        self,
    ) -> pd.DataFrame:

        return self._reports_by_schemas( self._justification_counts )

    def _get_total_vacation_days(
        self,
    ) -> pd.DataFrame:

        def year_validity_date_fn(s: HorizontalSeries):

            # Obtención del año del período vacacional
            year = s[COLUMN.YEAR_PERIOD]
            # Obtención de la fecha de ingreso a la empresa
            hire_date = s[COLUMN.HIRE_DATE]

            # Construcción de fecha de validez de período vacacional para el año provisto
            year_validity_date = date(year, hire_date.month, 1)

            return year_validity_date

        def has_available_vacation_days_fn(s: HorizontalSeries):

            # Obtención del año del período vacacional
            hire_date = s[COLUMN.HIRE_DATE]
            # Obtención de fecha de validez de período vacacional para el año provisto
            year_validity_date = s[COLUMN.YEAR_VALIDITY_DATE]

            # Indicador de no año actual
            not_current_year = hire_date.year < year_validity_date.year
            # Indicador de que el mes actual es mayor o igual al de la fecha de validez
            is_current_month_greater_or_equal = hire_date.month <= year_validity_date.month

            # Evaluación de si el empleado tiene disponibles días de vacaciones
            has_available_vacation_days = not_current_year and is_current_month_greater_or_equal

            return has_available_vacation_days

        def years_in_company_fn(s: HorizontalSeries):

            # Obtención de año del período vacacional
            year = s[COLUMN.YEAR_PERIOD]
            # Obtención de la fecha de ingreso a la empresa
            hire_date = s[COLUMN.HIRE_DATE]

            # Obtención de los años de antigüedad en la empresa
            years_in_company = year - hire_date.year

            return years_in_company

        # Columnas temporales
        _HAS_AVAILABLE_VACATION_DAYS = '_has_available_vacation_days'
        _YEARS_IN_COMPANY = '_years_in_company'

        # Inicialización de lista de DataFrames a concatenar
        dfs_to_concat: list[pd.DataFrame] = []

        # Función para obtención de días de vacaciones del período en base a la antigüedad del empleado
        vacation_days_apply: SeriesApply[int] = (
            lambda years: VACATION_DAYS_PER_YEAR[years] if years >= 0 else 0
        )

        # Inicialización de diccionario de columnas a asignar a los DataFrames
        columns_to_assign: ColumnAssignation = {
            # Fecha de validez de inicio de período vacacional en base al año provisto
            COLUMN.YEAR_VALIDITY_DATE: (
                lambda df: (
                    df
                    .apply(
                        year_validity_date_fn,
                        axis= 1,
                    )
                )
            ),
            # Indicador de si el empleado tiene derecho a período vacacional
            _HAS_AVAILABLE_VACATION_DAYS: (
                lambda df: (
                    df
                    .apply(
                        has_available_vacation_days_fn,
                        axis= 1,
                    )
                )
            ),
            # Años de antigüedad en la empresa
            _YEARS_IN_COMPANY: (
                lambda df: (
                    df
                    .apply(
                        years_in_company_fn,
                        axis= 1,
                    )
                )
            ),
            # Cantidad de días de vacaciones del período vacacional
            COLUMN.AVAILABLE_VACATION_DAYS: (
                lambda df: (
                    df[_YEARS_IN_COMPANY]
                    .apply(vacation_days_apply)
                )
            ),
        }

        # Creación de lista de años a iterar para obtención de períodos vacacionales
        years = [year for year in range(2024, self._main._schemas._today.year + 1)]

        # Iteración sobre una lista de valores de año comenzando por 2024
        for year_i in years:

            # Función de asignación de valor de año
            year_assignation: ColumnAssignation = {
                COLUMN.YEAR_PERIOD: year_i,
            }

            # Procesamiento de DataFrame con los datos correspondientes al año i
            df_i = (
                self._main._data.users
                # Asignación de valor de año provisto
                .assign(**year_assignation)
                # Asignación de columnas
                .assign(**columns_to_assign)
                # Se filtran los registros solo por los que tienen un período vacacional disponible
                .pipe(lambda df: df[df[_HAS_AVAILABLE_VACATION_DAYS]])
                # Se filtran los registros cuya fecha de validez ya es menor o igual al día de hoy
                .pipe(lambda df: df[df[COLUMN.YEAR_VALIDITY_DATE] <= self._main._schemas._today])
            )

            # Se añade el DataFrame i a la lista de DataFrames a concaterar
            dfs_to_concat.append(df_i)

        return (
            pd.concat(
                # Concatenación de DataFrames
                dfs_to_concat,
                ignore_index= True,
            )
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.YEAR_VALIDITY_DATE,
                COLUMN.AVAILABLE_VACATION_DAYS,
            ]]
            # Agrupamiento por ID de usuario para obtener total de días de vacaciones
            .groupby(COLUMN.USER_ID)
            .agg({
                COLUMN.YEAR_VALIDITY_DATE: 'max',
                COLUMN.AVAILABLE_VACATION_DAYS: 'sum',
            })
            # Reseteo de índice
            .reset_index()
        )

    def _get_vacation_days_taken(
        self,
    ) -> pd.DataFrame:

        # Función para renombrar valores de tipos de permisos
        def rename_permission_names(df: pd.DataFrame) -> pd.DataFrame:
            return (
                df
                .astype({COLUMN.PERMISSION_TYPE: 'string'})
                .replace({COLUMN.PERMISSION_TYPE: PERMISSION_TYPE_REASSIGNATION_NAMES})
                .astype({COLUMN.PERMISSION_TYPE: 'category'})
            )

        # Asignaciónes de columnas para obtención únicamente de los valores de fecha sin hora
        get_dates_only: ColumnAssignation = {
            COLUMN.PERMISSION_START: lambda df: df[COLUMN.PERMISSION_START].dt.date,
            COLUMN.PERMISSION_END: lambda df: df[COLUMN.PERMISSION_END].dt.date,
        }

        # Función de conteo de días de vacaciones tomadas
        day_assignations: ColumnAssignation = {
            COLUMN.REST_DAYS_COUNT: ( lambda df: df.apply(self._main._schedules.count_rest_days, axis= 1, result_type= 'reduce') ),
            COLUMN.HOLIDAYS_COUNT: ( lambda df: df.apply(self._main._schedules.count_holidays, axis= 1, result_type= 'reduce') ),
            COLUMN.VACATION_DAYS_TAKEN: ( lambda df: df.apply(self._main._schedules.count_vacation_days, axis= 1, result_type= 'reduce') ),
        }

        return (
            self._main._data.justifications
            # Reasignación de nombres de permisos
            .pipe(rename_permission_names)
            # Se filtran solo los registros cuyo tipo de permiso es de vacaciones
            .pipe(lambda df: df[df[COLUMN.PERMISSION_TYPE] == PERMISSION_NAME.VACATION])
            # Obtención de los valores de fecha sin hora
            .assign(**get_dates_only)
            # Recuperación de tipos de datos
            .astype({
                COLUMN.PERMISSION_START: 'datetime64[s]',
                COLUMN.PERMISSION_END: 'datetime64[s]',
            })
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            ]]
            # Se concatena el resultado con el historial de vacaciones tomadas desde 2024
            .pipe(
                lambda df: pd.concat([
                    self._main._data.vacations_history_old,
                    df,
                ])
            )
            # Obtención de días de vacaciones tomadas
            .assign(**day_assignations)
            # Agrupamiento por ID de usuario
            .groupby(COLUMN.USER_ID)
            .agg({
                COLUMN.VACATION_DAYS_TAKEN: 'sum',
            })
            # Reseteo de índice
            .reset_index()
        )

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
            COLUMN.REST_DAYS_TAKEN: self._rest_days_taken(schema),
            COLUMN.REST_DAYS_CONFIRMED: self._rest_days_confirmed(schema),
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

        # Obtención de días laborados por usuario
        worked_days_per_user = (
            # Obtención de los registros
            self._records_into_schema(schema)
            # Obtención de los registros que son tipo de registro de entrada
            .pipe(lambda df: df[df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN])
            # Agrupamiento por ID de usuario
            .groupby(COLUMN.USER_ID)
            .agg({COLUMN.REGISTRY_TYPE: 'count'})
            # Reasignación de nombre de columna
            .rename(
                columns= {
                    COLUMN.REGISTRY_TYPE: COLUMN.WORKED_DAYS,
                },
            )
            # Asignación de tipo de dato
            .astype({
                COLUMN.WORKED_DAYS: 'uint8',
            })
            # Reseteo de índice
            .reset_index()
        )

        return (
            # Uso de los datos de usuarios
            self._main._data.users
            # Unión con cálculo d econteo de días laborados
            .merge(
                right= worked_days_per_user,
                on= COLUMN.USER_ID,
                how= 'left',
            )
            # Reemplazo de valores nulos encontrados
            .replace({
                COLUMN.WORKED_DAYS: {np.nan: 0}
            })
            # Conversión de tipo de dato
            .astype({
                COLUMN.WORKED_DAYS: 'uint8',
            })
        )

    def _rest_days_taken(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:

        agg_data = self._rest_days(schema, True)

        return agg_data

    def _rest_days_confirmed(
        self,
        schema: _DateSchema,
    ) -> pd.DataFrame:

        agg_data = self._rest_days(schema, False)

        return agg_data

    def _rest_days(
        self,
        schema: _DateSchema,
        cut_to_today: bool,
    ) -> pd.DataFrame:

        _VALID = '_valid'

        # Tipos de permiso que invalidan un día de descanso
        LEAVE_JUSTIFICACION_NAMES = [
            PERMISSION_NAME.SICK_GENERAL,
            PERMISSION_NAME.WORK_RISK,
            PERMISSION_NAME.MATERNITY,
        ]

        # Asignación de columnas en formato de fecha
        to_date_fn: ColumnAssignation = {
            COLUMN.PERMISSION_START: lambda df: df[COLUMN.PERMISSION_START].dt.date,
            COLUMN.PERMISSION_END: lambda df: df[COLUMN.PERMISSION_END].dt.date,
        }

        # Obtención de incidencias de incapacidad
        leave_justifications: pd.DataFrame = (
            self._main.data.justifications
            # Se renombran los tipos de permiso
            .pipe(self._main._pipes.rename_permission_types)
            # Se conservan únicamente las incidencias que sean incapacidades
            .pipe(lambda df: df[df[COLUMN.PERMISSION_TYPE].isin(LEAVE_JUSTIFICACION_NAMES)])
            # Conversión de tipos de dato de fecha y hora a solo fecha
            .assign(**to_date_fn)
            # Filtro por fechas relevantes únicamente
            .pipe(
                lambda df: (
                    df[
                        (
                            (
                                ( df[COLUMN.PERMISSION_START] >= self._main._schemas.min_date() )
                                & ( df[COLUMN.PERMISSION_START] <= self._main._schemas.max_date() )
                            )
                            | (
                                ( df[COLUMN.PERMISSION_END] >= self._main._schemas.min_date() )
                                & ( df[COLUMN.PERMISSION_END] <= self._main._schemas.max_date() )
                            )
                        )
                    ]
                )
            )
        )

        def discard_rest_days_into_leaves(rest_days: pd.DataFrame):

            # Inicialización de valor de validación en Falso
            rest_days[_VALID] = False

            def tag_valid_rest_days(s: pd.Series):
                # Obtención de valores
                start_date: date = s[COLUMN.PERMISSION_START]
                end_date: date = s[COLUMN.PERMISSION_END]
                user_id: int = int(s[COLUMN.USER_ID])

                # Construcción de rango de fechas para evaluar valores
                leave_justification_date_range = (
                    pd.date_range(start_date, end_date)
                    .date
                    .tolist()
                )

                # Reasignación de valor
                rest_days[_VALID] = (
                    rest_days[_VALID]
                    | (
                        rest_days
                        .pipe(
                            lambda df: (
                                # La ID de usuario corresponde al de la incapacidad
                                ( df[COLUMN.USER_ID] == user_id )
                                # La fecha se encuentra dentro del rango de la fecha de incapacidad
                                & ( df[COLUMN.REST_DATE].dt.date.isin(leave_justification_date_range) )
                            )
                        )
                    )
                )

            # Iteración por cada registro de incapacidad
            leave_justifications.apply(tag_valid_rest_days, axis= 1)

            return rest_days.pipe(lambda df: df[df[_VALID] == False])

        end_date_limit = (
            min(schema.end_date, self._main._schemas._today)
                if cut_to_today
                else schema.end_date
        )

        _REST_DAYS_COLUMN = (
            COLUMN.REST_DAYS_TAKEN
                if cut_to_today
                else COLUMN.REST_DAYS_CONFIRMED
        )

        # Conteo de días de descanso asignados
        assigned_days_count_per_user = (
            self._main._data.rest_schedules
            # Filtro por las fechas provistas en el esquema de tiempo y la fecha de hoy
            .pipe(
                lambda df: df[
                    ( df[COLUMN.REST_DATE].dt.date >= schema.start_date )
                    & ( df[COLUMN.REST_DATE].dt.date <= end_date_limit )
                ]
            )
            # Se descartan días de descanso dentro de incidencias de incapacidad
            .pipe(discard_rest_days_into_leaves)
            # Agrupamiento por ID de usuario
            .groupby(COLUMN.USER_ID)
            .agg({COLUMN.REST_DATE: 'count'})
            # Reasignación de nombre de columna
            .rename(
                columns= {COLUMN.REST_DATE: _REST_DAYS_COLUMN},
            )
            # Reseteo de índice
            .reset_index()
        )

        return (
            # Uso de los datos de usuarios
            self._main._data.users
            # Unión con conteo de días asignados por usuario ya tomados hasta la fecha de hoy
            .merge(
                right= assigned_days_count_per_user,
                on= COLUMN.USER_ID,
                how= 'left'
            )
            # Reemplazo de valores nulos por ceros
            .replace({_REST_DAYS_COLUMN: {np.nan: 0}})
            # Asignación de tipo de dato
            .astype({
                _REST_DAYS_COLUMN: 'uint8',
            })
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                _REST_DAYS_COLUMN,
            ]]
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
                # Conversión de columna a string para hacer el reemplazo
                .astype({
                    COLUMN.PERMISSION_TYPE: 'string[python]',
                })
                # Reemplazo de nombre de tipo de permiso
                .replace({
                    COLUMN.PERMISSION_TYPE: {
                        PERMISSION_NAME.UNPAID_EXTRA_HOURS_PERMISSION: PERMISSION_NAME.HOURS_PERMISSION,
                    },
                })
                # Conversión de columna a categórica
                .astype({
                    COLUMN.PERMISSION_TYPE: 'category',
                })
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
