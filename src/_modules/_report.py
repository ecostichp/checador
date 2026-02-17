from datetime import (
    date,
    timedelta,
)
import pandas as pd
import numpy as np
from .._constants import (
    COLUMN,
    LABEL_SCHEMA,
    REPORT,
)
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Report,
)
from .._resources import _DateSchema
from .._typing import (
    DatetimeStr,
    DataFramePipe,
)
from .._values import TIME_DELTA_ON_ZERO

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
            self._main._date.most_recent_available_date
            .__str__()
            .replace('-', '')
        )

        # Construcción del nombre del archivo de Excel
        file_name = f'{string_date}_{REPORT.SUMMARY.NAME}.xlsx'

        # Inicio de generación del archivo
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:

            # Usuarios
            (
                self._main._data.users
                # Exportación a Excel
                .to_excel(
                    writer,
                    sheet_name= REPORT.SUMMARY.SHEET.USERS,
                    index= False,
                )
            )

            # Datos completos verificados
            (
                self._summary()
                # Exportación del archivo a Excel
                .to_excel(
                    writer,
                    sheet_name= REPORT.SUMMARY.SHEET.COMPLETE,
                    index= False,
                )
            )

            # Incidencias del mes
            (
                self._main._data.justifications
                # Exportación del archivo a Excel
                .to_excel(
                    writer,
                    sheet_name= REPORT.SUMMARY.SHEET.MONTHLY_JUSTIFICATIONS,
                    index= False,
                )
            )

            # Resumen acumulado
            (
                pd.concat(
                    [
                        self._cummulated_summary(schema_i)
                        for schema_i in self._main._schemas
                    ]
                )
                # Exportación del archivo a Excel
                .to_excel(writer, sheet_name= REPORT.SUMMARY.SHEET.CUMMULATED_SUMMARY, index=False)
            )

            # Incidencias
            (
                pd.concat(
                    [
                        self._justification_counts(schema_i)
                        for schema_i in self._main._schemas
                    ]
                )
                # Exportación del archivo a Excel
                .to_excel(writer, sheet_name= REPORT.SUMMARY.SHEET.JUSTIFICATIONS, index=False)
            )

    def _summary(
        self,
    ) -> pd.DataFrame:
        """
        ### Resumen completo
        Este método genera los datos del resumen completo utilizado para cálculos más
        específicos.
        """

        return (
            # Obtención de los registros
            self._main.data.records
            # Se añaden las correcciones
            .pipe(self._main._pipes.common_operations)
        )

    # def _monthly_justifications(
    #     self,
    # ) -> pd.DataFrame:
    #     """
    #     ### Justificaciones del mes
    #     Este método genera la tabla de justificaciones del mes completo.
    #     """

    #     return (
    #         self._main._data.justifications
    #         # Se filtran todas las incidencias dentro del mes actual
    #         .pipe(
    #             self._main._factory.get_records_in_range(
    #                 self._main._date.month_start_date,
    #                 self._main._date.month_end_date,
    #             )
    #         )
    #     )

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
        }

        def fn(data: pd.DataFrame) -> pd.DataFrame:

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
            .pipe(fn)
            # Ejecución dentro de una función para utilizar el estado desde aquí
            .pipe(
                lambda df: (
                    df
                    # Se asigna una columna para capturar el esquema actual
                    .assign(**{LABEL_SCHEMA: schema.name})
                    # Reordenamiento de columnas
                    [ [LABEL_SCHEMA] + df.columns.tolist() ]
                )
            )
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
            .pipe( self._main._factory.vacation_days(schema) )
            # Obtención de los conteos de justificaciones distintas a vacaciones
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= self._justifications(schema),
                        on= [COLUMN.USER_ID, COLUMN.NAME],
                    )
                )
            )
            # Ejecución dentro de una función para utilizar el estado desde aquí
            .pipe(
                lambda df: (
                    df
                    # Se asigna una columna para capturar el esquema actual
                    .assign(**{LABEL_SCHEMA: schema.name})
                    # Reordenamiento de columnas
                    [ [LABEL_SCHEMA] + df.columns.tolist() ]
                )
            )
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

        # Función para generar lambda de filtro
        def filter_(start_date: date, end_date: date):

            # Lambda de filtro en base a las fechas provistas en el esquema de tiempo
            fn: DataFramePipe = (
                lambda df: df[
                    ( df[COLUMN.REGISTRY_TIME].dt.date >= start_date )
                    & ( df[COLUMN.REGISTRY_TIME].dt.date <= end_date )
                ]
            )

            return fn

        return (
            # Obtención de los registros
            self._main.data.records
            # Filtro por fechas usando lambda generada
            .pipe( filter_(schema.start_date, schema.end_date) )
            # Se añaden las correcciones
            .pipe(self._main._pipes.common_operations)
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

        # Función para generar lambda de filtro
        def filter_(start_date: date, end_date: date):

            # Lambda de filtro en base a las fechas provistas en el esquema de tiempo
            fn: DataFramePipe = (
                lambda df: df[
                    ( df[COLUMN.REGISTRY_TIME].dt.date >= start_date )
                    & ( df[COLUMN.REGISTRY_TIME].dt.date <= end_date )
                ]
            )

            return fn

        return (
            # Obtención de los registros
            self._main.data.records
            # Filtro por fechas usando lambda generada
            .pipe( filter_(schema.start_date, schema.end_date) )
            # Se añaden las correcciones
            .pipe(self._main._pipes.common_operations)
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

        # Función para generar lambda de filtro
        def filter_(start_date: date, end_date: date):

            # Lambda de filtro en base a las fechas provistas en el esquema de tiempo
            fn: DataFramePipe = (
                lambda df: df[
                    ( df[COLUMN.REGISTRY_TIME].dt.date >= start_date )
                    & ( df[COLUMN.REGISTRY_TIME].dt.date <= end_date )
                ]
            )

            return fn

        return (
            # Obtención de los registros
            self._main.data.records
            # Filtro por fechas usando lambda generada
            .pipe( filter_(schema.start_date, schema.end_date) )
            # Se añaden las correcciones
            .pipe(self._main._pipes.common_operations)
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

    def _justifications(
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

        return (
            self._main.data.justifications
            # Obtención del resumen de permisos de tipo día
            .pipe( self._main._factory.get_permissions_summary('days', schema) )
            # Se une el DataFrame con el resultado de...
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= (
                            self._main.data.justifications
                            # Obtención del resumen de permisos de tipo tiempo
                            .pipe( self._main._factory.get_permissions_summary('time', schema) )
                        ),
                        on= [COLUMN.USER_ID, COLUMN.NAME],
                        how= 'outer',
                    )
                )
            )
        )
