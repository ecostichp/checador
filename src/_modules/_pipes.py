import pandas as pd
import numpy as np
from .._constants import (
    COLUMN,
    PERMISSION_NAME,
    REGISTRY_TYPE,
    REPORT,
    VALIDATION,
)
from .._core import (
    _CoreRegistryProcessing,
    _Interface_Pipes,
)
from .._mapping import (
    LUNCH_REGISTRY_TYPES,
    ORDERED_REGISTRY_TYPE,
    PERMISSION_TYPE_REASSIGNATION_NAMES,
    WAREHOUSE_RENAME,
)
from .._typing import (
    ColumnAssignation,
    Many2One,
    DataFramePipe,
    SeriesApply,
    SeriesFromDataFrame,
    SeriesPipe,
)
from .._rules import (
    CHECK_SPECIFIC_DAY,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from .._values import (
    LUNCH_DURATION_LIMIT,
    TIME_DELTA_ON_ZERO,
)

class _Pipes(_Interface_Pipes):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def get_user_names(
        self,
        records: pd.DataFrame,
    ) -> pd.DataFrame:

        # Función para descartar la columna de nombre del DataFrame
        discard_name_column: DataFramePipe = (
            lambda df: (
                df
                [
                    [col for col in df.columns if col != COLUMN.NAME]
                ]
            )
        )

        return (
            records
            # Se descarta la columna de nombre desde los registros
            .pipe(discard_name_column)
            # Se usa la ID de usuarios para obtener nombre desde los datos de Odoo
            .pipe(
                lambda df: (
                    pd.merge(
                        left= self._main._data.users,
                        right= df,
                        on= COLUMN.USER_ID,
                        how= 'right',
                    )
                )
            )
            # Conversión de columna de nombres a categoría
            .pipe(self._categorize_names)
        )

    def check_integrity(
        self,
    ) -> pd.DataFrame | None:

        validations = (
            # Obtención de los registros
            self._main._data.records
            # Se descartan inicio y fin de jornada duplicados
            .pipe(self._discard_duplicated)
            # Se añaden las correcciones
            .pipe(self._add_corrections)
            # Validación de registros
            .pipe(self._validate_records)
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
            ]]
            # Se descartan todos los registros que contengan alguna validación no aprobada
            .pipe( self._main._factory.filter_by_validity('invalid') )
            # Se selecciona un día específico en caso de existir algún valor para éste
            .pipe(CHECK_SPECIFIC_DAY)
        )

        # Si existen registros a validar...
        if len(validations):
            # Se genera el archivo Excel con los registros a validar
            validations.to_excel(f'{REPORT.VERIFICATION}.xlsx', index= False)
            # Se indica al usuario que hay registros que requieren ser corregidos
            print('Se encontraron registros para corregir.')
            print('Accede a la información a través del atributo [validations] o al Excel generado.')
            # Se retorna el DataFrame
            return validations

        # Si no existen registros a validar...
        else:
            # Se indica que todo está correcto
            print("Todo está correcto")

    def common_operations(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        return (
            data
            # Se descartan inicio y fin de jornada duplicados
            .pipe(self._discard_duplicated)
            # Se añaden las correcciones
            .pipe(self._add_corrections)
            # Validación de registros
            .pipe(self._validate_records)
            # Se filtran únicamente los registros correctos
            .pipe( self._main._factory.filter_by_validity('valid') )
            # Definición de inicio y finalización de tiempos permitidos para cada usuario
            .pipe(self._define_allowed_start_and_end_time)
            # Obtención de tiempo acumulado en entradas tardías y salidas anticipadas
            .pipe(self._get_cummulated_time)
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.TIME,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.IS_DUPLICATED,

                COLUMN.REGISTRY_TIME,
                COLUMN.IS_CORRECTION,
                VALIDATION.COMPLETE,
                VALIDATION.BREAK_PAIRS,
                VALIDATION.UNIQUE_START_AND_END,
                COLUMN.WEEKDAY,
                COLUMN.ALLOWED_START,
                COLUMN.ALLOWED_END,
                VALIDATION.IS_LATE_START,
                VALIDATION.IS_EARLY_END,
                COLUMN.LATE_TIME,
                COLUMN.EARLY_TIME,
            ]]
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Se ordenan los valores por fecha y hora
            .sort_values([COLUMN.DATE, COLUMN.TIME])
        )

    def get_exceeding_lunch_time(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        return (
            data
            # Ordenamiento de registros por ID de usuario y fecha
            .sort_values([COLUMN.USER_ID, COLUMN.DATE])
            # Asignación de índice por usuario y día de registro
            .pipe(self._assign_day_and_user_id_index)
            # Se unen el Dataframe entrante y su transformación para obtención de cálculos agregados
            .pipe(
                lambda df: (
                    pd.merge(
                        left= (
                            df
                            # Selección de columnas
                            [[
                                COLUMN.USER_ID,
                                COLUMN.NAME,
                                COLUMN.DATE,
                                COLUMN.USER_AND_DATE_INDEX,
                            ]]
                        ),
                        right= (
                            df
                            # Obtención de total de intervalos de comida
                            .pipe(self._get_lunch_intervals_total)
                        ),
                        on= COLUMN.USER_AND_DATE_INDEX,
                        how= 'inner',
                    )
                    # Agrupación por nombres de usuario y fecha para obtención de valores únicos
                    .groupby(
                        [
                            COLUMN.NAME,
                            COLUMN.DATE,
                        ],
                        observed= True,
                    )
                    .agg({
                        COLUMN.USER_ID: 'first',
                        COLUMN.EXCEEDING_LUNCH_TIME: 'first',
                    })
                    # Reseteo de índice
                    .reset_index()
                )
            )
        )

    def get_user_id(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        return (
            data
            .pipe(
                lambda df: (
                    pd.merge(
                        left= (
                            self._main._data.users
                            [[
                                COLUMN.USER_ID,
                                COLUMN.NAME,
                            ]]
                        ),
                        right= df,
                        on= COLUMN.NAME,
                        how= 'right',
                    )
                )
            )
        )

    def get_warehouse_name(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Función para extraer la ID del registro many2one de Odoo
        extract_id: SeriesApply[Many2One] = (
            lambda reference: reference[0]
        )

        # Función para asignar nombre a la ID de almacén
        rename_id: SeriesApply[int] = (
            lambda warehouse_id: WAREHOUSE_RENAME[warehouse_id]
        )

        # Función para extraer y renombrar la información de ID de almacén
        process_warehouse_data: ColumnAssignation = {
            COLUMN.WAREHOUSE: (
                lambda df: (
                    df[COLUMN.WAREHOUSE]
                    .apply(extract_id)
                    .apply(rename_id)
                )
            )
        }

        return (
            data
            .assign(**process_warehouse_data)
        )

    def get_job_name(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Función para extraer el nombre del registro many2one de Odoo
        extract_name: SeriesApply[Many2One] = (
            lambda reference: reference[1]
        )

        # Función para reasignar el valor procesado a la misma columna
        reassign_value: ColumnAssignation = {
            COLUMN.JOB: (
                lambda df: (
                    df[COLUMN.JOB]
                    .apply(extract_name)
                )
            )
        }

        return (
            data
            .assign(**reassign_value)
        )

    def assign_ordered_registry_type(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Obtención de los valores categóricos encontrados en el DataFrame
        available_values = data[COLUMN.REGISTRY_TYPE].cat.categories
        # Generación de los elementos ordenados y filtrados a usar para reordenamiento
        items = [value for value in ORDERED_REGISTRY_TYPE if value in available_values]

        # Construcción de función a usar para la reasignación de columna con categorías ordenadas
        categorized_registry_type_assignation: ColumnAssignation = {
            COLUMN.REGISTRY_TYPE: (
                lambda df: (
                    df[COLUMN.REGISTRY_TYPE]
                    # Se forza la asignación de tipo de dato a categoría
                    .astype('category')
                    # Ordenamiento de categorías
                    .cat.reorder_categories(
                        items,
                        ordered= True
                    )
                )
            )
        }

        return (
            data
            # Reasignación de columna
            .assign(**categorized_registry_type_assignation)
        )

    def total_vacation_days(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de total de días de vacaciones
        Este método realiza un recuento de días de descanso, días festivos y, en base a
        éstos y a un rango de fechas en cada registro de justificaciones, realiza un
        conteo de total de días de vacaciones en donde las justificaciones sean de tipo
        *Vacaciones*.
        """

        # Funciones de conteo de tipos de día y obtención de total de días de vacaciones
        day_assignations: ColumnAssignation = {
            COLUMN.REST_DAYS: ( lambda df: df.apply(self._main._apply.count_rest_days, axis= 1, result_type= 'reduce') ),
            COLUMN.HOLIDAYS: ( lambda df: df.apply(self._main._apply.count_holidays, axis= 1, result_type= 'reduce') ),
            COLUMN.VACATION_DAYS: ( lambda df: df.apply(self._main._apply.count_vacation_days, axis= 1, result_type= 'reduce') ),
        }

        return (
            data
            # Se filtran los registros únicamente por los tipos de permiso de vacaciones
            .pipe(lambda df: df[ df[COLUMN.PERMISSION_TYPE] == PERMISSION_NAME.VACATION ])
            # Conteo de tipos de día y obtención de total de días de vacaciones
            .assign(**day_assignations)
        )

    def rename_permission_types(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Reasignación de columna con los nombres reemplazados
        renamed_permission_types: ColumnAssignation = {
            COLUMN.PERMISSION_TYPE: (
                lambda df: (
                    # Uso de la columna de tipo de permiso
                    df[COLUMN.PERMISSION_TYPE]
                    # Se convierte el tipo de dato a cadena de texto para perder referencia en categorías
                    .astype('string')
                    # Reemplazo de valores
                    .replace(PERMISSION_TYPE_REASSIGNATION_NAMES)
                )
            )
        }

        return (
            data
            .assign(**renamed_permission_types)
            # Asignación de tipos de datos para reasignar el tipo de dato como categoría
            .pipe(self._main._processing.assign_dtypes)
        )

    def _categorize_names(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Categorizar nombres
        Esta función reasigna la columna de nombre de usuario como tipo de categoría en
        base a los valores provistos a la memoria de usuarios.

        :param data DataFrame: Datos entrantes.
        """

        # Definición de función para reasignar o no nombres de categorías
        if self._main._names.categories:
            fn: SeriesPipe = lambda s: s.cat.set_categories(self._main._names.categories)
        else:
            fn: SeriesPipe = lambda s: s

        # Función para reasignar nombres como categorías
        reassign_names_as_categories: ColumnAssignation = {
            COLUMN.NAME: data[COLUMN.NAME].pipe(fn)
        }

        return (
            data
            .assign(**reassign_names_as_categories)
        )

    def _define_allowed_start_and_end_time(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Definición de horarios de inicio y fin permitidos
        Esta función calcula los horarios permitidos de inicio y fin para cada
        registro. Asigna el día de la semana, incorpora los horarios laborales
        estándar mediante una unión por día y aplica desfases específicos para ciertos
        usuarios. Los desfases ausentes se rellenan con un desplazamiento nulo.
        Finalmente combina la fecha del registro con los horarios base y sus ajustes
        para producir los valores completos de inicio y fin permitidos. El resultado
        conserva las columnas originales y añade las columnas de día de la semana y
        los horarios permitidos.

        :param data DataFrame: Datos entrantes.
        """

        # Asignación de día de la semana
        weekday_assignation: ColumnAssignation = {
            COLUMN.WEEKDAY: (
                lambda df: (
                    (
                        # Se convierte el tipo de dato a fecha de Pandas
                        pd.to_datetime( df[COLUMN.DATE] )
                        # Obtención de valor numérico de día de la semana
                        .dt.weekday
                    )
                )
            )
        }

        # Asignación de tiempo permitido
        allowed_time_assignation: ColumnAssignation = {
            # Inicio de tiempo permitido
            COLUMN.ALLOWED_START: (
                lambda df: (
                    # Se convierte el tipo de dato a fecha de Pandas
                    pd.to_datetime( df[COLUMN.DATE] )
                    # Se suma el tiempo de inicio general de jornada laboral
                    + df[COLUMN.START_SCHEDULE]
                    # Se suma el tiempo de desfase asignado al usuario
                    + df[COLUMN.START_OFFSET]
                )
            ),
            # Fin de tiempo permitido
            COLUMN.ALLOWED_END: (
                lambda df: (
                    # Se convierte el tipo de dato a fecha de Pandas
                    pd.to_datetime(df[COLUMN.DATE])
                    # Se suma el tiempo de fin general de jornada laboral
                    + df[COLUMN.END_SCHEDULE]
                    # Se suma el tiempo de desfase asignado al usuario
                    # (Los valores diferentes a cero son negativos, por eso se suman)
                    + df[COLUMN.END_OFFSET]
                )
            ),
        }

        return (
            data
            # Asignación de día de la semana
            .assign(**weekday_assignation)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Obtención de horarios laborales por día
            .pipe(self._get_daily_schedules)
            # Obtención de desfases de horarios para gerentes
            .pipe(self._get_managers_schedules)
            # Los usuarios que no tienen desfases requieren que sus valores sean 0 y no np.nan
            .replace({
                COLUMN.START_OFFSET: {np.nan: TIME_DELTA_ON_ZERO},
                COLUMN.END_OFFSET: {np.nan: TIME_DELTA_ON_ZERO}
            })
            # Asignación de tiempo permitido
            .assign(**allowed_time_assignation)
            # Selección de columnas
            [
                # Se seleccionan las columnas del DataFrame entrante
                data.columns.tolist()
                # Se suman la columna de día de la semana y los tiempos permitidos de inicio y final
                + [
                    COLUMN.WEEKDAY,
                    COLUMN.ALLOWED_START,
                    COLUMN.ALLOWED_END,
                ]
            ]
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _get_daily_schedules(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de horarios laborales por día
        Esta función asigna los horarios laborales establecidos a los registros del
        DataFrame entrante, en base al día de la semana de éstos.

        :param data DataFrame: Datos entrantes.
        """

        return (
            data
            # Obtención de horarios laborales por día
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= self._main._data.schedules,
                        left_on= COLUMN.WEEKDAY,
                        right_on= COLUMN.WEEKDAY,
                        how= 'left',
                    )
                )
            )
        )

    def _get_managers_schedules(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de desfases de horarios para gerentes
        Esta función asigna los desfases de horarios para gerentes o un desfase en 0
        para el resto de los empleados.

        :param data DataFrame: Datos entrantes.
        """

        return (
            data
            # Obtención de desfases de horarios para gerentes
            .pipe(
                lambda df: (
                    pd.merge(
                        left= df,
                        right= self._main._data.schedule_offsets,
                        left_on= [COLUMN.USER_ID, COLUMN.WEEKDAY],
                        right_on= [COLUMN.USER_ID, COLUMN.WEEKDAY],
                        how= 'left',
                    )
                )
            )
        )

    def _get_cummulated_time(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de tiempo acumulado de entrada tardía o salida anticipada
        Esta función calcula para cada registro si hubo entrada tardía o salida
        anticipada, y en esos casos determina cuántos minutos (o el intervalo
        correspondiente) representan esa entrada tardía o salida anticipada.

        :param data DataFrame: Datos entrantes.
        """

        # Clasificación de entradas tardías y salidas anticipadas
        is_late_or_early_start: ColumnAssignation = {
            # Validación de entrada tardía
            VALIDATION.IS_LATE_START: (
                lambda df: (
                    ( df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN )
                    & ( df[COLUMN.REGISTRY_TIME] > df[COLUMN.ALLOWED_START] )
                )
            ),
            # Validación de salida anticipada
            VALIDATION.IS_EARLY_END: (
                lambda df: (
                    ( df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_OUT )
                    & ( df[COLUMN.REGISTRY_TIME] < df[COLUMN.ALLOWED_END] )
                )
            ),
        }

        # Asignación de minutos de entrada tardía y salida anticipada
        late_and_early_time: ColumnAssignation = {
            # Obtención de minutos de entrada tardía
            COLUMN.LATE_TIME: (
                lambda df: (
                    ( df[COLUMN.REGISTRY_TIME] - df[COLUMN.ALLOWED_START] )
                    .where(
                        df[VALIDATION.IS_LATE_START],
                        TIME_DELTA_ON_ZERO,
                    )
                )
            ),
            # Obtención de minutos de salida anticipada
            COLUMN.EARLY_TIME: (
                lambda df: (
                    ( df[COLUMN.ALLOWED_END] - df[COLUMN.REGISTRY_TIME] )
                    .where(
                        df[VALIDATION.IS_EARLY_END],
                        TIME_DELTA_ON_ZERO,
                    )
                )
            ),
        }

        return (
            data
            # Clasificación de entradas tardías y salidas anticipadas
            .assign(**is_late_or_early_start)
            # Asignación de minutos de entrada tardía y salida anticipada
            .assign(**late_and_early_time)
        )

    def _get_lunch_intervals_total(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de total de intervalos de comida
        Esta función calcula, por cada usuario y día, la duración total de sus
        intervalos de comida. Para ello selecciona únicamente los registros de tiempo
        de comida, convierte las horas a intervalos acumulables y suma por separado los
        tiempos etiquetados como inicio y fin de comida. Luego reorganiza esos valores
        en un DataFrame pivote para obtener la duración efectiva de la pausa y
        finalmente determina si supera el límite permitido, produciendo una columna con
        el exceso correspondiente.

        :param data DataFrame: Datos entrantes.
        """

        # Literal de columna de diferencia
        _LUNCH_CUMMULATED_INTERVAL = '_lunch_cummulated_interval'

        # Asignación de tiempo de inicio y fin en formato timedelta
        time_in_delta_assignation: ColumnAssignation = {
            COLUMN.TIME_IN_DELTA: (
                lambda df: pd.to_timedelta( df[COLUMN.TIME] )
            ),
        }

        # Asignación de diferencia de intervalos de inicio y fin de tiempo de comida
        time_difference_assignation: ColumnAssignation = {
            _LUNCH_CUMMULATED_INTERVAL: (
                lambda pivoted_df: (
                    pivoted_df[REGISTRY_TYPE.BREAK_IN]
                    - pivoted_df[REGISTRY_TYPE.BREAK_OUT]
                )
            ),
        }

        # Asignación de tiempo excedente en tiempo de comida
        exceeding_lunch_time_assignation: ColumnAssignation = {
            COLUMN.EXCEEDING_LUNCH_TIME: (
                lambda pivoted_df: (
                    (
                        pivoted_df[_LUNCH_CUMMULATED_INTERVAL]
                        - LUNCH_DURATION_LIMIT
                    )
                    .where(
                        (
                            pivoted_df[_LUNCH_CUMMULATED_INTERVAL]
                            > LUNCH_DURATION_LIMIT
                        ),
                        TIME_DELTA_ON_ZERO,
                    )
                )
            )
        }

        return (
            data
            # Selección de registros que pertenecen a tiempo de comida
            .pipe(
                lambda df: (
                    df[ df[COLUMN.REGISTRY_TYPE].isin(LUNCH_REGISTRY_TYPES) ]
                )
            )
            # Obtención de tiempo de inicio y fin en formato timedelta
            .assign(**time_in_delta_assignation)
            # Suma de tiempo de inicio y de fin de registros
            .groupby(
                [COLUMN.USER_AND_DATE_INDEX, COLUMN.REGISTRY_TYPE],
                # Se descartan los tipos de registro de inicio y fin de jornada laboral
                observed= False,
            )
            # Suma de tiempos de inicio y de fin de registros por tipo de registro
            .agg( {COLUMN.TIME_IN_DELTA: 'sum'} )
            # Reseteo de índice
            .reset_index()
            # Conversión explícita de tipo de dato a timedelta
            .astype({COLUMN.TIME_IN_DELTA: 'timedelta64[ns]'})
            # Pivoteo de tabla para obtención de diferencias de tiempo entre intervalos
            .pivot_table(
                index= COLUMN.USER_AND_DATE_INDEX,
                columns= COLUMN.REGISTRY_TYPE,
                values= COLUMN.TIME_IN_DELTA,
                # Se descartan los tipos de registro de inicio y fin de jornada laboral
                observed= False,
            )
            # Asignación de diferencia de intervalos de inicio y fin de tiempo de comida
            .assign(**time_difference_assignation)
            # Obtención de total de tiempo excedente en tiempo de comida
            .assign(**exceeding_lunch_time_assignation)
            # Reseteo de índice
            .reset_index()
            # Selección de columnas
            [[
                COLUMN.USER_AND_DATE_INDEX,
                COLUMN.EXCEEDING_LUNCH_TIME,
            ]]
        )

    def _discard_duplicated(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Descartar duplicados
        Este método evalúa registros duplicados en base a la ID de usuario, fecha de
        registro y tipo de registro y etiqueta registros duplicados:
        - Se etiquetan todos los registros de inicio de jornada después del primero
        como duplicado
        - Se etiquetan todos los registros de fin de jordana antes del último como
        duplicado.
        """

        # Columnas temporales
        _CHECK_IN_DUPLICATED = '_check_in_duplicated'
        _CHECK_OUT_DUPLICATED = '_check_out_duplicated'

        # Columna que etiqueta todo tipo de registros duplicados con los criterios indicados
        is_duplicated: SeriesFromDataFrame = (
            lambda df: (
                df
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.DATE,
                    COLUMN.REGISTRY_TYPE,
                ]]
                # Se etiquetan registros duplicados
                .duplicated()
            )
        )
        # El registro es inicio de jornada
        is_check_in: SeriesFromDataFrame = (
            lambda df: (
                df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN
            )
        )
        # El registro es fin de jornada
        is_check_out: SeriesFromDataFrame = (
            lambda df: (
                df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_OUT
            )
        )

        # Asignación de duplicados
        tag_duplicated: ColumnAssignation = {
            COLUMN.IS_DUPLICATED: (
                lambda df: (
                    df[_CHECK_IN_DUPLICATED] | df[_CHECK_OUT_DUPLICATED]
                )
            )
        }

        # Función para ordenar inicio de jornada duplicado
        def tag_check_in(data: pd.DataFrame) -> pd.DataFrame:

            # Asignación de etiqueta de inicio de jornada duplicado
            check_in_duplicated: ColumnAssignation = {
                _CHECK_IN_DUPLICATED: (
                    lambda df: (
                        is_duplicated(df) & is_check_in(df)
                    )
                )
            }

            return (
                data
                # Ordenamiento de registros
                .sort_values(
                    [COLUMN.DATE, COLUMN.TIME],
                    ascending= [True, True],
                )
                # Asignación de etiqueta
                .assign(**check_in_duplicated)
            )

        def tag_check_out(data: pd.DataFrame) -> pd.DataFrame:

            # Asignación de etiqueta de fin de jornada duplicado
            check_out_duplicated: ColumnAssignation = {
                _CHECK_OUT_DUPLICATED: (
                    lambda df: (
                        is_duplicated(df) & is_check_out(df)
                    )
                )
            }

            return (
                data
                # Ordenamiento de registros
                .sort_values(
                    [COLUMN.DATE, COLUMN.TIME],
                    ascending= [True, False],
                )
                # Asignación de etiqueta
                .assign(**check_out_duplicated)
            )

        # Columnas seleccionadas
        selected_columns = list(data.columns) + [COLUMN.IS_DUPLICATED]

        return (
            data
            # Se etiquetan los registros duplicados
            .pipe(tag_check_in)
            .pipe(tag_check_out)
            # Asignación de etiqueta de duplicados con todas las evaluaciones
            .assign(**tag_duplicated)
            # Se descartan todos los registros duplicados
            .pipe(lambda df: df[~df[COLUMN.IS_DUPLICATED]])
            # Se ordenan los registros
            .sort_values(
                [COLUMN.DATE, COLUMN.TIME],
                ascending= [True, True],
            )
            # Se conservan las columnas originales
            [selected_columns]
        )

    def _validate_records(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Validación de registros
        Genera las validaciones por usuario/día y las incorpora a los datos
        originales. Primero asigna el índice compuesto usuario/día, luego calcula las
        validaciones correspondientes a cada combinación y finalmente une esos
        resultados con el DataFrame original para que cada registro incluya sus
        columnas de validación. El resultado es el conjunto completo de registros
        enriquecido con la información de integridad.

        :param data DataFrame: Datos entrantes.
        """

        # Obtención de los nombres de las validaciones en lista
        validation_names = list( VALIDATIONS_PER_DAY_AND_USER_ID.keys() )

        return (
            data
            # Asignación de índice por usuario y día de registro
            .pipe(self._assign_day_and_user_id_index)
            # Unión con validaciones
            .pipe(
                lambda df: (
                    pd.merge(
                        left= (
                            df
                            # Validación de registros por usuario/día
                            .pipe(self._validate_records_per_day_and_user)
                        ),
                        right= df,
                        left_on= COLUMN.USER_AND_DATE_INDEX,
                        right_on= COLUMN.USER_AND_DATE_INDEX,
                        how= 'left',
                    )
                )
            )
            # Selección de las columnas originales mas las validaciones
            [data.columns.to_list() + validation_names]
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _assign_day_and_user_id_index(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de índice por usuario y día de registro
        Esta función concatena los valores de ID de usuario y fecha de registro para
        usarlos como índice.

        Ejemplo:
        >>> data # DataFrame
        >>> #    user_id        date
        >>> # 0        5  2025-11-24
        >>> # 1        5  2025-11-24
        >>> # 2        5  2025-11-24
        >>> # 3        5  2025-11-24
        >>> # 4        5  2025-11-25
        >>> 
        >>> data.pipe(assign_day_and_user_id)
        >>> #    user_id        date  user_date_index
        >>> # 0        5  2025-11-24     5|2025-11-24
        >>> # 1        5  2025-11-24     5|2025-11-24
        >>> # 2        5  2025-11-24     5|2025-11-24
        >>> # 3        5  2025-11-24     5|2025-11-24
        >>> # 4        5  2025-11-25     5|2025-11-25

        :param data DataFrame: Datos entrantes.
        """

        # Conversión de tipos de dato a string
        user_id_column_str: pd.Series = data[COLUMN.USER_ID].astype('string')
        date_column_str: pd.Series = data[COLUMN.DATE].astype('string')

        return (
            data
            .assign(
                **{
                    COLUMN.USER_AND_DATE_INDEX: user_id_column_str + '|' + date_column_str
                }
            )
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _validate_records_per_day_and_user(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Validación de registros por usuario/día
        Esta función construye un DataFrame de validaciones por usuario y día. Toma el
        conjunto completo de registros, calcula los conteos por tipo de registro para
        cada índice usuario/día y aplica las reglas de integridad definidas. Devuelve
        únicamente el índice usuario/día junto con las columnas de validación
        resultantes.

        :param data DataFrame: Datos entrantes.
        """

        # Obtención de los nombres de las validaciones en lista
        validation_names = list( VALIDATIONS_PER_DAY_AND_USER_ID.keys() )
        # Se usa el índice usuario/día y las columnas generadas por las validaciones
        selected_columns = [COLUMN.USER_AND_DATE_INDEX] + validation_names

        return (
            data
            # Conteo por tipo de registro
            .pipe(self._count_per_registry_type)
            # Validación de registros por usuario/día
            .pipe(self._validate_day_pivoted_records)
            # Selección de columnas
            [selected_columns]
        )

    def _count_per_registry_type(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Conteo por tipo de registro
        Esta función realiza un pivoteo de tabla usando:

        - Índice: Índice de ID de usuario/fecha.
        - Columnas: Tipo de registro.
        - Valores: Conteo de tipos de registro por índice de ID de usuario/fecha.

        :param data DataFrame: Datos entrantes.
        """

        return (
            data
            .pipe(
                lambda df: (
                    df
                    # Agrupamiento por índice de ID de usuario/fecha y tipo de registro
                    .groupby(
                        [
                            COLUMN.USER_AND_DATE_INDEX,
                            COLUMN.REGISTRY_TYPE,
                        ],
                        observed= False,
                    )
                    # Conteo de registros por índice de ID de usuario/fecha
                    .agg( {COLUMN.USER_AND_DATE_INDEX: COLUMN.COUNT} )
                    # Se renombra la columna de índice de ID de usuario/fecha a 'conteo'
                    .rename(
                        columns= {COLUMN.USER_AND_DATE_INDEX: COLUMN.COUNT},
                    )
                    # Se restablece el índice del DataFrame
                    .reset_index()
                    # Se descartan todos los registros que no tengan tipo de registro especificado
                    .pipe(
                        lambda grouped_df: (
                            grouped_df[ grouped_df[COLUMN.REGISTRY_TYPE] != REGISTRY_TYPE.UNDEFINED ]
                        )
                    )
                )
            )
            # Pivoteo de tabla para obtención de conteos explícitos
            .pivot_table(
                index= COLUMN.USER_AND_DATE_INDEX,
                columns= COLUMN.REGISTRY_TYPE,
                values= COLUMN.COUNT,
                observed= False,
            )
            # Se restablece el índice del DataFrame
            .reset_index()
            # Se establecen a cero todos los np.nan
            .replace( {np.nan: 0} )
            # Se establecen los tipos de dato a entero de 8 bits
            .astype({
                REGISTRY_TYPE.CHECK_IN: 'uint8',
                REGISTRY_TYPE.BREAK_OUT: 'uint8',
                REGISTRY_TYPE.BREAK_IN: 'uint8',
                REGISTRY_TYPE.CHECK_OUT: 'uint8',
            })
        )

    def _validate_day_pivoted_records(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Validación de registros por usuario/día
        Esta función recibe un DataFrame pivote por usuario y día, y le inyecta
        validaciones produciendo columnas booleanas que indican si cada día/usuario
        pasa o no cada regla.

        - Validación de que existen los cuatro registros
        - Validación de que conteo de registros de comida son pares

        :param data DataFrame: Datos entrantes.
        """

        return (
            data
            # Se asignan las validaciones por día/ID de usuario
            .assign(
                **VALIDATIONS_PER_DAY_AND_USER_ID
            )
        )

    def _add_corrections(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Añadir correcciones
        Esta función orquesta el proceso completo de integración de correcciones sobre
        un conjunto de registros.

        :param data DataFrame: Datos entrantes.
        """

        return (
            data
            # Se descartan los registros corregidos
            .pipe(self._discard_corrected_records_from_original_data)
            # Se añaden las correcciones manuales
            .pipe(self._concat_corrections)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _concat_corrections(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Concatenación de correcciones
        Esta función toma el conjunto depurado, una vez filtrados los registros
        descartados por correcciones, y lo fusiona con el DataFrame que contiene las
        correcciones válidas. El propósito es reconstruir una secuencia completa donde
        registros originales y correcciones coexisten en un mismo flujo preparado para
        análisis posteriores.

        Durante la concatenación:
        - Se normaliza el indicador de "es corrección" para garantizar que todos los
        valores sean estrictamente booleanos, eliminando cualquier ambigüedad
        proveniente de valores nulos.
        - Se aplica ordenamiento de valores de tipo de registro para reestablecer el
        orden lógico entre tipos de registro, asegurando una estructura coherente.
        - Se ordenan todos los valores por tiempo de registro, lo que produce una línea
        temporal continua.
        - Se restaura el tipo categórico de la columna de dispositivo para preservar su
        semántica y eficiencia.

        El resultado es un DataFrame final que refleja con fidelidad la secuencia
        corregida de eventos, ya depurada y ordenada, listo para cualquier etapa
        posterior de procesamiento o análisis.

        :param data DataFrame: Datos entrantes.
        """

        # Asignación para convertir los valores np.nan a booleano
        force_booleans: ColumnAssignation = {
            COLUMN.IS_CORRECTION: (
                lambda df: (
                    ( df[COLUMN.IS_CORRECTION] )
                    .where(
                        df[COLUMN.IS_CORRECTION].isin([True, False]),
                        False
                    )
                )
            ),
            COLUMN.IS_DUPLICATED: (
                lambda df: (
                    ( df[COLUMN.IS_DUPLICATED] )
                    .where(
                        df[COLUMN.IS_DUPLICATED].isin([True, False]),
                        False
                    )
                )
            ),
        }

        return (
            # Se concatenan los registros y las correcciones
            pd.concat(
                [
                    data,
                    (
                        self._main._data.corrections
                        # Se toman los resultados dentro del rango de fechas de los registros
                        .pipe(
                            lambda df: (
                                df[
                                    (
                                        ( df[COLUMN.DATE] >= data[COLUMN.DATE].min() )
                                        & ( df[COLUMN.DATE] <= data[COLUMN.DATE].max() )
                                    )
                                ]
                            )
                        )
                    ),
                ]
            )
            # Se forza el tipo de dato a booleano en indicadores de "es corrección" y "es duplicado"
            .assign(**force_booleans)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Asignación de ordenamiento de valores de tipo de registro
            .pipe(self.assign_ordered_registry_type)
            # Ordenamiento por fecha de registro
            .sort_values(COLUMN.REGISTRY_TIME)
            # Se recupera el tipo de dato en la columna de dispositivo
            .astype( {COLUMN.DEVICE: 'category'} )
        )

    def _discard_corrected_records_from_original_data(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Descartar registros corregidos
        Esta función elimina del conjunto original de registros aquellos elementos que
        ya fueron corregidos. Para ello, realiza una concatenación entre el DataFrame
        de registros originales y el DataFrame de correcciones utilizando como claves
        el identificador de usuario, la fecha y la hora. Los registros presentes en el
        DataFrame de correcciones se marcan mediante una columna auxiliar y
        posteriormente se descartan. El resultado es un DataFrame que conserva
        únicamente los registros originales que no han sido reemplazados o corregidos.

        :param data DataFrame: Datos entrantes.
        """

        # Literal de indicador para descartar registros
        _TO_DROP = '_to_drop'

        return (
            data
            .pipe(
                lambda records_: (
                    # Se unen los DataFrames de registros y correcciones
                    pd.merge(
                        left= records_,
                        right= (
                            self._main._data.corrections
                            # Se asigna la columna indicadora de eliminación
                            .assign(**{_TO_DROP: True})
                            # Selección de columnas
                            [[
                                COLUMN.USER_ID,
                                COLUMN.DATE,
                                COLUMN.TIME,
                                _TO_DROP,
                            ]]
                        ),
                        # Para coincidir, los registros deben ser iguales en ID de usuario, fecha y hora
                        left_on= [COLUMN.USER_ID, COLUMN.DATE, COLUMN.TIME],
                        right_on= [COLUMN.USER_ID, COLUMN.DATE, COLUMN.TIME],
                        how= 'left',
                    )
                    # Se descartan todos los registros que no aparecen reasignados en correcciones
                    .pipe(lambda merged_df: merged_df[ merged_df[_TO_DROP].isna() ] )
                    # Selección de las columnas originales del DataFrame de registros
                    [records_.columns]
                )
            )
        )
