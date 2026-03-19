import pandas as pd
from ..constants import (
    COLUMN,
    PERMISSION_NAME,
    REGISTRY_TYPE,
    TIME_DELTA_ON_ZERO,
)
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Pipes,
)
from ..mapping import (
    LUNCH_REGISTRY_TYPES,
    ORDERED_REGISTRY_TYPE,
    PERMISSION_TYPE_REASSIGNATION_NAMES,
)
from ..typing import ColumnAssignation
from ..settings import CONFIG

class _Pipes(_Interface_Pipes):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

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
                )
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

    def assign_ordered_registry_type(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Obtención de los valores categóricos encontrados en el DataFrame
        available_values = data[COLUMN.REGISTRY_TYPE].cat.categories
        # Generación de los elementos ordenados y filtrados a usar para reordenamiento
        items = [value for value in ORDERED_REGISTRY_TYPE if value in available_values]

        # Función para reasignación y ordenamiento de categorías de tipo de registro
        def _handle_category_integrity(s: pd.Series) -> pd.Series:

            # Si no existe registro de fin de jornada laboral...
            if 'checkOut' not in items:
                # Se agrega éste a las categorías a asignar (Se espera que el resto vaya)
                new_items = [
                    REGISTRY_TYPE.UNDEFINED,
                    REGISTRY_TYPE.CHECK_IN,
                    REGISTRY_TYPE.BREAK_OUT,
                    REGISTRY_TYPE.BREAK_IN,
                    REGISTRY_TYPE.CHECK_OUT,
                ]

                return (
                    s
                    # Se añade la categoría de fin de jornada laboral
                    .cat.add_categories([REGISTRY_TYPE.CHECK_OUT])
                    # Ordenamiento de categorías
                    .cat.reorder_categories(
                        new_items,
                        ordered= True
                    )
                )
            else:

                return (
                    s
                    # Ordenamiento de categorías
                    .cat.reorder_categories(
                        items,
                        ordered= True
                    )
                )

        # Construcción de función a usar para la reasignación de columna con categorías ordenadas
        categorized_registry_type_assignation: ColumnAssignation = {
            COLUMN.REGISTRY_TYPE: (
                lambda df: (
                    df[COLUMN.REGISTRY_TYPE]
                    # Se forza la asignación de tipo de dato a categoría
                    .astype('category')
                    # Reasignación y ordenamiento de categorías
                    .pipe(_handle_category_integrity)
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
            COLUMN.REST_DAYS_COUNT: ( lambda df: df.apply(self._main._schedules.count_rest_days, axis= 1, result_type= 'reduce') ),
            COLUMN.HOLIDAYS_COUNT: ( lambda df: df.apply(self._main._schedules.count_holidays, axis= 1, result_type= 'reduce') ),
            COLUMN.VACATION_DAYS_COUNT: ( lambda df: df.apply(self._main._schedules.count_vacation_days, axis= 1, result_type= 'reduce') ),
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
                        - CONFIG.LUNCH_DURATION_LIMIT
                    )
                    .where(
                        (
                            pivoted_df[_LUNCH_CUMMULATED_INTERVAL]
                            > CONFIG.LUNCH_DURATION_LIMIT
                        ),
                        TIME_DELTA_ON_ZERO,
                    )
                )
            )
        }

        # Obtención de los registros únicamente de hora de comida
        lunch_records = (
            data
            # Selección de registros que pertenecen a tiempo de comida
            .pipe(
                lambda df: (
                    df[ df[COLUMN.REGISTRY_TYPE].isin(LUNCH_REGISTRY_TYPES) ]
                )
            )
        )

        # Si existen registros de hora de comida...
        if len(lunch_records):

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

        # Si no existen registros de hora de comida...
        else:

            # Se retorna DataFrame vacío para poder manejar los merge posteriores
            return (
                pd.DataFrame(
                        {
                        COLUMN.USER_AND_DATE_INDEX: [],
                        COLUMN.EXCEEDING_LUNCH_TIME: [],
                    }
                )
                .astype({
                    COLUMN.USER_AND_DATE_INDEX: 'string',
                    COLUMN.EXCEEDING_LUNCH_TIME: 'timedelta64[ns]',
                })
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
