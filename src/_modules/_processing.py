from datetime import timedelta
import pandas as pd
from .._constants import COLUMN
from .._core import (
    _CoreRegistryProcessing,
    _Interface_Processing,
)
from .._mapping import (
    ASSIGNED_DTYPES,
    DAY_PERMISSIONS,
    ORDERED_REGISTRY_TYPE,
    TIME_PERMISSIONS,
)
from .._typing import (
    ColumnAssignation,
    DataFramePipe,
    SeriesApply,
)

class _Processing(_Interface_Processing):

    _DIFF = '_diff'
    """
    Columna para cálculos temporales.
    """
    _validations: pd.DataFrame | None
    """
    `DataFrame | None` Datos para validaciones.
    """

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de la instancia principal
        self._main = main

        self._initialize_functions()

    def assign_dtypes(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de tipos de datos
        Esta función asigna los tipos de datos establecidos para las columnas de
        un DataFrame y ordena los tipos de registro en caso de existir la
        columna de éstos.
        """

        # Generación del mapa de tipos de datos
        existing_dtypes = {
            column: dtype
            for ( column, dtype )
            in ASSIGNED_DTYPES.items()
            if column in data.columns
        }

        # Función para ordenar tipos de registro si es que la columna existe
        def reorder_registry_types(df: pd.DataFrame) -> pd.DataFrame:
            # Si existe columna de tipos de registro en el DataFrame...
            if COLUMN.REGISTRY_TYPE in df.columns:
                return (
                    df
                    # Asignación de ordenamiento de valores de tipo de registro
                    .pipe(self.assign_ordered_registry_type)
                )
            # Si no existe columna de tipos de registro en el DataFrame...
            else:
                return df

        # Función para ordenar días numéricos de la semana
        def reorder_weekdays(df: pd.DataFrame) -> pd.DataFrame:
            # Si existe columna de tipos de registro en el DataFrame...
            if COLUMN.WEEKDAY in df.columns:

                # Creación de función para obtener los días de la semana existentes en los valores
                def existing_weekdays(s: pd.Series) -> list[int]:
                    # Obtención de los valores únicos de días de la semana
                    unique_days = s.unique()
                    # Generación de las categorías ordenadas en base a los datos existentes
                    filtered_days = [wd for wd in range(7) if wd in unique_days]

                    return filtered_days

                # Función para ordernar los valores categóricos
                reorder_categoric_values: dict[str, DataFramePipe] = {
                    COLUMN.WEEKDAY: (
                        lambda df: (
                            df[COLUMN.WEEKDAY]
                            .astype('category')
                            # Ordenamiento de categorías
                            .cat.reorder_categories(
                                existing_weekdays(df[COLUMN.WEEKDAY]),
                                ordered= True,
                            )
                        )
                    )
                }

                return (
                    df
                    .assign(**reorder_categoric_values)
                )

            # Si no existe columna de tipos de registro en el DataFrame...
            else:
                return df

        return (
            data
            # Reasignación de tipos de dato
            .astype(existing_dtypes)
            # Reordenamiento de tipos de registro si es que existen
            .pipe(reorder_registry_types)
            # Reordenamiento de días numéricos de la semana si es que existen
            .pipe(reorder_weekdays)
        )

    def assign_ordered_registry_type(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de tipo de registro (ordenado)
        Esta función convierte la columna de tipo de registro en un tipo categórico
        ordenado.

        1. Identifica los tipos de registro presentes en el DataFrame.
        2. Se filtra según un orden ORDERED_REGISTRY_TYPE.
        3. Reasigna la columna como categoría y aplica ese orden.

        Esto permite trabajar con los tipos de registro de manera consistente,
        facilitando comparaciones, ordenamientos y cualquier proceso que 
        dependa del orden lógico de los eventos.
        """

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

    def format_permission_date_strings(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Asignación de columnas a formatear
        formatted_days: ColumnAssignation = {
            col: (
                    lambda df, closure_col= col: (
                    pd.to_datetime(
                        df[closure_col],
                        dayfirst= True,
                    )
                )
            ) for col in [
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            ]
        }

        return (
            data
            # Formateo de fechas provenientes de los documentos de Google Sheets
            .assign(**formatted_days)
        )

    def add_registry_time(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de fecha y hora de registro
        Esta función concatena fecha y hora  en base a las columnas `'date'` y
        `'time'` de los registros.
        """

        # Columna de fecha y hora de registro
        string_registry_time: ColumnAssignation = {
            COLUMN.REGISTRY_TIME: (
                lambda df: (
                    pd.to_datetime(
                        (
                            df
                            [COLUMN.DATE]
                            .astype('string[python]')
                        )
                        + ' '
                        + df[COLUMN.TIME]
                    )
                )
            )
        }

        return (
            data
            # Se asigna la columna de fecha y hora de registro
            .assign(**string_registry_time)
            # Asignación de tipos de datos
            .pipe(self.assign_dtypes)
        )

    def _initialize_functions(
        self,
    ) -> None:

        # Función para obtener la cantidad de días desde un timedelta
        days_from_delta: SeriesApply[timedelta] = lambda delta: delta.days + 1

        # Función para calcular la diferencia de tiempo en un rango de tiempo
        time_diff: ColumnAssignation = {
            self._DIFF: lambda df: (
                df[COLUMN.PERMISSION_END]
                - df[COLUMN.PERMISSION_START]
            )
        }

        # Función para contar los días en un rango de fechas
        count_days_in_range: DataFramePipe = (
            lambda df: (
                (
                    df[COLUMN.PERMISSION_END].dt.date
                    - df[COLUMN.PERMISSION_START].dt.date
                )
                .apply(days_from_delta)
            )
        )

        # Función para contar los días de descanso en un rango de fechas
        count_rest_days_in_range: DataFramePipe = (
            lambda df: (
                df
                .apply(
                    self._main._apply.count_rest_days,
                    axis= 1,
                    result_type= 'reduce',
                )
            )
        )

        # Función para calcular la diferencia de días en un rango de fechas
        days_diff: ColumnAssignation = {
            self._DIFF: (
                lambda df: (
                    count_days_in_range(df) - count_rest_days_in_range(df)
                )
            )
        }

        # Función asignable para obtener la diferencia en el rango de fechas de cada permiso
        self.range_diff = {
            'time': time_diff,
            'days': days_diff,
        }

        # Categorías asignables
        self.categories = {
            'time': TIME_PERMISSIONS,
            'days': DAY_PERMISSIONS,
        }

        # Tipos de dato asignables
        self.dtypes = {
            'time': 'timedelta64[ns]',
            'days': 'uint8',
        }
