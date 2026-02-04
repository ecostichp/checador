import pandas as pd
from pandas._typing import AstypeArg
from ..._typing import (
    ColumnAssignation,
    PermissionOptionGenericMap,
)

class _Interface_Processing:
    """
    `[Submódulo]` Funciones de procesamiento de datos.
    """

    _DIFF = '_diff'
    """
    Columna para cálculos temporales.
    """

    # Categorías asignables
    categories: PermissionOptionGenericMap[list[str]]
    # Tipos de dato asignables
    dtypes: PermissionOptionGenericMap[AstypeArg]
    # Función asignable para obtener la diferencia en el rango de fechas de cada permiso
    range_diff: PermissionOptionGenericMap[ColumnAssignation]
    # Datos de validaciones
    _validations: pd.DataFrame | None

    def assign_dtypes(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de tipos de datos
        Esta función asigna los tipos de datos establecidos para las columnas de un
        DataFrame y ordena los tipos de registro en caso de existir la columna de éstos.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def assign_ordered_registry_type(
        cls,
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
        facilitando comparaciones, ordenamientos y cualquier proceso que  dependa del
        orden lógico de los eventos.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def format_permission_date_strings(
        cls,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Formateo de fechas de justificaciones
        Este método convierte los valores de cadena de texto de las columnas de fecha
        de permiso en valores de tipo fecha.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def add_registry_time(
        cls,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Asignación de fecha y hora de registro
        Esta función concatena fecha y hora  en base a las columnas `'date'` y `'time'`
        de los registros.

        :param data DataFrame: Datos entrantes.
        """
        ...
