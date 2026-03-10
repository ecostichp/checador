import pandas as pd
from ..constants import COLUMN
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Transformation,
)
from ..typing import (
    ColumnAssignation,
    DataFramePipe,
)

class _Transformation(_Interface_Transformation):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def reassign_registry_type_categories(
        self,
        categories: list[str],
    ) -> DataFramePipe:

        # Función para establecer las categorías disponibles en la columna
        set_categories: ColumnAssignation = {
            COLUMN.PERMISSION_TYPE: (
                lambda df: (
                    # Se usa la columna de tipo de permiso
                    df[COLUMN.PERMISSION_TYPE]
                    # Se asignan las categorías provistas
                    .cat.set_categories(categories)
                )
            )
        }

        def fn(data: pd.DataFrame) -> pd.DataFrame:

            return (
                data
                # Se filtran los registros por las categorías provistas
                .pipe(
                    lambda df: (
                        df[ df[COLUMN.PERMISSION_TYPE].isin(categories) ]
                    )
                )
                # Se asignan las categorías provistas
                .assign(**set_categories)
            )

        return fn
