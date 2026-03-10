import pandas as pd
from ..constants import COLUMN
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Factory,
)
from ..resources import _DateSchema

from ..typing import DataFramePipe
from ..typing.literals import PermissionTypeOption

class _Factory(_Interface_Factory):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def get_permissions_summary(
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
