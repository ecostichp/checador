from datetime import date
from ..._typing import (
    DataFramePipe,
    PermissionTypeOption,
    ValidityOptions,
)
from .._interface import _Interface_DateSchema

class _Interface_Factory:
    """
    `[Submódulo]` Fábricas de funciones pipe para DataFrame.
    """

    def vacation_days(
        self,
        schema: _Interface_DateSchema,
    ) -> DataFramePipe:
        """
        ### Resumen de días de vacaciones
        Este método fabrica una función que realiza un resumen de días de vacaciones
        tomados por los usuarios seleccionados en el esquema de tiempo.

        :param schema _DateSchema: Esquema de tiempo.
        """
        ...

    def get_permissions_summary(
        self,
        perm_type: PermissionTypeOption,
        schema: _Interface_DateSchema,
    ) -> DataFramePipe:
        """
        ### Resumen de permisos
        Este método fabrica una función que realiza un resumen de permisos otorgados a
        los usuarios seleccionados en el esquema de tiempo.

        :param perm_type PermissionTypeOption: Tipo de permisos.
        :param schema _DateSchema: Esquema de tiempo.
        """
        ...

    def filter_by_validity(
        self,
        by: ValidityOptions,
    ) -> DataFramePipe:
        """
        ### Mantener registros válidos/inválidos
        Esta función filtra de manera rápida los registros que
        contengan todas las validaciones aprobadas o no aprobadas
        en base a cómo se configura.

        :param by ValidityOptions: Opciones de validación.
        """
        ...

    def get_records_in_range(
        self,
        start_date: date,
        end_date: date,
    ) -> DataFramePipe:
        """
        ### Extraer rangos
        Este método fabrica una función que extrae registros que cruzan dentro del
        rango las fechas especificadas en el esquema de tiempo.

        Para que se considere que un registro cruza el rango de fecha, su inicio o
        término de fecha deben encontrarse dentro del rango de fecha del esquema de
        tiempo provisto.

        Ejemplo de entrada:
        >>> data # DataFrame
        >>> #         start         end
        >>> # 0  2026-01-01  2026-01-08
        >>> # 1  2026-01-01  2026-01-15
        >>> # 2  2026-01-03  2026-01-25
        >>> # 3  2026-01-10  2026-01-16
        >>> # 4  2026-01-13  2026-01-25
        >>> # 5  2026-01-21  2026-01-23
        
        Se etiquetan los registros, por ejemplo, con el rango provisto entre
        `datetime(2026, 1, 10)` y `datetime(2026, 1, 20)`:
        >>> #         start         end  crosses_range
        >>> # 0  2026-01-01  2026-01-08          False
        >>> # 1  2026-01-01  2026-01-15           True
        >>> # 2  2026-01-03  2026-01-25           True
        >>> # 3  2026-01-10  2026-01-16           True
        >>> # 4  2026-01-13  2026-01-25           True
        >>> # 5  2026-01-21  2026-01-23          False

        Se filtran los registros en `True`:
        >>> #         start         end
        >>> # 1  2026-01-01  2026-01-15
        >>> # 2  2026-01-03  2026-01-25
        >>> # 3  2026-01-10  2026-01-16
        >>> # 4  2026-01-13  2026-01-25
        """
        ...
