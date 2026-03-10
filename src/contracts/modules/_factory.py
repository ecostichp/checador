from ...resources import _DateSchema
from ...typing import DataFramePipe
from ...typing.literals import (
    PermissionTypeOption,
    ValidityOptions,
)

class _Interface_Factory:
    """
    `[Submódulo]` Fábricas de funciones pipe para DataFrame.
    """

    def get_permissions_summary(
        self,
        perm_type: PermissionTypeOption,
        schema: _DateSchema,
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
        /,
        by: ValidityOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:
        """
        ### Mantener registros válidos/inválidos
        Esta función filtra de manera rápida los registros que
        contengan todas las validaciones aprobadas o no aprobadas
        en base a cómo se configura.

        :param by ValidityOptions: Opciones de validación.
        :param keep_today_check_in bool: Mantener los registros `checkIn` de hoy.
        """
        ...
