from ...resources import _DateSchema
from ...typing import DataFramePipe
from ...typing.literals import PermissionTypeOption

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
