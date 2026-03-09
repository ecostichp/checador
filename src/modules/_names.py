import pandas as pd
from ..constants import COLUMN
from ..contracts import _Interface_Names

class _Names(_Interface_Names):

    def __init__(
        self,
    ) -> None:
        ...

    def register_names(
        self,
        users: pd.DataFrame,
    ) -> pd.DataFrame:

        # Obtención de los nombres de los usuarios
        names: list[str] = (
            users
            [COLUMN.NAME]
            .to_list()
        )
        # Registro de los nombres para usarse como categorías
        self._update(names)

        return users

    def _update(
        self,
        names: list[str],
    ) -> None:
        """
        ### Actualización de valores
        Este método actualiza la lista de los valores categóricos con los datos
        provistos.
        """

        # Se asignan los nombres para usarlos en nuevas categorizaciones
        self.categories = names
