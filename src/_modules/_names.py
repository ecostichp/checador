import pandas as pd
from .._constants import COLUMN
from .._core import _Interface_Names

class _Names(_Interface_Names):

    categories: list[str] = []

    def __init__(
        self,
    ) -> None:
        ...

    def register_names(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        # Obtención de los nombres de los usuarios
        names: list[str] = (
            data
            [COLUMN.NAME]
            .to_list()
        )
        # Registro de los nombres para usarse como categorías
        self._update(names)

        return data

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
