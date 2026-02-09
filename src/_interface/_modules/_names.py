import pandas as pd

class _Interface_Names:
    """
    `[Submódulo]` Memoria de nombres de usuarios.
    """

    categories: list[str] = []
    """
    Valores categóricos disponibles.
    """

    def register_names(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Registrar nombres
        Este método toma los nombres de usuarios encontrados en los registros de
        usuarios y los almacena para ser usados posteriormente como valores categóricos
        en conversión de tipos de dato.
        """
        ...
