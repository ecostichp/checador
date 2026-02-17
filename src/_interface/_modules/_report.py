import pandas as pd

class _Interface_Report:
    """
    `[Submódulo]` Funciones de generación de reportes.
    """

    def generate(
        self,
    ) -> None:
        """
        ### Generar reporte
        Este método realiza la generación del reporte en Excel.
        """
        ...

    def complete_general_summary(
        self,
    ) -> pd.DataFrame:
        ...

    def lunch_summary(
        self,
    ) -> pd.DataFrame:
        ...

    def justfications_summmary(
        self,
    ) -> pd.DataFrame:
        ...
