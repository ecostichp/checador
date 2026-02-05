import pandas as pd

class _Interface_Data:
    """
    `[Submódulo]` Acceso a objetos de datos en formato DataFrame.
    """

    users: pd.DataFrame
    records: pd.DataFrame
    corrections: pd.DataFrame
    justifications: pd.DataFrame
    holidays: pd.DataFrame
    schedules: pd.DataFrame
    schedule_offsets: pd.DataFrame

    def load(
        self,
    ) -> None:
        """
        ### Cargar datos
        Este método inicia la carga de datos para almacenarlos en DataFrames.
        """
        ...
