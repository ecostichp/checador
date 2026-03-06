import pandas as pd

class _Interface_Apply:
    """
    `[Submódulo]` Funciones de aplicación en columnas de DataFrame.
    """

    def count_rest_days(
        self,
        record_row: pd.Series,
    ) -> int:
        """
        ### Conteo de días de descanso
        Este método se aplica en una Pandas Series horizontal.

        Obtiene la ID de usuario, la fecha de inicio y la fecha de término y calcula
        los días de descanso existentes en este rango de fecha en base al usuario y sus
        días de descanso asignados.

        :param Series record_row: Pandas Series en formato horizontal.
        """
        ...

    def count_holidays(
        self,
        record_row: pd.Series,
    ) -> int:
        """
        ### Conteo de días festivos
        Este método se aplica en una Pandas Series horizontal.

        Obtiene la fecha de inicio y la fecha de término y calcula los días festivos
        existentes en este rango de fecha.

        :param Series record_row: Pandas Series en formato horizontal.
        """
        ...

    def count_vacation_days(
        self,
        record_row: pd.Series,
    ) -> int:
        """
        ### Conteo de días de vacaciones
        Este método se aplica en una Pandas Series horizontal.

        Obtiene la fecha de inicio, la fecha de término, el conteo de días de descanso
        y conteo de días festivos y calcula los días de vacaciones existentes en este
        rango de fecha y restando los conteos de días de descanso y días festivos.

        :param Series record_row: Pandas Series en formato horizontal.
        """
        ...
