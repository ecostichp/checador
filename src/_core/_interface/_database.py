import pandas as pd
from typing import Literal

class _Interface_Database():
    """
    `[Submódulo]` Conexión a base de datos.
    """

    def load_data_from_query(
        self,
        query: str,
    ) -> pd.DataFrame:
        ...

    def save_in_database(
        self,
        data: pd.DataFrame,
        table_name: str,
        if_exists: Literal['fail', 'replace', 'append'],
    ) -> None:
        ...
