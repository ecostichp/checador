import pandas as pd
from typing import Literal
from ...typing.callables import ConnFunction
from ...typing.generics import _T

class _Contract_Database:

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

    def _execute_on_connection(
        self,
        fn: ConnFunction[_T]
    ) -> _T:
        ...
