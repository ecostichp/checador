from datetime import datetime
from typing import Literal
import pandas as pd
from ...typing.callables import ConnFunction
from ...typing.generics import _T
from ...typing.misc import RecordsLastDates

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

    def get_records_last_date_saved(
        self,
        warehouse_name: str,
    ) -> datetime:
        ...

    def update_last_update_dates(
        self,
        max_dates: RecordsLastDates,
    ) -> None:
        ...

    def _execute_on_connection(
        self,
        fn: ConnFunction[_T]
    ) -> _T:
        ...
