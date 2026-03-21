import pandas as pd
from ...typing import DataFramePipe
from ...typing.literals import ViewOptions
from ...typing.misc import DataTypeOrNone

class _Interface_Validations:

    def check_integrity(
        self,
    ) -> DataTypeOrNone[pd.DataFrame]:
        ...

    def filter_by_validity(
        self,
        /,
        view: ViewOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:
        ...

    def records_for_report(
        self,
    ) -> pd.DataFrame:
        ...

    def base_records_for_report(
        self,
    ) -> pd.DataFrame:
        ...
