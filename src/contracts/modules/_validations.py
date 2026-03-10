from ...typing import DataFramePipe
from ...typing.literals import ValidityOptions

class _Interface_Validations:

    def filter_by_validity(
        self,
        /,
        by: ValidityOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:
        ...
