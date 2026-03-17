from ...typing import DataFramePipe
from ...typing.literals import ViewOptions

class _Interface_Validations:

    def filter_for_view(
        self,
        /,
        view: ViewOptions,
        keep_today_check_in: bool = False,
    ) -> DataFramePipe:
        ...
