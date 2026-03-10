from ...resources import _DateSchema
from ...typing import DataFramePipe
from ...typing.aliases import UserID
from ...typing.interfaces import HorizontalSeries
from ...typing.literals import NumericWeekday



class _Interface_Schedules:

    def vacation_days(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:
        ...

    def get_permissions_in_date_range(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:
        ...

    def cut_justifications_date_ranges(
        self,
        schema: _DateSchema,
    ) -> DataFramePipe:
        ...

    def count_rest_days(
        self,
        record_row: HorizontalSeries,
    ) -> int:
        ...

    def count_holidays(
        self,
        record_row: HorizontalSeries,
    ) -> int:
        ...

    def count_vacation_days(
        self,
        record_row: HorizontalSeries,
    ) -> int:
        ...

    def _get_user_rest_days(
        self,
        user_id: UserID,
    ) -> list[NumericWeekday]:
        ...
