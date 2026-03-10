from ...typing.aliases import UserID
from ...typing.interfaces import HorizontalSeries
from ...typing.literals import NumericWeekday

class _Interface_Schedules:

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
