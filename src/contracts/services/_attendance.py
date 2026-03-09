from datetime import datetime
from ...typing.misc import DataTypeOrNone
from ..resources import _Contract_DataToSave

class _Contract_Attendance:

    def get_warehouse_records_from_api(
        self,
        warehouse_name: str,
        last_date_saved: datetime,
    ) -> DataTypeOrNone[_Contract_DataToSave]:
        ...
