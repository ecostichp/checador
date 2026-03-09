import pandas as pd
from ..typing.literals import Devices
from ..contracts.resources import _Contract_DataToSave

class DataToSave(_Contract_DataToSave):

    def __init__(
        self,
        *,
        data: pd.DataFrame,
        max_found_datetime: str,
        warehouse_name: Devices,
    ) -> None:

        # Se almacenan los datos
        self.data = data
        self.max_found_datetime = max_found_datetime
        self.warehouse_name = warehouse_name
