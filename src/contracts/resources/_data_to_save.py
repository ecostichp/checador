import pandas as pd
from ...typing.literals import Devices

class _Contract_DataToSave():

    data: pd.DataFrame
    """Datos a guardar."""
    max_found_datetime: str
    """Máxima fecha de datos encontrada."""
    warehouse_name: Devices
    """Nombre corto del almacén del dispositivo."""
