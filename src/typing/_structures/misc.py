from .._base.aliases import DatetimeStr
from .._base.literals import Devices

RecordsLastDates = list[tuple[Devices, DatetimeStr]]
"""
Lista de tuplas de última fecha y hora de registros por almacén obtenidos desde
la API.
"""
