from typing import Union
from .._base.aliases import DatetimeStr
from .._base.literals import Devices
from .._base.generics import _T

RecordsLastDates = list[tuple[Devices, DatetimeStr]]
"""
Lista de tuplas de última fecha y hora de registros por almacén obtenidos desde
la API.
"""

DataTypeOrNone = Union[_T | None]
"""
Tipo de dato que indica un tipo `_T` o un `None`.
"""
