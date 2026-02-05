from .._constants import (
    COLUMN,
    WEEKDAY,
)
from .._typing import (
    DatetimeStr,
    UserID,
    NumericWeekday,
)

REST_DAYS: dict[UserID, list[NumericWeekday]] = {
    36: [
        WEEKDAY.SATURDAY,
        WEEKDAY.SUNDAY,
    ],
}
"""
`dict[UserID, list[NumericWeekday]]` Días de descanso por ID de usuario.
"""

USER_DEFAULT_REST_DAYS = [
    WEEKDAY.SUNDAY,
]
"""
`list[NumericWeekday]` Días de descanso predeterminados para los usuarios.
"""

WEEK_PERIOD_END = WEEKDAY.FRIDAY
"""
`Literal` Día de término de ciclo semanal.
"""

class DEVICE_SERIAL_NUMBER:
    """
    `CONST` Números de serie de dispositivos.
    """
    CSL = 'G97954302'
    SJC = 'G97954418'
