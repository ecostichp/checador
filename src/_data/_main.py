from datetime import timedelta
import pandas as pd
from .._constants import (
    COLUMN,
    WEEKDAY,
)
from .._typing import (
    DatetimeStr,
    UserID,
    NumericWeekday,
)

SCHEDULES = (
    pd.DataFrame(
        {
            COLUMN.WEEKDAY: [
                WEEKDAY.MONDAY,
                WEEKDAY.TUESDAY,
                WEEKDAY.WEDNESDAY,
                WEEKDAY.THURSDAY,
                WEEKDAY.FRIDAY,
                WEEKDAY.SATURDAY,
            ],
            COLUMN.START_SCHEDULE: [
                timedelta(hours= 8, seconds= 59),
                timedelta(hours= 8, seconds= 59),
                timedelta(hours= 8, seconds= 59),
                timedelta(hours= 8, seconds= 59),
                timedelta(hours= 8, seconds= 59),
                timedelta(hours= 8, seconds= 59),
            ],
            COLUMN.END_SCHEDULE: [
                timedelta(hours= 18),
                timedelta(hours= 18),
                timedelta(hours= 18),
                timedelta(hours= 18),
                timedelta(hours= 18),
                timedelta(hours= 16),
            ],
        },
    )
    .assign(
        **{
            COLUMN.WEEKDAY: lambda df: df[COLUMN.WEEKDAY].astype('category'),
            COLUMN.START_SCHEDULE: lambda df: df[COLUMN.START_SCHEDULE].astype('timedelta64[ns]'),
            COLUMN.END_SCHEDULE: lambda df: df[COLUMN.END_SCHEDULE].astype('timedelta64[ns]'),
        }
    )
)
"""
`DataFrame` Horarios laborales por día.
"""

MANAGERS_SCHEDULES = (
    pd.DataFrame({
        COLUMN.USER_ID: [
            23,
            29,
            10,
        ],
        COLUMN.WEEKDAY: [
            WEEKDAY.SATURDAY,
            WEEKDAY.SATURDAY,
            WEEKDAY.SATURDAY,
        ],
        COLUMN.START_OFFSET: [
            timedelta(hours= 2),
            timedelta(hours= 2),
            timedelta(hours= 0),
        ],
        COLUMN.END_OFFSET: [
            timedelta(hours= 0),
            timedelta(hours= 0),
            timedelta(hours= -2),
        ],
    })
    .assign(
        **{
            COLUMN.USER_ID: lambda df: df[COLUMN.USER_ID].astype('uint16'),
            COLUMN.WEEKDAY: lambda df: df[COLUMN.WEEKDAY].astype('category'),
            COLUMN.START_OFFSET: lambda df: df[COLUMN.START_OFFSET].astype('timedelta64[ns]'),
            COLUMN.END_OFFSET: lambda df: df[COLUMN.END_OFFSET].astype('timedelta64[ns]'),
        }
    )
)
"""
`DataFrame` Desfases de horarios laborales para gerentes.
"""

REST_DAYS: dict[UserID, list[NumericWeekday]] = {
    36: [
        WEEKDAY.SATURDAY,
        WEEKDAY.SUNDAY,
    ],
}
"""
`dict[UserID, list[NumericWeekday]]` Días de descanso por ID de usuario.
"""

HOLIDAYS: list[DatetimeStr] = [
    # Año nuevo
    '2026-01-01',
]
"""
`list[DatetimeStr]` Días festivos.
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
