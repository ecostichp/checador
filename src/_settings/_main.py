from datetime import date
from .._core import env

class CONFIG:
    TODAY = env.variable('TODAY', date.fromisoformat, date.today)

    class DATE_LIMITS:
        FIRST_HALF_MONTH_END = 15
        SECOND_HALF_MONTH_START = 16
