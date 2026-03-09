from datetime import (
    date,
    timedelta,
)
from ._env import env
from ..constants import ENV_VARIABLE
from ..domain_data import (
    PERMISSIONS_VALUES,
    VISUALIZATIONS_FILE,
    WAREHOUSE_IDS,
)

class CONFIG:
    SELECTED_DATABASE = env.variable(ENV_VARIABLE.SELECTED_DATABASE)
    """`str` Base de datos a usar."""

    TODAY = env.variable(ENV_VARIABLE.TODAY, date.fromisoformat, date.today)
    """`date` Día en curso."""

    LUNCH_DURATION_LIMIT = timedelta(hours= 1, seconds= 59)
    """
    `timedelta` Límite de duración de tiempo de comida.
    """

    class DATE_LIMITS:
        """Fechas límite."""
        FIRST_HALF_MONTH_END = 15
        """Día final de la primera quincena del mes."""
        SECOND_HALF_MONTH_START = 16
        """Día inicial de la segunda quincena del mes."""

class INPUT:
    class FORM:
        PERMISSIONS = PERMISSIONS_VALUES

class OUTPUT:
    class FILE:
        VISUALIZATIONS = VISUALIZATIONS_FILE

class DATA:
    class IDS:
        WAREHOUSE = WAREHOUSE_IDS
