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
    class VALUE:
        JUSTIFICATION = 'Incidencia'

class OUTPUT:
    class FILE:
        VISUALIZATIONS = VISUALIZATIONS_FILE

class DATA:
    class IDS:
        WAREHOUSE = WAREHOUSE_IDS

class REPORT:
    """
    `CONST` Nombres de reportes que se generan en Excel.
    """
    class VERIFICATION:
        """`CONST` Valores de reporte de verificaciones en Excel."""
        NAME = 'verification'

    class SUMMARY:
        """`CONST` Valores de reporte general en Excel."""
        NAME = 'resumen_de_registros'

        class SHEET:
            COMPLETE = 'Datos completos'
            '`Literal` Hoja de datos completos.'
            CUMMULATED_SUMMARY = 'Resumen'
            '`Literal` Hoja de resumen de acumulados.'
            JUSTIFICATIONS = 'Justificaciones'
            '`Literal` Hoja de resumen de incidencias.'
            MONTHLY_JUSTIFICATIONS = 'Incidencias del mes'
            '`Literal` Hoja de historial de incidencias.'
            USERS = 'Usuarios'
            '`Literal` Hoja de usuarios.'

class DATABASE:
    """
    `CONST` Nombres en base de datos.
    """
    class TABLE:
        """
        `CONST` Nombres de tablas en la base de datos.
        """
        ASSISTANCE_RECORDS = 'assistance_records'
        """`Literal` Tabla de registros de asistencia."""
        HOLIDAYS = 'holidays'
        """`Literal` Tabla de días festivos."""
        SCHEDULES = 'schedules'
        """`Literal` Tabla de horarios."""
        SCHEDULE_OFFSETS = 'schedule_offsets'
        """`Literal` Tabla de desfases de horarios."""
        LAST_UPDATE_DATES = 'last_update_dates'
        """`Literal` Tabla de última de hora de actualización en datos."""
