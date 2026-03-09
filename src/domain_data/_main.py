from ..constants import WEEKDAY
from ..typing.aliases import UserID
from ..typing.literals import NumericWeekday

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

class PERMISSIONS_VALUES:
    class LABEL:
        VACATION = 'Vacaciones'
        SICK_GENERAL = 'Incapacidad (Enfermedad general)'
        WORK_RISK = 'Incapacidad (Riesgo de trabajo)'
        MATERNITY = 'Incapacidad (Maternidad)'
        UNJUSTIFIED_ABSENCE = 'Ausencias laborales (Falta)'
        UNJUSTIFIED_ABSENCE_ = 'Ausencias laborales(Falta)'
        UNPAID_EXTRA_ABSENCE = 'Ausencia extra (Sin gose y sin contar en bono)'
        HOLIDAY_ABSENCE = 'Ausencia por Día festivo'
        HOLIDAY_COMPENSATION = 'Compensación por Día festivo'
        HOURS_PERMISSION = 'Permiso de horas'
        OVERTIME = 'Horas extra'
        MEAL_BREAK_MISSING = 'Sin hora de comida (por reponer)'
        MEAL_BREAK_COMPENSATION = 'Compensación hora de comida'
        HOLIDAY_ABSENCE_ = 'Ausencia pago (Día festivo)'

    class COLUMN:
        NAME = 'Permiso o falta (Empleado):'
        PERMISSION_TYPE = 'Tipo de permiso o falta'
        PERMISSION_START = 'Ingresa fecha de inicio'
        PERMISSION_END = 'Ingresa fecha final'

class VISUALIZATIONS_FILE:
    NAME = 'Checador_visualizaciones'
    class SHEET:
        USERS = 'Usuarios'
        COMPLETE_GENERAL_SUMMARY = 'datos'
        JUSTIFICATIONS_HISTORY = 'Incidencias'
        LUNCH_SUMMARY = 'Min comida'
        JUSTIFICATIONS_SUMMARY = 'Resumen Incidencias'

class WAREHOUSE_IDS:
    CSL = 2
    SJC = 3

USERS_TO_DISCARD = [
    1,
]
