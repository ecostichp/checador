from pandas._typing import AstypeArg
from .._constants import (
    COLUMN,
    PERMISSION_NAME,
    REGISTRY_TYPE,
    WAREHOUSE_NAME,
)

ASSIGNED_DTYPES: dict[str, AstypeArg] = {
    COLUMN.ID: 'string[python]',
    COLUMN.USER_ID: 'uint16',
    COLUMN.NAME: 'category',
    COLUMN.DATE: 'datetime64[s]',
    COLUMN.TIME: 'timedelta64[ns]',
    COLUMN.REGISTRY_TYPE: 'category',
    COLUMN.DEVICE: 'category',
    COLUMN.PAY_FREQUENCY: 'category',
    COLUMN.IS_DUPLICATED: 'bool',
    COLUMN.REGISTRY_TIME: 'datetime64[ns]',
    COLUMN.IS_CORRECTION: 'bool',
    COLUMN.USER_AND_DATE_INDEX: 'string[python]',
    COLUMN.COUNT: 'uint64',
    COLUMN.WEEKDAY: 'category',
    COLUMN.START_SCHEDULE: 'timedelta64[ns]',
    COLUMN.END_SCHEDULE: 'timedelta64[ns]',
    COLUMN.START_OFFSET: 'timedelta64[ns]',
    COLUMN.END_OFFSET: 'timedelta64[ns]',
    COLUMN.ALLOWED_START: 'datetime64[ns]',
    COLUMN.ALLOWED_END: 'datetime64[ns]',
    COLUMN.LATE_TIME: 'timedelta64[ns]',
    COLUMN.EARLY_TIME: 'timedelta64[ns]',
    COLUMN.TIME_IN_DELTA: 'timedelta64[ns]',
    COLUMN.EXCEEDING_LUNCH_TIME: 'timedelta64[ns]',
    COLUMN.PERMISSION_TYPE: 'category',
    COLUMN.PERMISSION_START: 'datetime64[ns]',
    COLUMN.PERMISSION_END: 'datetime64[ns]',
    COLUMN.HOLIDAY_NAME: 'string[python]',
    COLUMN.HOLIDAY_DATE: 'datetime64[ns]',
    COLUMN.IS_CLOSED_CORRECT: 'bool',
    COLUMN.IS_CURRENT_DAY_CHECKIN: 'bool',
}
"""
`dict[str, AstypeArg]` Tipos de dato asignados a las columnas de DataFrames.
"""

ORDERED_REGISTRY_TYPE = [
    REGISTRY_TYPE.UNDEFINED,
    REGISTRY_TYPE.CHECK_IN,
    REGISTRY_TYPE.BREAK_OUT,
    REGISTRY_TYPE.BREAK_IN,
    REGISTRY_TYPE.CHECK_OUT,
]
"""
`list[Literal]` Orden de tipos de registro.
"""

LUNCH_REGISTRY_TYPES = [
    REGISTRY_TYPE.BREAK_OUT,
    REGISTRY_TYPE.BREAK_IN,
]
"""
`list[Literal]` Tipos de registro de tiempo de comida.
"""

DAY_PERMISSIONS = [
    PERMISSION_NAME.SICK_GENERAL,
    PERMISSION_NAME.WORK_RISK,
    PERMISSION_NAME.MATERNITY,
    PERMISSION_NAME.UNJUSTIFIED_ABSENCE,
    PERMISSION_NAME.UNPAID_EXTRA_ABSENCE,
    PERMISSION_NAME.HOLIDAY_ABSENCE,
    PERMISSION_NAME.HOLIDAY_COMPENSATION,
]
"""
`list[Literal]` Permisos por días.
"""

TIME_PERMISSIONS = [
    PERMISSION_NAME.HOURS_PERMISSION,
    PERMISSION_NAME.OVERTIME,
    PERMISSION_NAME.MEAL_BREAK_MISSING,
    PERMISSION_NAME.MEAL_BREAK_COMPENSATION,
]
"""
`list[Literal]` Permisos por horas.
"""

PERMISSION_TYPE_REASSIGNATION_NAMES = {
    'Vacaciones': PERMISSION_NAME.VACATION,
    'Incapacidad (Enfermedad general)': PERMISSION_NAME.SICK_GENERAL,
    'Incapacidad (Riesgo de trabajo)': PERMISSION_NAME.WORK_RISK,
    'Incapacidad (Maternidad)': PERMISSION_NAME.MATERNITY,
    'Ausencias laborales (Falta)': PERMISSION_NAME.UNJUSTIFIED_ABSENCE,
    'Ausencias laborales(Falta)': PERMISSION_NAME.UNJUSTIFIED_ABSENCE,
    'Ausencia extra (Sin gose y sin contar en bono)': PERMISSION_NAME.UNPAID_EXTRA_ABSENCE,
    'Ausencia por Día festivo': PERMISSION_NAME.HOLIDAY_ABSENCE,
    'Compensación por Día festivo': PERMISSION_NAME.HOLIDAY_COMPENSATION,
    'Permiso de horas': PERMISSION_NAME.HOURS_PERMISSION,
    'Horas extra': PERMISSION_NAME.OVERTIME,
    'Sin hora de comida (por reponer)': PERMISSION_NAME.MEAL_BREAK_MISSING,
    'Compensación hora de comida': PERMISSION_NAME.MEAL_BREAK_COMPENSATION,
    'Ausencia pago (Día festivo)': PERMISSION_NAME.HOLIDAY_ABSENCE,
}
"""
`dict[str, str]` Mapeo de reasignación de nombres para tipos de permiso
provenientes de los datos entrantes.
"""

ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES = {
    'Permiso o falta (Empleado):': COLUMN.NAME,
    'Tipo de permiso o falta': COLUMN.PERMISSION_TYPE,
    'Ingresa fecha de inicio': COLUMN.PERMISSION_START,
    'Ingresa fecha final': COLUMN.PERMISSION_END,
}
"""
`dict[str, str]` Mapeo de reasignación de nombres de columna de datos de
incidencias.
"""

WAREHOUSE_RENAME = {
    2: WAREHOUSE_NAME.CSL,
    3: WAREHOUSE_NAME.SJC,
}
"""
Mapa de reasignación de nombres de almacenes provenientes de Odoo.
"""
