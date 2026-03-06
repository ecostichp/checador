from datetime import (
    date,
    timedelta,
)
from ..constants import ENV_VARIABLE
from ._env import env

class CONFIG:
    TODAY = env.variable(ENV_VARIABLE.TODAY, date.fromisoformat, date.today)

    class DATE_LIMITS:
        FIRST_HALF_MONTH_END = 15
        SECOND_HALF_MONTH_START = 16

LUNCH_DURATION_LIMIT = timedelta(hours= 1, seconds= 59)
"""
`timedelta` Límite de duración de tiempo de comida.
"""

class INPUT:
    class FORM:
        class PERMISSIONS:
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

class OUTPUT:
    class FILE:
        class VISUALIZATIONS:
            NAME = 'Checador_visualizaciones'
            class SHEET:
                USERS = 'Usuarios'
                COMPLETE_GENERAL_SUMMARY = 'datos'
                JUSTIFICATIONS_HISTORY = 'Incidencias'
                LUNCH_SUMMARY = 'Min comida'
                JUSTIFICATIONS_SUMMARY = 'Resumen Incidencias'

class DATA:
    class IDS:
        class WAREHOUSE:
            CSL = 2
            SJC = 3

SELECTED_DATABASE = env.variable('DATABASE')
