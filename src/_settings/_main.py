from datetime import date
from .._core import env

class MAIN_FILE:
    NAME = 'Checador_visualizaciones'

    class SHEET:
        USERS = 'Usuarios'
        COMPLETE_GENERAL_SUMMARY = 'datos'
        JUSTIFICATIONS_HISTORY = 'Incidencias'
        LUNCH_SUMMARY = 'Min comida'
        JUSTIFICATIONS_SUMMARY = 'Resumen Incidencias'

class CONFIG:
    TODAY = env.variable('TODAY', date.fromisoformat, date.today)

    class DATE_LIMITS:
        FIRST_HALF_MONTH_END = 15
        SECOND_HALF_MONTH_START = 16

SELECTED_DATABASE = env.variable('DATABASE')
