from datetime import date

class _Contract_Date():

    today: date
    """`date` Fecha de hoy."""
    current_year: int
    """`int` Año en curso."""
    current_month: int
    """`int` Mes en curso."""
    month_start_date: date
    """`date` Fecha de inicio de mes."""
    month_end_date: date
    """`date` Fecha de fin de mes."""
    most_recent_available_date: date
    """`date` Fecha más reciente disponible."""
    first_week_start_date: date
    """`date` Fecha de inicio del primer ciclo semanal del mes."""
    first_week_end_date: date
    """`date` Fecha de fin del primer ciclo semanal del mes."""
