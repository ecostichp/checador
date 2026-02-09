from datetime import date

class _Interface_Date():

    today: date
    """Fecha de hoy."""
    current_year: int
    """
    `int` Año en curso.
    """
    current_month: int
    """
    `int` Mes en curso.
    """
    month_start_date: date
    """
    `date` Fecha de inicio de mes.
    """
    month_end_date: date
    """
    `date` Fecha de fin de mes.
    """
    most_recent_available_date: date
    """
    `date` Fecha más reciente disponible.
    """
