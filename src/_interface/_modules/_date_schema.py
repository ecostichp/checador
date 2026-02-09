from datetime import (
    date,
    datetime,
)
from ..._typing import PayFrequency

class _Interface_DateSchema:

    frequency: PayFrequency
    """
    Frecuencia de pago.

    Valores disponibles:
    - `'weekly'`: Semanal.
    - `'biweekly'`: Quincenal.
    """
    start_date: datetime
    """Fecha de inicio."""
    end_date: datetime
    """Fecha de término."""
    name: str
    """Nombre usado para generar reportes."""

    _today: date
    """Fecha de hoy."""
