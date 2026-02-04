from datetime import (
    date,
    datetime,
)
from .._core import _Interface_DateSchema
from .._typing import PayFrequency

class _DateSchema(_Interface_DateSchema):

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

    def __init__(
        self,
        frequency: PayFrequency,
        start_date: datetime,
        end_date: datetime,
        name: str,
    ) -> None:

        # Se guardan los valores provistos
        self.frequency = frequency
        self.start_date = start_date
        self.end_date = end_date
        self.name = name

    def __repr__(
        self,
    ) -> str:

        return f'DateSchema["{self.frequency}"]({self.start_date}, {self.end_date}, {self.name})'
