from datetime import (
    date,
    datetime,
)
from ...typing.literals import PayFrequency

class _Interface_DateSchema():

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

    current: bool
    """El esquema pertenece al día en curso."""

    def __repr__(
        self,
    ) -> str:
        ...

    def __contains__(
        self,
        date_value: str | date,
    ) -> bool:
        ...
