from datetime import (
    date,
    datetime,
)
from .._interface.resources import _Interface_DateSchema
from .._typing import PayFrequency

# TODO incluir identificador de espquema en curso
class _DateSchema(_Interface_DateSchema):

    def __init__(
        self,
        *,
        frequency: PayFrequency,
        start_date: datetime,
        end_date: datetime,
        name: str,
        current: bool,
    ) -> None:

        # Se guardan los valores provistos
        self.frequency = frequency
        self.start_date = start_date
        self.end_date = end_date
        self.name = name
        self.current = current

    def __repr__(
        self,
    ) -> str:

        # Obtención de valores a mostrar
        frequency = self.frequency
        start_date = self.start_date
        end_date = self.end_date
        name = self.name
        current = f'-current' if self.current else ''

        return f'DateSchema["{frequency}"{current}]({start_date}, {end_date}, {name})'

    def __contains__(
        self,
        date_value: str | date,
    ) -> bool:

        # Si el valor es una cadena de texto...
        if isinstance(date_value, str):
            # Se convierte éste a valor de fecha
            date_value = date.fromisoformat(date_value)

        # Validaciones
        ge = date_value >= self.start_date
        le = date_value <= self.end_date
        in_range = ge and le

        return in_range
