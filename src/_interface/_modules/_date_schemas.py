from typing import Iterator
from datetime import date
from . import _Interface_DateSchema

class _Interface_DateSchemas:

    _today: date
    """
    `date` Fecha de hoy.
    """
    _most_recent_available_date: date
    """
    `date` Fecha más reciente disponible.
    """

    def min_date(
        self,
    ) -> date:
        """
        ### Fecha mínima
        Obtención del valor mínimo de fecha encontrado en los esquemas individiduales.
        """
        ...

    def max_date(
        self,
    ) -> date:
        """
        ### Fecha máxima
        Obtención del valor máximo de fecha encontrado en los esquemas individiduales.
        """
        ...

    def __iter__(
        self,
    ) -> Iterator[_Interface_DateSchema]:
        ...
