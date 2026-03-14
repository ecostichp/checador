from typing import Iterator
from datetime import date
from ...resources import _DateSchema

class _Interface_DateSchemas:

    _today: date
    """
    Fecha de hoy.
    """
    _most_recent_available_date: date
    """
    Fecha más reciente disponible.
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

    @property
    def start_month(
        self,
    ) -> int:
        ...

    @property
    def end_month(
        self,
    ) -> int:
        ...

    @property
    def start_year(
        self,
    ) -> int:
        ...

    @property
    def end_year(
        self,
    ) -> int:
        ...

    @property
    def cross_months(
        self,
    ) -> bool:
        ...

    def __iter__(
        self,
    ) -> Iterator[_DateSchema]:
        ...
