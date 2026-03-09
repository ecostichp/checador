import pandas as pd
from .modules import (
    _Interface_Apply,
    _Interface_Data,
    _Interface_DateSchemas,
    _Interface_Factory,
    _Interface_Names,
    _Interface_Pipes,
    _Interface_Processing,
    _Interface_Report,
    _Interface_Update,
)
from .services import _Contract_ServicesMain
from ..typing.aliases import UserID
from ..typing.literals import NumericWeekday

class _CoreRegistryProcessing:

    _schemas: _Interface_DateSchemas
    """
    Esquemas de tiempo creados dinámicamente para generar reportes.
    """
    _apply: _Interface_Apply
    """
    `[Submódulo]` Funciones de aplicación en columnas de DataFrame.
    """
    _data: _Interface_Data
    """
    `[Submódulo]` Acceso a objetos de datos en formato DataFrame.
    """
    _factory: _Interface_Factory
    """
    `[Submódulo]` Fábricas de funciones pipe para DataFrame.
    """
    _names: _Interface_Names
    """
    `[Submódulo]` Memoria de nombres de usuarios.
    """
    _pipes: _Interface_Pipes
    """
    `[Submódulo]` Funciones pipe para DataFrame.
    """
    _processing: _Interface_Processing
    """
    `[Submódulo]` Funciones de procesamiento de datos.
    """
    _update: _Interface_Update
    """
    `[Submódulo]` Módulo de actualización de datos.
    """
    _report: _Interface_Report
    """
    `[Submódulo]` Funciones de generación de reportes.
    """

    _services: _Contract_ServicesMain
    """Servicios de la clase principal."""

    def report(
        self,
    ) -> None:
        ...

    @property
    def data(
        self,
    ) -> _Interface_Data:
        ...

    def to_verify(
        self,
    ) -> pd.DataFrame | None:
        ...

    def _get_user_rest_days(
        self,
        user_id: UserID,
    ) -> list[NumericWeekday]:
        ...
