import pandas as pd
from .modules import (
    _Interface_Data,
    _Interface_DateSchemas,
    _Interface_Factory,
    _Interface_Pipes,
    _Interface_Processing,
    _Interface_Report,
    _Interface_Schedules,
    _Interface_Transformation,
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
    _data: _Interface_Data
    """
    `[Submódulo]` Acceso a objetos de datos en formato DataFrame.
    """
    _factory: _Interface_Factory
    """
    `[Submódulo]` Fábricas de funciones pipe para DataFrame.
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
    _schedules: _Interface_Schedules
    """
    `[Submódulo]` Funciones de horarios y fechas.
    """
    _transformation: _Interface_Transformation
    """
    `[Submódulo]` Funciones de transformación de datos.
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
