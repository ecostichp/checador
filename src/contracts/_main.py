import pandas as pd
from .modules import (
    _Interface_Data,
    _Interface_DateSchemas,
    _Interface_Pipes,
    _Interface_Processing,
    _Interface_Report,
    _Interface_Schedules,
    _Interface_Transformation,
    _Interface_Update,
    _Interface_Validations,
)
from ..typing.aliases import UserID
from ..typing.literals import NumericWeekday
from .pipes import _Contract_PipeMethods
from .services import _Contract_ServicesMain

class _CoreRegistryProcessing:

    _schemas: _Interface_DateSchemas
    """
    Esquemas de tiempo creados dinámicamente para generar reportes.
    """
    _data: _Interface_Data
    """
    `[Submódulo]` Acceso a objetos de datos en formato DataFrame.
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
    _validations: _Interface_Validations
    """
    `[Submódulo]` Funciones de validación de datos.
    """

    _services: _Contract_ServicesMain
    """Servicios de la clase principal."""

    _pipe_methods: _Contract_PipeMethods
    """
    `[Pipes]` Pipes para procesamientos de Pandas DataFrames.
    """

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
