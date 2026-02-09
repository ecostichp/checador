from odoo_api_manager import OdooAPIManager
from ._modules import (
    _Interface_Apply,
    _Interface_Data,
    _Interface_Database,
    _Interface_DateSchemas,
    _Interface_Factory,
    _Interface_Names,
    _Interface_Pipes,
    _Interface_Processing,
    _Interface_Update,
    _Interface_Report,
    _Interface_Date,
)

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
    _database: _Interface_Database
    """
    `[Submódulo]` Conexión a base de datos.
    """
    _date: _Interface_Date
    """
    `[Submódulo]` Definición de fechas.
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
    _odoo: OdooAPIManager
    """
    `OdooAPIManager` Módulo de conexión a API de Odoo.
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
