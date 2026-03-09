from odoo_api_manager import OdooAPIManager
from ._attendance import _Contract_Attendance
from ._database import _Contract_Database
from ._date import _Contract_Date
from ._google_sheets import _Contract_GoogleSheets

class _Contract_ServicesMain:
    attendance: _Contract_Attendance
    """`[Servicio]` Manejo de obtención de registros de asistencia."""
    database: _Contract_Database
    """`[Servicio]` Manejo de conexión a la base de datos."""
    date: _Contract_Date
    """`[Servicio]` Manejo de definición de dominio de fechas."""
    google_sheets: _Contract_GoogleSheets
    """`[Servicio]` Manejo de conexión con Hojas de Cálculo de Google."""
    odoo: OdooAPIManager
    """`[Servicio]` Manejo de conexión con la API de Odoo."""
