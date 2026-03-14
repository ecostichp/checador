from ._attendance import _Contract_Attendance
from ._database import _Contract_Database
from ._date import _Contract_Date
from ._excel import _Contract_Excel
from ._google_sheets import _Contract_GoogleSheets
from ._odoo_api import _Contract_OdooAPI

class _Contract_ServicesMain:
    attendance: _Contract_Attendance
    """`[Servicio]` Manejo de obtención de registros de asistencia."""
    database: _Contract_Database
    """`[Servicio]` Manejo de conexión a la base de datos."""
    date: _Contract_Date
    """`[Servicio]` Manejo de definición de dominio de fechas."""
    excel: _Contract_Excel
    """`[Servicio]` Manejo de obtención y exportación de archivos Excel."""
    google_sheets: _Contract_GoogleSheets
    """`[Servicio]` Manejo de conexión con Hojas de Cálculo de Google."""
    odoo_api: _Contract_OdooAPI
    """`[Servicio]` Manejo de conexión con la API de Odoo."""
