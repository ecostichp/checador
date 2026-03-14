from ..contracts.services import _Contract_ServicesMain
from ._attendance import _Attendance
from ._date import DateService
from ._excel import Excel
from ._database import _Database
from ._google_sheets import _GoogleSheets
from ._odoo_api import OdooAPI

class Services(_Contract_ServicesMain):

    def __init__(
        self,
    ) -> None:

        # Inicialización del servicio de fecha
        self.date = DateService()
        # Inicialización de proxy de conexión a la API de Odoo
        self.odoo_api = OdooAPI()
        # Inicialización del servicio de conexión a la base de datos
        self.database = _Database()
        # Inicialización del servicio de conexión con Hojas de Cálculo
        self.google_sheets = _GoogleSheets()
        # Inicialización del servicio de conexión a la API de HikVision
        self.attendance = _Attendance()
        # Inicialización del servicio de obtención y exportación de archivos en Excel
        self.excel = Excel()

