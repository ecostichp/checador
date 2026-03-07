from odoo_api_manager import OdooAPIManager
from ._date import DateService
from ..contracts.services import _Contract_ServicesMain

class Services(_Contract_ServicesMain):

    def __init__(
        self,
    ) -> None:

        # Inicialización del servicio de fecha
        self.date = DateService()
        # Inicialización de proxy de conexión a la API de Odoo
        self.odoo = OdooAPIManager()
