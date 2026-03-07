from odoo_api_manager import OdooAPIManager
from ._database import _Contract_Database
from ._date import _Contract_Date

class _Contract_ServicesMain:
    database: _Contract_Database
    date: _Contract_Date
    odoo: OdooAPIManager
