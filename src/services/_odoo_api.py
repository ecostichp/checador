from odoo_api_manager import OdooAPIManager
import pandas as pd
from ..constants import COLUMN
from ..contracts.services import _Contract_OdooAPI

class OdooAPI(_Contract_OdooAPI):

    def __init__(
        self,
    ) -> None:

        # Inicialización del proxy de conexión a la API de Odoo
        self._api_manager = OdooAPIManager()

    def get_users(
        self,
    ) -> pd.DataFrame:

        return (
            # Obtención de la lista de empleados desde Odoo
            self._api_manager.search_read(
                # Modelo de empleados
                'hr.employee',
                # Campos
                fields= [
                    'name',
                    'x_pay_frequency',
                    'x_warehouse_id',
                    'job_id',
                ],
            )
            # Reasignación de nombres de columnas
            .rename(
                columns= {
                    'id': COLUMN.USER_ID,
                    'name': COLUMN.NAME,
                    'x_pay_frequency': COLUMN.PAY_FREQUENCY,
                    'x_warehouse_id': COLUMN.WAREHOUSE,
                    'job_id': COLUMN.JOB,
                },
            )
            # Ordenamiento y selección de columnas de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.WAREHOUSE,
                COLUMN.PAY_FREQUENCY,
                COLUMN.JOB,
            ]]
        )
