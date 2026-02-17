import pandas as pd
from odoo_api_manager import OdooAPIManager
from ._interface import _CoreRegistryProcessing
from ._data import (
    REST_DAYS,
    USER_DEFAULT_REST_DAYS,
)
from ._modules import (
    _Apply,
    _DateSchemas,
    _Data,
    _Database,
    _Date,
    _Factory,
    _Names,
    _Pipes,
    _Processing,
    _Report,
    _Update,
    _Upload,
)
from ._typing import (
    UserID,
    NumericWeekday,
)

class RegistryProcessing(_CoreRegistryProcessing):

    def __init__(
        self,
    ) -> None:

        # Inicialización de submódulos de funciones
        self._apply = _Apply(self)
        self._pipes = _Pipes(self)
        self._factory = _Factory(self)
        self._processing = _Processing(self)

        # Inicialización de instancia de conexión a API de Odoo
        self._odoo = OdooAPIManager()
        # Incialización de módulo de fechas
        self._date = _Date(self)
        # Inicialización de módulo de nombres
        self._names = _Names()
        # Inicialización de módulo de datos
        self._data = _Data(self)
        # Inicialización de módulo de conexión a la base de datos
        self._database = _Database(self)
        # Inicialización de módulo de reportes
        self._report = _Report(self)
        # Inicialización de esquemas computados
        self._schemas = _DateSchemas(self)
        # Inicialización de módulo de actualización de datos
        self._update = _Update(self)
        # Inicialización de módulo de actualización de archivo
        self._upload = _Upload(self)

        # Se cargan los datos iniciales
        self._data.load()
        # Revisión de integridad de los datos
        self._to_verify = self._pipes.check_integrity()

    def report(
        self,
    ) -> None:
        """
        ### Generación de reportes
        Este método genera los reportes correspondientes en formato Excel.
        """

        # Se generan los reportes
        self._report.generate()

    def update(
        self,
    ) -> None:
        """
        ### Actualización
        Este método actualiza los datos en el archivo Google Sheets vinculado
        """

        # Actualización del archivo
        self._upload._update()

    @property
    def to_verify(
        self,
    ) -> pd.DataFrame | None:
        """
        ### Registros a verificar
        Este atributo contiene el DataFrame de registros a verificar o un valor en
        `None` en caso de no haber registros a verificar.
        """

        # Si no existe DataFrame de validaciones...
        if self._to_verify is None:
            # Se indica esto al usuario
            print('No hay validaciones para mostrar')
        # Si existe DataFrame de validaciones...
        else:
            # Se retorna éste
            return self._to_verify

    @property
    def data(
        self,
    ) -> _Data:
        """
        ### Datos
        Acceso los datos que se usan para procesamiento.
        """

        return self._data

    @property
    def schemas(
        self,
    ) -> _DateSchemas:
        """
        ### Esquemas de tiempo
        Acceso a los esquemas de tiempo usados para generación de reportes.
        """

        return self._schemas

    @property
    def reports(
        self,
    ) -> _Report:

        return self._report

    def _get_user_rest_days(
        self,
        user_id: UserID,
    ) -> list[NumericWeekday]:
        """
        ### Obtención de días de descanso
        Este método obtiene los días de decanso de un usuario en base a su ID provista.
        En caso de no encontrarse un dato especificado para este usuario se utiliza un
        valor prestablecido.
        """

        # Obtención de los datos de días de descanso
        rest_days = REST_DAYS.get(user_id, USER_DEFAULT_REST_DAYS)

        return rest_days
