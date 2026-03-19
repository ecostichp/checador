import pandas as pd
from IPython.display import display
from .constants import COLUMN
from .contracts import _CoreRegistryProcessing
from .domain_data import (
    REST_DAYS,
    USER_DEFAULT_REST_DAYS,
)
from .modules import (
    _DateSchemas,
    _Data,
    _Pipes,
    _Processing,
    _Report,
    _Schedules,
    _Transformation,
    _Update,
    _Validations,
)
from .pipes import PipeMethods
from .services import Services
from .templates.messages import MESSAGE
from .typing import ColumnAssignation
from .typing.aliases import UserID
from .typing.literals import NumericWeekday

class RegistryProcessing(_CoreRegistryProcessing):

    def __init__(
        self,
    ) -> None:

        # Inicialización de submódulos de funciones
        self._pipes = _Pipes(self)
        self._processing = _Processing(self)

        # Inicialización de servicios
        self._services = Services()

        # Inicialización de métodos tipo pipes
        self._pipe_methods = PipeMethods(self)

        # Inicialización de módulo de datos
        self._data = _Data(self)
        # Inicialización de módulo de reportes
        self._report = _Report(self)
        # Inicialización de funciones de horarios
        self._schedules = _Schedules(self)
        # Inicialización de esquemas computados
        self._schemas = _DateSchemas(self)
        # Inicialización de funciones de transformación de datos
        self._transformation = _Transformation(self)
        # Inicialización de módulo de actualización de datos
        self._update = _Update(self)
        # Inicialización de funciones de validación de datos
        self._validations = _Validations(self)

        # Se cargan los datos iniciales
        self._data.load()
        # Revisión de integridad de los datos
        self._to_verify = self._validations.check_integrity()
        # Obtención de registros base para reporte
        self._records_for_report = self._validations.records_for_report()
        # Revisión de días con apertura tardía
        self._check_late_open()

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

        # Uso del servicio de conexión con Google Sheets para actualización
        self._services.google_sheets.update(self)

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
            print(MESSAGE.NO_VALIDATIONS_TO_SHOW)
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

    def _check_late_open(
        self,
    ) -> None:

        # Búsqueda de días con apertura tardía
        found_results = self._search_for_late_open()

        # Si existen registros de días con apertura tardía...
        if len(found_results):
            # Se muestra un mensaje
            print(MESSAGE.LATE_OPEN_FOUND)
            # Se muestran los registros
            display(found_results)

    def _search_for_late_open(
        self,
    ) -> None:

        # Obtención del resumen de registros completo
        summary = self._report.complete_general_summary()

        # Función para asignar columna de día de la semana
        weekday: ColumnAssignation = {
            COLUMN.WEEKDAY: (
                lambda df: (
                    df
                    [COLUMN.DATE]
                    .dt.weekday
                )
            )
        }

        return (
            summary
            # Agrupamiento por fecha y sucursal
            .groupby(
                [
                    COLUMN.DATE,
                    COLUMN.DEVICE,
                ],
                observed= False,
            )
            # Se busca el primer valor de hora
            [COLUMN.TIME]
            .first()
            # Reseteo de índice
            .reset_index()
            # Obtención del día de la semana
            .assign(**weekday)
            # Unión con datos de horarios para obtener hora de entrada
            .merge(
                right= (
                    # Uso de los datos de horarios de la semana
                    self._data.schedules
                    # Selección de columnas
                    [[COLUMN.WEEKDAY, COLUMN.START_SCHEDULE]]
                ),
                on= COLUMN.WEEKDAY,
                how= 'left',
            )
            # Se filtran las fechas cuyo primer registro sea posterior al inicio de jornada laboral
            .pipe(lambda df: df[df[COLUMN.TIME] > df[COLUMN.START_SCHEDULE]])
            # Selección de columnas
            [[
                COLUMN.DEVICE,
                COLUMN.DATE,
                COLUMN.TIME,
            ]]
        )
