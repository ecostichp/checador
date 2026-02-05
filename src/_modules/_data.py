import pandas as pd
from ccc_utils import spreadsheet
from ..utils import path_from_dropbox
from .._constants import (
    COLUMN,
    DATABASE,
)
from .._core import (
    _CoreRegistryProcessing,
    _Interface_Data,
)
from .._mapping import ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES
from .._templates import (
    EXCEL_FILE,
    QUERY,
    SPREADSHEET,
)
from .._typing import ColumnAssignation
from ..sql import load_from_database

class _Data(_Interface_Data):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def load(
        self,
    ) -> None:

        # Carga de datos
        self.users = self._load_users()
        self.records = self._load_records()
        self.corrections = self._load_corrections()
        self.justifications = self._load_justifications()
        self.holidays = self._load_holidays()
        self.schedules = self._load_schedules()
        self.schedule_offsets = self._load_schedule_offsets()

    def _load_users(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de usuarios
        Este método carga los datos de usuarios y los almacena en un DataFrame.
        """

        return (
            # Obtención de la lista de empleados desde Odoo
            self._main._odoo.search_read(
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
            # Ordenamiento de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.WAREHOUSE,
                COLUMN.PAY_FREQUENCY,
                COLUMN.JOB,
            ]]
            # Registro de los nombres
            .pipe(self._main._names.register_names)
            # Procesamiento de datos de almacén
            .pipe(self._main._pipes.get_warehouse_name)
            # Procesamiento de datos de puesto de trabajo
            .pipe(self._main._pipes.get_job_name)
            # Ordenamiento de valores por ID de usuario
            .sort_values(COLUMN.USER_ID)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _load_records(
        self,
    ) -> pd.DataFrame:

        # Construcción del query para leer la tabla
        query = (
            QUERY.GET_RECORDS_IN_DATE_RANGE
            .format(
                **{
                    'table_name': DATABASE.TABLE.ASSISTANCE_RECORDS,
                    'time_column': COLUMN.REGISTRY_TIME,
                    'start_date': self._main._schemas.min_date(),
                    'end_date': self._main._date.most_recent_available_date
                }
            )
        )

        # Asignación de columnas de fecha y tiempo
        date_and_time: ColumnAssignation = {
            COLUMN.DATE: (
                lambda df: (
                    df[COLUMN.REGISTRY_TIME]
                    .dt.date
                )
            ),
            COLUMN.TIME: (
                lambda df: (
                    df[COLUMN.REGISTRY_TIME]
                    .dt.time
                )
            ),
        }

        # Obtención de los datos desde la tabla de la base de datos
        data = self._main._database.load_data_from_query(query)

        return (
            data
            # Obtención de los nombres de empleados
            .pipe(self._main._pipes.get_user_names)
            # Se filtran los registros con nombres vacíos
            .pipe(lambda df: df[df[COLUMN.NAME].notna()])
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Asignación de columnas de fecha y hora
            .assign(**date_and_time)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.REGISTRY_TIME,
                COLUMN.DATE,
                COLUMN.TIME,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
            ]]
        )

    def _load_corrections(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de correcciones
        Este método carga los datos de correcciones y los almacena en un DataFrame.
        """

        return (
            # Se cargan archivos de correcciones
            self._load_corrections_files()
            # Asignación de tipos de dato
            .pipe(self._main._processing.assign_dtypes)
            # Asignación de ordenamiento de valores de tipo de registro
            .pipe(self._main._pipes.assign_ordered_registry_type)
            # Se asigna la columna de fecha y hora de registro
            .pipe(self._main._processing.add_registry_time)
            # Se añade la columna de indicador de corrección con valor en True
            .assign(**{COLUMN.IS_CORRECTION: True})
            # Selección de columnas
            [[
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.TIME,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.REGISTRY_TIME,
                COLUMN.IS_CORRECTION,
            ]]
            # Ordenamiento de registros por fecha
            .sort_values(COLUMN.DATE)
        )

    def _load_justifications(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de justificaciones
        Este método carga los datos de justificaciones y los almacena en un DataFrame.
        """

        return (
            (
                # Se cargan los datos desde los documentos de Hojas de Cálculo
                pd.concat(
                    [
                        (
                            spreadsheet.load(
                                SPREADSHEET.JUSTIFICATIONS.NAME,
                                sheet_name,
                            )
                        )
                        for sheet_name in SPREADSHEET.JUSTIFICATIONS.SHEETS
                    ]
                )
            )
            # Reasignación de nombres de columnas
            .rename(columns= ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES)
            # Selección de columnas
            [ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES.values()]
            # Obtención de la ID de usuario
            .pipe(
                lambda df: (
                    pd.merge(
                        left= self.users[[COLUMN.USER_ID, COLUMN.NAME]],
                        right= df,
                        left_on= COLUMN.NAME,
                        right_on= COLUMN.NAME,
                        how= 'right'
                    )
                )
            )
            # Se descartan los usuarios cuya ID no fue encontrada ya que están inactivos
            .pipe(lambda df: df[ df[COLUMN.USER_ID].notna() ])
            # Formateo de fechas provenientes de los documentos de Google Sheets
            .pipe(self._main._processing.format_permission_date_strings)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
            # Se ordenan los datos por término de fecha de pérmiso
            .sort_values(COLUMN.PERMISSION_END)
        )

    def _load_holidays(
        self,
    ) -> pd.DataFrame:

        return (
            # Se cargan los datos desde la base de datos
            load_from_database(DATABASE.TABLE.HOLIDAYS)
            # Asignación de tipos de datos
            .pipe(self._main._processing.assign_dtypes)
        )

    def _load_schedules(
        self,
    ) -> pd.DataFrame:

        return (
            # Se cargan los datos desde la base de datos
            load_from_database(
                'schedules',
                # Conversión de tipos de dato ya que SQLite no soporta INTERVAL
                {
                    COLUMN.WEEKDAY: 'uint8',
                    COLUMN.START_SCHEDULE: 'timedelta64[ns]',
                    COLUMN.END_SCHEDULE: 'timedelta64[ns]',
                }
            )
        )

    def _load_schedule_offsets(
        self,
    ) -> pd.DataFrame:

        return (
            # Se cargan los datos desde la base de datos
            load_from_database(
                'schedule_offsets',
                # Conversión de tipos de dato ya que SQLite no soporta INTERVAL
                {
                    COLUMN.USER_ID: 'uint16',
                    COLUMN.WEEKDAY: 'uint8',
                    COLUMN.START_OFFSET: 'timedelta64[ns]',
                    COLUMN.END_OFFSET: 'timedelta64[ns]',
                }
            )
        )

    def _load_corrections_files(
        self,
    ) -> pd.DataFrame:
        """
        ### Cargar archivos de correcciones
        Este método se encarga de evaluar si se requiere cargar uno o dos libros de
        datos de diferentes meses en base a la fecha inicial y final de los datos
        requeridos y carga los libros en DataFrames que retorna concatenados.
        """

        # Mes de inicio
        start_month = (
            self._main._schemas
            .min_date()
            .month
        )
        # Mes de fin
        end_month = (
            self._main._date
            .most_recent_available_date
            .month
        )

        # Si el mes de inicio es distinto al mes de fin...
        if start_month != end_month:
            # Año de inicio
            start_year = (
                self._main._schemas
                .min_date()
                .year
            )
            # Año de fin
            end_year = (
                self._main._date
                .most_recent_available_date
                .year
            )
            # Parámetros para iterar
            params = [
                (start_year, start_month),
                (end_year, end_month),
            ]

            # Inicialización de lista de DataFrames
            corrections_in_required_dates: list[pd.DataFrame] = []

            # Carga de dos meses
            for ( year, month ) in params:
                # Se intenta cargar un libro de correcciones
                try:
                    # Obtención de los datos desde un archivo de Excel
                    corrections_per_month = self._load_corrections_book(year, month)
                    # Se añade el DataFrame a la lista
                    corrections_in_required_dates.append(corrections_per_month)
                # Si no fue encontrado...
                except FileNotFoundError:
                    # Se indica el error y se continúa con el siguiente libro
                    print(f'No se encontraron corrección del año y mes {year}/{month}')

            # Concatenación de DataFrames
            corrections = pd.concat(corrections_in_required_dates)

        # Si el mes de inicio es igual al mes de fin...
        else:
            # Carga del archivo
            corrections = self._load_corrections_book(self._main._date.current_year, self._main._date.current_month)

        return corrections

    def _load_corrections_book(
        self,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        """
        ### Cargar correcciones desde Excel
        Este método carga los datos de un Excel en base a los parámetros
        proporcionados.
        """

        # Generación de nombre de archivo a buscar
        file_name = (
            EXCEL_FILE.CORRECTIONS.NAME
            .format(
                **{
                    'year': year,
                    'month': month,
                }
            )
        )
        # Obtención de los datos desde un archivo de Excel
        data = pd.read_excel( path_from_dropbox(f'{file_name}.xlsx') )

        return data
