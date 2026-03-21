import pandas as pd
from ..contracts import (
    _CoreRegistryProcessing,
    _Interface_Data,
)
from ..core import pipeline_hub
from ..rules import PIPELINE

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
        self.employees_data = self._load_employees_data()

    def _load_users(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de usuarios
        Este método carga los datos de usuarios y los almacena en un DataFrame.
        """

        # Obtención de la lista de empleados desde Odoo
        users = self._main._services.odoo_api.get_users()
        # Procesamiento por medio de pipe
        processed_data = pipeline_hub.run_pipe_flow(users, PIPELINE.GET_USERS)

        return processed_data

    def _load_records(
        self,
    ) -> pd.DataFrame:

        # Obtención de los datos desde la tabla de la base de datos
        records = self._main._services.database.load_assistance_records(
            self._main._schemas.min_date(),
            self._main._services.date.today,
        )
        # Procesamiento por medio de pipe
        processed_data = pipeline_hub.run_pipe_flow(records, PIPELINE.GET_RECORDS)

        return processed_data

    def _load_corrections(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de correcciones
        Este método carga los datos de correcciones y los almacena en un DataFrame.
        """

        # Obtención de datos de correcciones
        corrections = self._load_corrections_files()
        # Procesamiento por medio de pipe
        processed_data = pipeline_hub.run_pipe_flow(corrections, PIPELINE.GET_CORRECTIONS)

        return processed_data

    def _load_justifications(
        self,
    ) -> pd.DataFrame:
        """
        ### Carga de datos de justificaciones
        Este método carga los datos de justificaciones y los almacena en un DataFrame.
        """

        # Obtención de datos de incidencias
        justifications = self._main._services.google_sheets.load_justifications()
        # Procesamiento por medio de pipe
        result = pipeline_hub.run_pipe_flow(justifications, PIPELINE.GET_JUSTIFICATIONS)

        return result

    def _load_holidays(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        holidays = self._main._services.database.load_holidays()

        return holidays

    def _load_schedules(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        schedules = self._main._services.database.load_schedules()

        return schedules

    def _load_schedule_offsets(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde la base de datos
        schedule_offsets = self._main._services.database.load_schedule_offsets()

        return schedule_offsets

    def _load_corrections_files(
        self,
    ) -> pd.DataFrame:
        """
        ### Cargar archivos de correcciones
        Este método se encarga de evaluar si se requiere cargar uno o dos libros de
        datos de diferentes meses en base a los datos de los esquemas y carga los
        libros en DataFrames que retorna concatenados.
        """

        # Si el mes de inicio es distinto al mes de fin...
        if self._main._schemas.cross_months:
            # Obtención de correcciones desde más de un libro de Excel
            corrections = self._main._services.excel.load_corrections_books(self._main._schemas)

        # Si el mes de inicio es igual al mes de fin...
        else:
            # Carga del archivo
            corrections = self._main._services.excel.load_corrections_book(
                self._main._services.date.current_year,
                self._main._services.date.current_month,
            )

        return corrections

    def _load_employees_data(
        self,
    ) -> pd.DataFrame:

        # Se cargan los datos desde Excel
        users_data = self._main._services.excel.load_users_data()
        # Procesamiento por medio de pipe
        result = pipeline_hub.run_pipe_flow(users_data, PIPELINE.GET_EMPLOYEES_DATA)

        return result
