import pandas as pd
from ccc_utils import spreadsheet
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Upload,
)
from .._settings import MAIN_FILE

class _Upload(_Interface_Upload):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

    def _update(
        self,
    ) -> None:

        # Hojas y reportes para actualizaciones
        sheets_and_reports: dict[str, pd.DataFrame] = {
            # Usuarios
            MAIN_FILE.SHEET.USERS: self._main._data.users,
            # Resumen general completo
            MAIN_FILE.SHEET.COMPLETE_GENERAL_SUMMARY: self._main._report.complete_general_summary(),
            # Historial completo de incidencias
            MAIN_FILE.SHEET.JUSTIFICATIONS_HISTORY: self._main._data.justifications,
            # Resumen de acumulados en horas de comida
            MAIN_FILE.SHEET.LUNCH_SUMMARY: self._main._report.lunch_summary(),
            # Resumen de acumulados y conteos en incidencias
            MAIN_FILE.SHEET.JUSTIFICATIONS_SUMMARY: self._main._report.justfications_summmary(),
        }

        # Actualización
        for ( sheet_name, report ) in sheets_and_reports.items():
            # Se actualiza la hoja con el reporte
            spreadsheet.write(report, MAIN_FILE.NAME, sheet_name)
