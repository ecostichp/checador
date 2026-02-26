import pandas as pd
from ccc_utils import spreadsheet
from .._interface import (
    _CoreRegistryProcessing,
    _Interface_Upload,
)
from .._settings import OUTPUT

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
            OUTPUT.FILE.VISUALIZATIONS.SHEET.USERS: self._main._data.users,
            # Resumen general completo
            OUTPUT.FILE.VISUALIZATIONS.SHEET.COMPLETE_GENERAL_SUMMARY: self._main._report.complete_general_summary(),
            # Historial completo de incidencias
            OUTPUT.FILE.VISUALIZATIONS.SHEET.JUSTIFICATIONS_HISTORY: self._main._data.justifications,
            # Resumen de acumulados en horas de comida
            OUTPUT.FILE.VISUALIZATIONS.SHEET.LUNCH_SUMMARY: self._main._report.lunch_summary(),
            # Resumen de acumulados y conteos en incidencias
            OUTPUT.FILE.VISUALIZATIONS.SHEET.JUSTIFICATIONS_SUMMARY: self._main._report.justfications_summmary(),
        }

        # Actualización
        for ( sheet_name, report ) in sheets_and_reports.items():
            # Se actualiza la hoja con el reporte
            spreadsheet.write(report, OUTPUT.FILE.VISUALIZATIONS.NAME, sheet_name)
