import pandas as pd
from ..constants import (
    COLUMN,
    VALIDATION,
)
from ..contracts.resources import _Contract_ReportsToUpload
from ..mapping import COLUMN_LABELS
from ..settings import OUTPUT
from ..typing.interfaces import Interface_RegistryProcessing

class GoogleSheetsReports(_Contract_ReportsToUpload):

    # Registro de reportes
    _reports_to_get = {
        # Usuarios
        OUTPUT.FILE.VISUALIZATIONS.SHEET.USERS: (
            lambda main: (
                main._report.holidays_summary()
            )
        ),
        # Resumen general completo
        OUTPUT.FILE.VISUALIZATIONS.SHEET.COMPLETE_GENERAL_SUMMARY: (
            lambda main: (
                main._report.complete_general_summary()
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.TIME,
                    COLUMN.DATE,
                    COLUMN.REGISTRY_TYPE,
                    COLUMN.DEVICE,
                    COLUMN.IS_DUPLICATED,
                    COLUMN.NULL_BY_JUSTIFICATION,
                    COLUMN.IS_CORRECTION,
                    VALIDATION.COMPLETE,
                    VALIDATION.BREAK_PAIRS,
                    VALIDATION.UNIQUE_START_AND_END,
                    COLUMN.WEEKDAY,
                    COLUMN.ALLOWED_START,
                    COLUMN.ALLOWED_END,
                    VALIDATION.IS_LATE_START,
                    VALIDATION.IS_EARLY_END,
                    COLUMN.LATE_TIME,
                    COLUMN.EARLY_TIME,
                    COLUMN.WAREHOUSE,
                ]]
            )
        ),
        # Historial completo de incidencias
        OUTPUT.FILE.VISUALIZATIONS.SHEET.JUSTIFICATIONS_HISTORY: (
            lambda main: (
                main._report.justifications()
            )
        ),
        # Resumen de acumulados en horas de comida
        OUTPUT.FILE.VISUALIZATIONS.SHEET.LUNCH_SUMMARY: (
            lambda main: (
                main._report.lunch_summary()
            )
        ),
        # Resumen de acumulados y conteos en incidencias
        OUTPUT.FILE.VISUALIZATIONS.SHEET.JUSTIFICATIONS_SUMMARY: (
            lambda main: (
                main._report.justfications_summmary()
            )
        ),
    }

    def __init__(
        self,
        main: Interface_RegistryProcessing,
    ) -> None:

        # Asignación de la instancia principal
        self._main = main

    def generate_reports(
        self,
    ) -> dict[str, pd.DataFrame]:

        # Generación de los reportes
        reports_to_upload = {
            sheet: (
                generator(self._main)
                .pipe(self._get_labels)
            )
            for ( sheet, generator )
            in self._reports_to_get.items()
        }

        return reports_to_upload

    def _get_labels(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:

        return (
            data
            .rename(
                columns= {
                    old: new
                    for ( old, new ) in COLUMN_LABELS.items()
                    if old in data.columns
                }
            )
        )
