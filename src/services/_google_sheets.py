import pandas as pd
from ccc_utils import spreadsheet
from ..contracts import _CoreRegistryProcessing
from ..contracts.services import _Contract_GoogleSheets
from ..resources import GoogleSheetsReports
from ..settings import OUTPUT
from ..templates.files import SPREADSHEET

class _GoogleSheets(_Contract_GoogleSheets):

    def update(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Generación de objeto de reportes
        google_sheets_reports = GoogleSheetsReports(main)

        # Generación de reportes para actualizaciones
        sheets_and_reports = google_sheets_reports.generate_reports()

        # Actualización
        for ( sheet_name, report ) in sheets_and_reports.items():
            # Se actualiza la hoja con el reporte
            spreadsheet.write(report, OUTPUT.FILE.VISUALIZATIONS.NAME, sheet_name)

    def load_justifications(
        self,
    ) -> pd.DataFrame:

        return (
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
