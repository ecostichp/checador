from ccc_utils import spreadsheet
from ..contracts import _CoreRegistryProcessing
from ..contracts.services import _Contract_GoogleSheets
from ..resources import GoogleSheetsReports
from ..settings import OUTPUT

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
