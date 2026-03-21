import pandas as pd
from ..constants import COMMON_ARGS
from ..contracts.services import _Contract_Excel
from ..contracts.modules import _Interface_DateSchemas
from ..templates.files import EXCEL_FILE
from ..templates.messages import MESSAGE
from ..utils import path_from_dropbox

class Excel(_Contract_Excel):

    def load_users_data(
        self,
    ) -> pd.DataFrame:

        # Generación de nombre de archivo a buscar
        file_name = EXCEL_FILE.USERS_DATA.NAME
        # Obtención de la ruta del archivo
        file_path = path_from_dropbox(f'{file_name}.xlsx')

        # Obtención de los datos desde un archivo de Excel
        data = pd.read_excel(file_path)

        return data

    def load_corrections_books(
        self,
        schemas: _Interface_DateSchemas,
    ) -> pd.DataFrame:

        # Obtención del mes de inicio
        start_month = schemas.start_month
        # Obtención del mes de fin
        end_month = schemas.end_month
        # Año de inicio
        start_year = schemas.start_year
        # Año de fin
        end_year = schemas.end_year
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
                corrections_per_month = self.load_corrections_book(year, month)
                # Se añade el DataFrame a la lista
                corrections_in_required_dates.append(corrections_per_month)
            # Si no fue encontrado...
            except FileNotFoundError:
                # Se indica el error y se continúa con el siguiente libro
                print(
                    MESSAGE.CORRECTIONS_FILE_NOT_FOUND
                    .format(
                        **{
                            COMMON_ARGS.YEAR: year,
                            COMMON_ARGS.MONTH: month,
                        }
                    )
                )

        # Concatenación de DataFrames
        corrections = pd.concat(corrections_in_required_dates)

        return corrections

    def load_corrections_book(
        self,
        year: int,
        month: int,
    ) -> pd.DataFrame:

        # Generación de nombre de archivo a buscar
        file_name = (
            EXCEL_FILE.CORRECTIONS.NAME
            .format(
                **{
                    COMMON_ARGS.YEAR: year,
                    COMMON_ARGS.MONTH: month,
                }
            )
        )
        # Obtención de la ruta del archivo
        file_path = path_from_dropbox(f'{file_name}.xlsx')

        # Obtención de los datos desde un archivo de Excel
        data = pd.read_excel(
            file_path,
            keep_default_na= False,
        )

        return data
