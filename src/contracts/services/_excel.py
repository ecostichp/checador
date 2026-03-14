import pandas as pd

class _Interface_DateSchemas:
    ...

class _Contract_Excel:

    def load_corrections_books(
        self,
        schemas: _Interface_DateSchemas,
    ) -> pd.DataFrame:
        ...

    def load_corrections_book(
        self,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        """
        ### Cargar correcciones desde Excel
        Este método carga los datos de un Excel en base a los parámetros
        proporcionados.
        """
        ...
