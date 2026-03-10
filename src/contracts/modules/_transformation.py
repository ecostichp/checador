from ...typing import DataFramePipe

class _Interface_Transformation:

    def reassign_registry_type_categories(
        self,
        categories: list[str],
    ) -> DataFramePipe:
        """
        ### Reasignar categorías de tipo de registro
        Este método fabrica una función que reasigna las categorías de tipos de
        registro en base a los valores disponibles en el DataFrame provisto. De esta
        manera las agrupaciones y pivoteos de tabla se mantienen consistentes.

        :param categories list[str]: Lista completa de categorías de tipo de registro.
        """
        ...
