from dataclasses import dataclass
from ..typing.callables import DataFramePipe

class PipeMetadata:

    def __init__(
        self,
        fn: DataFramePipe,
        registered_name: str,
        requires: set[str],
        creates: set[str],
        selects: set[str],
        renames: dict[str, str],
    ) -> None:

        # Asignación de atributos
        self.fn = fn
        self.specs = self.Specs(
            registered_name,
            fn.__name__,
            requires,
            creates,
            selects,
            renames,
        )

    @dataclass(slots= True)
    class Specs:
        registered_name: str
        real_name: str
        requires: set[str]
        creates: set[str]
        selects: set[str]
        renames: dict[str, str]

        @property
        def input_columns(
            self,
        ) -> list[str]:

            # Obtención de la declaración de columnas
            required = self.requires.copy()
            to_rename = set(self.renames.keys())
            renamed = set(self.renames.values())
            created = self.creates
            selected = self.selects

            # Se descartan las columnas renombradas y creadas en columnas a seleccionar
            to_select = selected - created - renamed

            # Inicialización de columnas a retornar
            columns = set()

            # Iteración de grupos de columnas a considerar
            for cols_group in [required, to_rename, to_select]:
                # Si hay columnas en el grupo...
                if cols_group:
                    # Se añaden a las columnas a retornar
                    columns |= cols_group

            # Conversión de conjunto a lista
            columns = list(columns)

            return columns

        @property
        def output_columns(
            self,
        ) -> list[str]:

            # Uso de las colummnas a seleccionar
            columns = list(self.selects)

            return columns
