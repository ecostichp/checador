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
