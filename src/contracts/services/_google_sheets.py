import pandas as pd
from ...typing.interfaces import Interface_RegistryProcessing

class _Contract_GoogleSheets:

    def update(
        self,
        main: Interface_RegistryProcessing,
    ) -> None:
        ...

    def load_justifications(
        self,
    ) -> pd.DataFrame:
        ...
