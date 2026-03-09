import pandas as pd
from ...typing.dicts import ReportsFromInstance
from ...typing.interfaces import Interface_RegistryProcessing

class _Contract_ReportsToUpload:
    _reports_to_get: ReportsFromInstance[Interface_RegistryProcessing]

    def generate_reports(
        self,
    ) -> dict[str, pd.DataFrame]:
        ...
