from .._base.generics import (
    _Main,
    _T,
)
from .._base.literals import PermissionTypeOption
from .._structures.callables import (
    DataFrameGetter,
    SeriesFromDataFrame,
)

ColumnAssignation = dict[str, SeriesFromDataFrame]
"""
### Asignación de columna
Este tipo de dato es un diccionario que contiene:

- Clave(s): Un nombre de columna que se asignará o reasignará en un DataFrame.
- Valor(es): Una función lambda que recibe un DataFrame y lo procesa para
retornar una Pandas Series que será la columna del DataFrame.

Ejemplo:
>>> ca: ColumnAssignation = {
>>>     'day': lambda df: df['date'].dt.day
>>> }
"""

PermissionOptionGenericMap = dict[PermissionTypeOption, _T]
"""
Mapa genérico para tipos de permiso.
"""

ReportsFromInstance = dict[str, DataFrameGetter[_Main]]
"""
Reportes de instancia.

Este tipo de dato requiere que se declare la instancia principal como genérico
por limitaciones de importación circular.
"""
