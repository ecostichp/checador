from .._base.generics import _T
from .._base.literals import PermissionTypeOption
from .._structures.callables import SeriesFromDataFrame

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
