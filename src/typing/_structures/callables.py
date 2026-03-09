from typing import (
    Any,
    Callable,
)
import pandas as pd
from sqlalchemy import Connection
from .._base.generics import (
    _Main,
    _T,
)

SeriesApply = Callable[[_T], Any]
"""
Función a aplicar en Pandas Series

El tipado recibe un genérico, por ejemplo, `int`.

>>> fn: SeriesApply[int] = lambda value: ...
>>> 
>>> # Esto es exactamente lo mismo
>>> def fn(value: int):
>>>     ...

El retorno se tipa en base al retorno de la misma función.
"""

SeriesFromDataFrame = Callable[[pd.DataFrame], pd.Series]
"""
Función que toma un Pandas DataFrame como argumento de entrada y lo procesa
para retornar una Pandas Series.

Ejemplo:
>>> fn: SeriesFromDataFrame = lambda df: df['user_id']
"""

DataFramePipe = Callable[[pd.DataFrame], pd.DataFrame]
"""
### Pipe para DataFrame
Este tipo de dato es una función

Esta función debe ser llamada por medio del método pipe de un DataFrame siendo
ésta provista en el argumento requerido del método.

El retorno es un DataFrame procesado según dicte la función.

Ejemplo:
>>> fn: DataFramePipe = lambda df: ...
>>> 
>>> # Esto es lo mismo
>>> def fn(df: pd.DataFrame) -> pd.DataFrame:
>>>     ...
"""

ConnFunction = Callable[[Connection], _T]
"""
Función ejecutable dentro de una conexión a base de datos.

El tipado recibe un genérico, por ejemplo, `DataFrame`.

>>> fn: ConnFunction[pd.DataFrame] = lambda conn: pd.read_sql_query(...)
>>> 
>>> # Esto es exactamente lo mismo
>>> def fn(conn: Connection) -> pd.DataFrame:
>>>     ...
"""

DataFrameGetter = Callable[[_Main], pd.DataFrame]
"""
Función que genera un reporte en formato Pandas DataFrame.

Este tipo de dato requiere que se declare la instancia principal como genérico
por limitaciones de importación circular.

>>> fn: DataFrameGetter[_CoreRegistryProcessing] = (
>>>     lambda main: main._report.generate_x_report(),
>>> )
>>> 
>>> # Esto es exactamente lo mismo
>>> def fn(main: _CoreRegistryProcessing) -> pd.DataFrame:
>>>     ...
"""
