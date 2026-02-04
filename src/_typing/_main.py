from typing import (
    Any,
    Callable,
    Literal,
    TypeVar,
    overload,
)
import pandas as pd
from sqlalchemy import Connection

# Tipos de dato

_T = TypeVar('_T')
"""
Genérico para uso en tipados complejos.
"""

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

SeriesPipe = Callable[[pd.Series], pd.Series]
"""
### Pipe para Pandas Series
Este tipo de dato es una función.

Esta función debe ser llamada por medio del método pipe de una Pandas Series
siendo ésta función provista en el argumento requerido del método
"""

UserID = int
"""
### ID de usuario
Tipo de dato que describe la ID de un usuario.
****
"""

NumericWeekday = Literal[0, 1, 2, 3, 4, 5, 6]
"""
### Día numérico de la semana
Tipo de dato que decribe los valores posibles de días de la semana en formato
numérico entero.
"""

DatetimeStr = str
"""
Fecha en formato texto

Ejemplo:
>>> start_date: DatetimeStr = '1998-07-28'
"""

PermissionTypeOption = Literal['time', 'days']
"""
Tipos de permiso.
"""

PermissionOptionGenericMap = dict[PermissionTypeOption, _T]
"""
Mapa genérico para tipos de permiso.
"""

ValidityOptions = Literal['valid', 'invalid']
"""
Opciones de validación.
"""

PayFrequency = Literal['weekly', 'biweekly']
"""
Frecuencia de pago.

Valores disponibles:
- `'weekly'`: Semanal.
- `'biweekly'`: Quincenal.
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

Devices = Literal['csl', 'sjc']
"""
Dispositivos en ubicaciones.
"""

class Many2One:
    """
    ### Muchos a uno
    Tipo de dato encontrado en valores de campos de tipo *many2one
    provenientes de datos de la API de Odoo.

    Este tipo de dato almacena una ID de tipo `int` en la posición `0` y un
    nombre de registro de tipo `str` en la posición `1`. El tipo de dato que
    porta estos valores es de tipo lista. Debido a que las listas no pueden
    tiparse por posición, este tipado provee una interfaz clara para poder
    tipar el valor retornado en base a la posición proporcionada para obtener
    un valor.

    Este tipado no debe utilizarse para instanciar, solo como máscara de lista
    para representar valores de tipo *many2one*.

    Uso:
    >>> m2o: Many2One
    >>> value = m2o[0] # int
    >>> value = m2o[1] # str
    """

    @overload
    def __getitem__(
        self,
        position: Literal[0],
    ) -> int:
        ...

    @overload
    def __getitem__(
        self,
        position: Literal[1],
    ) -> str:
        ...
