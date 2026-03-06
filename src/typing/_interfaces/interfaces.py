from typing import (
    Literal,
    overload,
)

class Many2One:
    """
    ### Muchos a uno
    Tipo de dato encontrado en valores de campos de tipo *many2one*
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
    def __getitem__(self, position: Literal[0]) -> int: ...
    @overload
    def __getitem__( self, position: Literal[1]) -> str: ...
