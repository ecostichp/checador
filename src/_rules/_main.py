from .._constants import (
    COLUMN,
    REGISTRY_TYPE,
    VALIDATION,
)
from .._typing import (
    ColumnAssignation,
    DataFramePipe,
)

GLOBAL_FILTERS: DataFramePipe = lambda df: (
    df[
        ~( df[COLUMN.USER_ID].isin([1, 24]) )
    ]
)
"""
`DataFramePipe` Filtros globales.
"""

CHECK_SPECIFIC_DAY: DataFramePipe = lambda df: (
    df[
        # Columna default para dar verdadero siempre si la condición de abajo se comenta
        (df[COLUMN.USER_ID] == df[COLUMN.USER_ID])
        # Comentar esta línea para no filtrar por día
        # ✨ Se puede usar date.today() o la fecha que se requiera revisar
        # & ( df[COLUMN.DATE] == '2026-01-05' )
    ]
)
"""
`DataFramePipe` Revisión por día específico.
"""

VALIDATIONS_PER_DAY_AND_USER_ID: ColumnAssignation = {

    # Validación de que existen los cuatro registros
    VALIDATION.COMPLETE: (
        lambda pivoted_df: (
            ( pivoted_df[REGISTRY_TYPE.BREAK_IN] > 0 )
            & ( pivoted_df[REGISTRY_TYPE.BREAK_OUT] > 0 )
            & ( pivoted_df[REGISTRY_TYPE.CHECK_IN] > 0 )
            & ( pivoted_df[REGISTRY_TYPE.CHECK_OUT] > 0 )
        )
    ),

    # Validación de que conteo de registros de comida son pares
    VALIDATION.BREAK_PAIRS: (
        lambda pivoted_df: (
            ( pivoted_df[REGISTRY_TYPE.BREAK_IN] == pivoted_df[REGISTRY_TYPE.BREAK_OUT] )
            & ( pivoted_df[REGISTRY_TYPE.BREAK_IN] > 0 )
            & ( pivoted_df[REGISTRY_TYPE.BREAK_OUT] > 0 )
        )
    ),

    # Validación de que los registros de inicio y fin de jornada laboral son estrictamente uno de cada uno.
    VALIDATION.UNIQUE_START_AND_END: (
        lambda pivoted_df: (
            ( pivoted_df[REGISTRY_TYPE.CHECK_IN] == 1 )
            & ( pivoted_df[REGISTRY_TYPE.CHECK_OUT] == 1 )
        )
    )
}
"""
### Validaciones por día y usuario

Este diccionario contiene las validaciones que se realizan por los
registros pivoteados por día y usuario para validar la integridad
de los datos entrantes:

- `COMPLETE`: Validación de registro diario por empleado donde debe existir
al menos un registro por cada tipo de registro:
    - `checkIn`: Inicio jornada laboral
    - `breakOut`: Inicio de hora de comida
    - `breakIn`: Fin de hora de comida
    - `checkOut`: Fin de jornada laboral
- `BREAK_PAIRS` Validación de registro diario por empleado donde:
    - Los conteos de tipos de registro `'breakOut'` y `'breakIn'` deben ser
    al menos 1 en cada tipo.
    - Los conteos de tipos de registro `'breakOut'` y `'breakIn'` deben ser
    iguales.
- `UNIQUE_START_AND_END` Validación de registro diario por empleado donde los
registros de inicio de jornada laboral y fin de jornada laboral son únicos
para cada uno de éstos.
"""
