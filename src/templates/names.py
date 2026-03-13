from ..constants import COMMON_ARGS

class SCHEMA:
    """
    `CONST` Plantillas de nombres de esquemas de tiempo.
    """
    WEEKLY = f'Semana {{{COMMON_ARGS.N}}}'
    """`Literal` Esquema semanal."""
    BIWEEKLY = f'Quincena {{{COMMON_ARGS.N}}}'
    """`Literal` Esquema quincenal."""
