from ..constants import ARGS

class SCHEMA:
    """
    `CONST` Plantillas de nombres de esquemas de tiempo.
    """
    WEEKLY = f'Semana {{{ARGS.N}}}'
    """`Literal` Esquema semanal."""
    BIWEEKLY = f'Quincena {{{ARGS.N}}}'
    """`Literal` Esquema quincenal."""
