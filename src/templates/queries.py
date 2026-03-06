from ..constants import ARGS

class QUERY:
    """
    `CONST` Plantillas de queries para SQL
    """

    GET_RECORDS_IN_DATE_RANGE = (
        f"""
        SELECT
            *
        FROM {{{ARGS.TABLE_NAME}}}
        WHERE (
            DATE({{{ARGS.REGISTRY_TIME}}}) >= '{{{ARGS.START_DATE}}}'
            AND DATE({{{ARGS.REGISTRY_TIME}}}) <= '{{{ARGS.END_DATE}}}'
        );
        """
    )
    """Obtención de registros en un rango de tiempo determinado."""

    UPDATE_LAST_UPDATE_IN_RECORDS = (
        f"""
        UPDATE {{{ARGS.TABLE_NAME}}}
            SET 'date' = '{{{ARGS.DATE}}}'
            WHERE name = '{{{ARGS.DEVICE_NAME}}}'
        ;
        """
    )
    """Actualización de última hora de actualización en registros."""
