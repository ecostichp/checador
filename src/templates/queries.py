from ..constants import COMMON_ARGS

class QUERY:
    """
    `CONST` Plantillas de queries para SQL
    """

    GET_RECORDS_IN_DATE_RANGE = (
        f"""
        SELECT
            *
        FROM {{{COMMON_ARGS.TABLE_NAME}}}
        WHERE (
            DATE({{{COMMON_ARGS.REGISTRY_TIME}}}) >= '{{{COMMON_ARGS.START_DATE}}}'
            AND DATE({{{COMMON_ARGS.REGISTRY_TIME}}}) <= '{{{COMMON_ARGS.END_DATE}}}'
        );
        """
    )
    """Obtención de registros en un rango de tiempo determinado."""

    UPDATE_LAST_UPDATE_IN_RECORDS = (
        f"""
        UPDATE {{{COMMON_ARGS.TABLE_NAME}}}
            SET 'date' = '{{{COMMON_ARGS.DATE}}}'
            WHERE name = '{{{COMMON_ARGS.DEVICE_NAME}}}'
        ;
        """
    )
    """Actualización de última hora de actualización en registros."""
