from .._constants import ARGS

class MESSAGE:
    """
    `Literal` Mensajes para imprimir.
    """
    LATE_OPEN_FOUND = 'Se encontraron días con apertura tardía.'
    NO_VALIDATIONS_TO_SHOW = 'No hay validaciones para mostrar.'
    CORRECTIONS_FILE_NOT_FOUND = f'No se encontraron corrección del año y mes {{{ARGS.YEAR}}}/{{{ARGS.MONTH}}}.'
    RECORDS_TO_FIX_WERE_FOUND = 'Se encontraron registros para corregir.'
    HINT_VALIDATIONS = f'Accede a la información a través del atributo [{{{ARGS.VALIDATIONS_ATTRIBUTE}}}] o al Excel generado.'
    ALL_OK = 'Todo está correcto.'

class EXCEL_FILE:
    """
    `CONST` Nombres de archivos de Excel y sus hojas.
    """
    class CORRECTIONS:
        """
        `CONST` Archivo de correcciones de registros de asistencia.
        """
        NAME = f'correcciones_{{{ARGS.YEAR}}}/correcciones_checador_{{{ARGS.MONTH}:02d}}_{{{ARGS.YEAR}}}'
        """
        `Literal` Nombre del archivo.
        """

class DB_TABLE:
    """
    `CONST` Nombres de tablas de base de datos.
    """
    RECORDS = f'registros_{{{ARGS.YEAR}}}_{{{ARGS.MONTH}:02d}}'
    """
    `Literal` Nombre de la tabla de registros.
    """

class SPREADSHEET:
    """
    `CONST` Nombres de archivos de Hojas de Cálculo de Google y sus hojas.
    """
    class JUSTIFICATIONS:
        """
        `CONST` Nombres de archivo de incidencias.
        """
        NAME = 'Original Registros Checador'
        """`Literal` Nombre del documento."""
        SHEETS = [
            'Incidencias',
            'Incidencias choferes',
        ]
        """`list[Literal]` Nombres de las hojas."""

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

class SCHEMA:
    """
    `CONST` Plantillas de nombres de esquemas de tiempo.
    """
    WEEKLY = f'Semana {{{ARGS.N}}}'
    """`Literal` Esquema semanal."""
    BIWEEKLY = f'Quincena {{{ARGS.N}}}'
    """`Literal` Esquema quincenal."""
