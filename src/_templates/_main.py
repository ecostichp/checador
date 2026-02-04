class EXCEL_FILE:
    """
    `CONST` Nombres de archivos de Excel y sus hojas.
    """
    class CORRECTIONS:
        """
        `CONST` Archivo de correcciones de registros de asistencia.
        """
        NAME = 'correcciones_{year}/correcciones_checador_{month:02d}_{year}'
        """
        `Literal` Nombre del archivo.
        """

class DB_TABLE:
    """
    `CONST` Nombres de tablas de base de datos.
    """
    RECORDS = 'registros_{year}_{month:02d}'
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

    GET_EXISTING_LAST_DATE = (
        """
        SELECT
            {id_column}
        FROM {table_name}
        ORDER BY {time_column} DESC
        LIMIT 1;
        """
    )
    """
    Obtención de última fecha existente en registros de asistencia.
    """

    GET_RECORDS_IN_DATE_RANGE = (
        """
        SELECT
            *
        FROM {table_name}
        WHERE (
            DATE({time_column}) >= '{start_date}'
            AND DATE({time_column}) <= '{end_date}'
        );
        """
    )
    """Obtención de registros en un rango de tiempo determinado."""

class SCHEMA:
    """
    `CONST` Plantillas de nombres de esquemas de tiempo.
    """
    WEEKLY = 'semanal {n}'
    """`Literal` Esquema semanal."""
    BIWEEKLY = 'quincenal {n}'
    """`Literal` Esquema quincenal."""
