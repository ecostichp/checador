from ..constants import ARGS

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
