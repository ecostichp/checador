from ..constants import COMMON_ARGS

class MESSAGE:
    """
    `Literal` Mensajes para imprimir.
    """
    LATE_OPEN_FOUND = 'Se encontraron días con apertura tardía.'
    NO_VALIDATIONS_TO_SHOW = 'No hay validaciones para mostrar.'
    CORRECTIONS_FILE_NOT_FOUND = f'No se encontraron corrección del año y mes {{{COMMON_ARGS.YEAR}}}/{{{COMMON_ARGS.MONTH}}}.'
    RECORDS_TO_FIX_WERE_FOUND = 'Se encontraron registros para corregir.'
    HINT_VALIDATIONS = f'Accede a la información a través del atributo [{{{COMMON_ARGS.VALIDATIONS_ATTRIBUTE}}}] o al Excel generado.'
    ALL_OK = 'Todo está correcto.'
