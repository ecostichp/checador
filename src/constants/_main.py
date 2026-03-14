from datetime import timedelta

ENV_VAR_PREFIX = 'CHECADOR_'
"""
`Literal` Prefijo de variables de entorno.
"""

class ENV_VARIABLE:
    """`CONST` Nombres de variables de entornos."""
    TODAY = 'TODAY'
    """`Literal` Día en curso."""
    SELECTED_DATABASE = 'DATABASE'
    """`Literal` Base de datos a utilizar."""

class COLUMN:
    """
    `CONST` Colección de nombres de columnas de DataFrame.
    """
    ID = 'id'
    """`str` ID en la base de datos."""
    DATE = 'date'
    """`datetime64[s]` Fecha del registro."""
    TIME = 'time'
    """`timedelta64[ns]` Hora del registro."""
    REGISTRY_TIME = 'registry_time'
    """`datetime64[ns]` Fecha y hora del registro."""
    NAME = 'name'
    """`category[str]` Nombre del usuario."""
    USER_ID = 'user_id'
    """`uint16` ID del usuario."""
    REGISTRY_TYPE = 'status'
    """
    `category[str]` Tipo de registro.

    Valores disponibles:
    - `'checkIn'`: Inicio de jornada laboral.
    - `'breakOut'`: Inicio de tiempo de comida.
    - `'breakIn'`: Fin de tiempo de comida.
    - `'checkOut'`: Fin de jornada laboral.
    - `'undefined'`: No especificado.
    - `'null'`: Registro anulado.
    """
    DEVICE = 'device'
    """
    `category[str]` Dispositivo de registro.

    Valores disponibles:
    - `'csl'`: Dispositivo de Cabo San Lucas.
    - `'sjc'`: Dispositivo de San José Del Cabo.
    """
    PAY_FREQUENCY = 'pay_frequency'
    """`category[str]` Frecuencia de pago del empleado."""
    WAREHOUSE = 'warehouse'
    """`category[str]` Almacén al que el usuario pertenece."""
    JOB = 'job'
    """`category[str]` Puesto de trabajo."""
    IS_CORRECTION = 'is_correction'
    """`bool` El registro es una corrección."""
    IS_DUPLICATED = 'duplicated'
    """
    `bool` El tipo de registro representa un mismo evento pero se creó dos o más
    veces por el usuario en el mismo día
    ."""
    NULL_BY_JUSTIFICATION = 'null_by_justification'
    """
    `bool` El registro fue anulado por una incidencia.
    """
    USER_AND_DATE_INDEX = 'user_date_index'
    """
    `str` Columna computada para mapear validaciones a nivel día y usuario en
    pivoteo de tablas.
    """
    COUNT = 'count'
    """`uint64` Conteo."""
    WEEKDAY = 'weekday'
    """
    `category[int]` Día de la semana.

    Las codificaciones son:
    - `0`: Lunes
    - `1`: Martes
    - `2`: Miércoles
    - `3`: Jueves
    - `4`: Viernes
    - `5`: Sábado
    - `6`: Domingo
    """
    START_SCHEDULE = 'start_schedule'
    """`timedelta64[ns]` Inicio general de jornada laboral."""
    END_SCHEDULE = 'end_schedule'
    """`timedelta64[ns]` Fin general de jornada laboral."""
    START_OFFSET = 'start_offset'
    """`timedelta64[ns]` Desfase de inicio de jornada laboral."""
    END_OFFSET = 'end_offset'
    """`timedelta64[ns]` Desfase de fin de jornada laboral."""
    ALLOWED_START = 'allowed_start'
    """`datetime64[ns]` Tiempo permitido de inicio de jornada laboral."""
    ALLOWED_END = 'allowed_end'
    """`datetime64[ns]` Tiempo permitido de fin de jornada laboral."""
    LATE_TIME = 'late_time'
    """`timedelta64[ns]` Tiempo de entrada tardía."""
    EARLY_TIME = 'early_time'
    """`timedelta64[ns]` Tiempo de salida anticipada."""
    TIME_IN_DELTA = 'time_in_delta'
    """`timedelta64[ns]` Tiempo en formato delta."""
    EXCEEDING_LUNCH_TIME = 'exceeding_lunch_time'
    """`timedelta64[ns]` Tiempo excedido en horario de comida."""
    PERMISSION_TYPE = 'permission_type'
    """
    `category[str]` Tipo de permiso.

    Los valores disponibles son:
    - Por día:
        - `'vacation'`: Ausencia por vacaciones.
        - `'sick_general'`: Ausencia por enfermedad general.
        - `'work_risk'`: Ausencia por riesgo de trabajo.
        - `'maternity'`: Ausencia por maternidad.
        - `'unjustified_absence'`: Ausencia injustificada (Falta).
        - `'unpaid_extra_absence'`: Ausencia extra sin goce de sueldo y sin afectar bonos.
        - `'holiday_absence'`: Ausencia por día festivo.
        - `'holiday_compensation'`: Ausencia por compensación de día festivo no descansado.
    - Por horas:
        - `'hours_permission'`: Permiso de horas.
        - `'overtime'`: Horas extra.
        - `'meal_break_missing'`: Hora de comida pendiente por reponer.
        - `'meal_break_compensation'`: Compensación por hora de comida no tomada.
    """
    PERMISSION_START = 'permission_start'
    """`datetime64[ns]` Fecha y hora de inicio de permiso."""
    PERMISSION_END = 'permission_end'
    """`datetime64[ns]` Fecha y hora de fin de permiso."""
    REST_DAYS_COUNT = 'rest_days_count'
    """`uint8` Conteo de días de descanso."""
    HOLIDAYS_COUNT = 'holidays_count'
    """`uint8` Conteo de días festivos."""
    VACATION_DAYS_COUNT = 'vacation_days_count'
    """`uint8` Conteo de días de vacaciones."""
    HOLIDAY_NAME = 'holiday_name'
    """`str` Nombre del día festivo."""
    HOLIDAY_DATE = 'holiday_date'
    """`datetime64[ns]` Fecha del día festivo."""

    IS_CLOSED_CORRECT = 'is_closed_correct'
    """`bool` Es día cerrado y correcto."""
    IS_CURRENT_DAY_CHECKIN = 'is_today_check_in'
    """`bool` Es registro de inicio de jornada laboral en el día en curso."""

    SCHEMA = 'schema'
    """`str` Nombre del esquema."""

    WORKED_DAYS = 'worked_days'
    """`uint8` Días laborados dentro del esquema de tiempo."""

class REGISTRY_TYPE:
    """`CONST` Nombres de tipos de registro."""
    CHECK_IN = 'checkIn'
    """`Literal` Inicio de jornada laboral."""
    BREAK_OUT = 'breakOut'
    """`Literal` Inicio de tiempo de comida."""
    BREAK_IN = 'breakIn'
    """`Literal` Fin de tiempo de comida."""
    CHECK_OUT = 'checkOut'
    """`Literal` Fin de jornada laboral."""
    UNDEFINED = 'undefined'
    """`Literal` No especificado."""
    NULL = 'null'
    """`Literal` Registro anulado."""

class VALIDATION:
    """`CONST` Nombres de columnas de DataFrame generados por validaciones."""
    COMPLETE = 'complete'
    """
    `bool` Validación de registro diario por empleado donde debe existir al menos un registro por cada tipo de registro:
    - `checkIn`: Inicio jornada laboral.
    - `breakOut`: Inicio de tiempo de comida.
    - `breakIn`: Fin de tiempo de comida.
    - `checkOut`: Fin de jornada laboral.
    """
    BREAK_PAIRS = 'break_pairs'
    """
    `bool` Validación de registro diario por empleado donde:
    - Los conteos de tipos de registro `'breakOut'` y `'breakIn'` deben ser al menos 1 en cada tipo.
    - Los conteos de tipos de registro `'breakOut'` y `'breakIn'` deben ser iguales.
    """
    UNIQUE_START_AND_END = 'unique_start_and_end'
    """
    `bool` Validación de registro diario por empleado donde los registros de inicio de jornada laboral y fin de jornada
    laboral son únicos para cada uno de éstos.
    """
    IS_LATE_START = 'is_late_start'
    """`bool` Es inicio de jornada laboral tardío."""
    IS_EARLY_END = 'is_early_end'
    """`bool` Es fin de jornada laboral anticipado."""

class PERMISSION_NAME:
    """
    `CONST` Nombres de tipos de permiso.
    """
    # Permisos generales por día
    VACATION = 'vacation'
    """`Literal` Ausencia por vacaciones."""
    SICK_GENERAL = 'sick_general'
    """`Literal` Ausencia por enfermedad general."""
    WORK_RISK = 'work_risk'
    """`Literal` Ausencia por riesgo de trabajo."""
    MATERNITY = 'maternity'
    """`Literal` Ausencia por maternidad."""

    # Ausencias
    UNJUSTIFIED_ABSENCE = 'unjustified_absence'
    """`Literal` Ausencia injustificada (Falta)."""
    UNPAID_EXTRA_ABSENCE = 'unpaid_extra_absence'
    """`Literal` Ausencia extra sin goce de sueldo y sin afectar bonos."""

    # Días festivos
    HOLIDAY_ABSENCE = 'holiday_absence'
    """`Literal` Ausencia por día festivo."""
    HOLIDAY_COMPENSATION = 'holiday_compensation'
    """`Literal` Ausencia por compensación de día festivo no descansado."""

    # Permisos generales por horas
    HOURS_PERMISSION = 'hours_permission'
    """`Literal` Permiso de horas."""
    OVERTIME = 'overtime'
    """`Literal` Horas extra."""

    # Tiempo de comida no tomado o compensado
    MEAL_BREAK_MISSING = 'meal_break_missing'
    """`Literal` Hora de comida pendiente por reponer."""
    MEAL_BREAK_COMPENSATION = 'meal_break_compensation'
    """`Literal` Compensación por hora de comida no tomada."""

class WEEKDAY:
    """
    `CONST` Valores numéricos de día de la semana.
    """
    MONDAY = 0
    """`int` Lunes."""
    TUESDAY = 1
    """`int` Martes."""
    WEDNESDAY = 2
    """`int` Miércoles."""
    THURSDAY = 3
    """`int` Jueves."""
    FRIDAY = 4
    """`int` Viernes."""
    SATURDAY = 5
    """`int` Sábado."""
    SUNDAY = 6
    """`int` Domingo."""

class WAREHOUSE_NAME:
    """
    `CONST` Nombres de sucursales.
    """
    CSL = 'CSL'
    """`Literal` Nombre de sucursal de Cabo San Lucas."""
    SJC = 'SJC'
    """`Literal` Nombre de sucursal de San José Del Cabo."""

WAREHOUSES = [
    'csl',
    'sjc',
]
"""
`list[Literal]` Lista de nombres abreviados de almacenes.
"""

TIME_DELTA_ON_ZERO = timedelta()
"""
`timedelta(00:00:00)` Valor de delta de tiempo en ceros.
"""

class PIPE:
    class DATA:
        class USERS:
            GET_WAREHOUSE_NAME = 'get_warehouse_name'
            """
            ### Obtención de nombre de almacén
            Este pipe procesa los datos retornados por la API de Odoo y extrae y renombra
            el valor de almacén designado de los usuarios activos encontrados, desde un
            valor de tipo *many2one*.

            :param records DataFrame: Datos entrantes.
            """
            GET_JOB_NAME = 'get_job_name'
            """
            ### Obtención de nombre de puesto de trabajo
            Este pipe procesa los datos retornados por la API de Odoo y extrae el nombre
            del puesto de trabajo, desde un valor de tipo *many2one*.

            :param records DataFrame: Datos entrantes.
            """
        class CORRECTIONS:
            ADD_CORRECTION_TAG = 'add_correction_tag'
            """
            ### Adición de etiqueta de corrección
            Este pipe añade una columna booleana en `True` que indica que todos los
            registros del DataFrame entrante son correcciones.

            :param records DataFrame: Datos entrantes.
            """
            SORT_BY_DATE = 'sort_by_date'
            """
            ### Ordenamiento por fecha
            Este pipe ordena los datos del DataFrame por su columna de fecha.

            :param records DataFrame: Datos entrantes.
            """
        class JUSTIFICATIONS:
            RENAME_COLUMNS = 'rename_justifications_columns'
            """
            ### Reasignación de nombres de columnas en incidencias
            Este pipe reasigna nombres de columnas al DataFrame entrante.

            :param records DataFrame: Datos entrantes.
            """
    class PROCESSING:
        ASSIGN_DTYPES = 'assign_dtypes'
        """
        ### Asignación de tipos de datos
        Esta función asigna los tipos de datos establecidos para las columnas de un
        DataFrame y ordena los tipos de registro en caso de existir la columna de éstos.

        :param records DataFrame: Datos entrantes.
        """
        GET_USER_NAMES = 'get_user_names'
        """
        ### Obtención de nombres de usuarios
        Este pipe obtiene los nombres de los usuarios en base a su ID de usuario.

        :param records DataFrame: Registros entrantes.
        """
        TIME_FIRST_TO_STRING = 'time_first_to_string'
        """
        ### Conversión de hora a cadena de texto
        Este pipe convierte la columna de hora en cadena de texto.

        :param records DataFrame: Registros entrantes.
        """
        NULL_BY_JUSTIFICATION = 'null_by_justification'
        """
        ### Anulado por incidencia
        Este pipe añade una columna que indica si el registro fue anulado por
        incidencia en base a si el tipo de registro fue descrito como incidencia. Además,
        convierte este tipo de registro en `'null'` para desacoplar correctamente la
        información y conservar la integridad de los datos.

        :param records DataFrame: Registros entrantes.
        """
        ASSIGN_ORDERED_REGISTRY_TYPE = 'assign_ordered_registry_type'
        """
        ### Asignación de tipo de registro (ordenado)
        Esta función convierte la columna de tipo de registro en un tipo categórico
        ordenado.

        1. Identifica los tipos de registro presentes en el DataFrame.
        2. Filtra según un orden ORDERED_REGISTRY_TYPE.
        3. Reasigna la columna como categoría y aplica ese orden.

        Esto permite trabajar con los tipos de registro de manera consistente,
        facilitando comparaciones, ordenamientos y cualquier proceso que 
        dependa del orden lógico de los eventos.

        :param records DataFrame: Datos entrantes.
        """
        ADD_REGISTRY_TIME = 'add_registry_time'
        """
        ### Asignación de fecha y hora de registro
        Esta función concatena fecha y hora  en base a las columnas `'date'` y
        `'time'` de los registros.
        """
        class RECORDS:
            ADD_DATE_AND_TIME = 'add_date_and_time'
            """
            ### Agregar fecha y hora
            Este pipe agrega columnas de fecha y de hora al DataFrame entrante.

            :param records DataFrame: Registros entrantes.
            """
            PROCESS_BEFORE_SAVE_IN_DATABASE = 'process_before_save_in_database'
            """
            ### Procesamiento antes de guardar en base de datos
            Este pipe reasigna nombres de columnas del DataFrame entrante para
            acondicionarlo para ser guardado en la base de datos y le crea una columna de
            ID de registro en la base de datos en base a la fecha, hora y el disposivo
            fuente de los registros.

            :param records DataFrame: Datos entrantes.
            """
        class JUSTIFICATIONS:
            GET_AND_KEEP_BY_USER_ID = 'get_and_keep_by_user_id'
            """
            ### Obtención de IDs de usuario
            Este pipe obtiene las IDs de los usuarios y filtra por todos los registros cuya
            ID de usuario fue hallada.

            :param records DataFrame: Datos entrantes.
            """
            FORMAT_PERMISSION_DATE_STRINGS = 'format_permission_date_strings'
            """
            ### Formateo de fechas de incidencias
            Este método convierte los valores de cadena de texto de las columnas de fecha
            de permiso en valores de tipo fecha.

            :param records DataFrame: Datos entrantes.
            """
    class COLUMNS_SELECTION:
        ASSISTANCE_RECORDS = 'select_columns_assistance_records'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        CORRECTIONS = 'select_columns_corrections'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        JUSTIFICATIONS = 'select_columns_justifications'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        ASSISTANCE_RECORDS_UPDATE = 'select_columns_assistance_records_update'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
