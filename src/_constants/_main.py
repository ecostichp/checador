class COLUMN:
    """
    `CONST` Colección de nombres de columnas de DataFrame.
    """
    ID = 'id'
    """
    `str` ID en la base de datos.
    """
    DATE = 'date'
    """`str[date]` Fecha del registro."""
    TIME = 'time'
    """`str[time]` Hora del registro."""
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
    """
    `category[str]` Puesto de trabajo.
    """
    IS_CORRECTION = 'is_correction'
    """`bool` El registro es una corrección."""
    IS_DUPLICATED = 'duplicated'
    """`bool` El tipo de registro se introdujo dos o más veces por el usuario."""
    USER_AND_DATE_INDEX = 'user_date_index'
    """`str` Columna computada para mapear validaciones a nivel día y usuario en pivoteo de tablas."""
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
    """`timedelta` Inicio general de jornada laboral."""
    END_SCHEDULE = 'end_schedule'
    """`timedelta` Fin general de jornada laboral."""
    START_OFFSET = 'start_offset'
    """`timedelta` Desfase de inicio de jornada laboral."""
    END_OFFSET = 'end_offset'
    """`timedelta` Desfase de fin de jornada laboral."""
    ALLOWED_START = 'allowed_start'
    """`datetime` Tiempo permitido de inicio de jornada laboral."""
    ALLOWED_END = 'allowed_end'
    """`datetime` Tiempo permitido de fin de jornada laboral."""
    LATE_TIME = 'late_time'
    """`timedelta` Tiempo de retardo."""
    EARLY_TIME = 'early_time'
    """`timedelta` Tiempo de salida anticipada."""
    TIME_IN_DELTA = 'time_in_delta'
    """`timedelta` Tiempo en formato delta."""
    EXCEEDING_LUNCH_TIME = 'exceeding_lunch_time'
    """`timedelta` Tiempo excedido en horario de comida."""
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
    """`datetime` Fecha y hora de inicio de permiso."""
    PERMISSION_END = 'permission_end'
    """`datetime` Fecha y hora de fin de permiso."""
    REST_DAYS = 'rest_days'
    """`uint8` Conteo de días de descanso."""
    HOLIDAYS = 'holidays'
    """`uint8` Conteo de días festivos."""
    VACATION_DAYS = 'vacation_days'
    """`uint8` Conteo de días de vacaciones."""
    HOLIDAY_NAME = 'holiday_name'
    """
    `str` Nombre del día festivo.
    """
    HOLIDAY_DATE = 'holiday_date'
    """
    `date` Fecha de validez del día festivo.
    """

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

class VALIDATION:
    """`CONST` Nombres de columnas de DataFrame generados por validaciones."""
    COMPLETE = 'complete'
    """
    `bool` Validación de registro diario por empleado donde debe existir al menos un registro por cada tipo de registro:
    - `checkIn`: Inicio jornada laboral
    - `breakOut`: Inicio de hora de comida
    - `breakIn`: Fin de hora de comida
    - `checkOut`: Fin de jornada laboral
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
    """`bool` Es retardo."""
    IS_EARLY_END = 'is_early_end'
    """`bool` Es salida anticipada."""

class REPORT:
    """
    `CONST` Nombres de reportes que se generan en Excel.
    """
    VERIFICATION = 'verification'
    """`CONST` Nombre de reporte de verificaciones en Excel."""
    SUMMARY__ = 'resumen_de_registros'
    """`CONST` Nombre de reporte de resumen en Excel."""
    class SUMMARY:
        NAME = 'resumen_de_registros'
        class SHEET:
            COMPLETE = 'Datos completos'
            CUMMULATED_SUMMARY = 'Resumen'
            JUSTIFICATIONS = 'Justificaciones'
            MONTHLY_JUSTIFICATIONS = 'Incidencias del mes'
            USERS = 'Usuarios'

class PERMISSION_NAME:
    """
    `CONST` Nombres de tipos de permiso.
    """
    VACATION = 'vacation'
    """`Literal` Ausencia por vacaciones."""
    SICK_GENERAL = 'sick_general'
    """`Literal` Ausencia por enfermedad general."""
    WORK_RISK = 'work_risk'
    """`Literal` Ausencia por riesgo de trabajo."""
    MATERNITY = 'maternity'
    """`Literal` Ausencia por maternidad."""

    UNJUSTIFIED_ABSENCE = 'unjustified_absence'
    """`Literal` Ausencia injustificada (Falta)."""
    UNPAID_EXTRA_ABSENCE = 'unpaid_extra_absence'
    """`Literal` Ausencia extra sin goce de sueldo y sin afectar bonos."""

    HOLIDAY_ABSENCE = 'holiday_absence'
    """`Literal` Ausencia por día festivo."""
    HOLIDAY_COMPENSATION = 'holiday_compensation'
    """`Literal` Ausencia por compensación de día festivo no descansado."""

    HOURS_PERMISSION = 'hours_permission'
    """`Literal` Permiso de horas."""
    OVERTIME = 'overtime'
    """`Literal` Horas extra."""

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

class DATABASE:
    """
    `CONST` Nombres en base de datos.
    """
    class TABLE:
        """
        `CONST` Nombres de tablas en la base de datos.
        """
        ASSISTANCE_RECORDS = 'assistance_records'
        """`Literal` Tabla de registros de asistencia."""
        HOLIDAYS = 'holidays'
        """`Literal` Tabla de días festivos."""
