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
        class EMPLOYEES_DATA:
            RENAME_COLUMNS = 'rename_employees_data_columns'
            """
            ### Reasignación de nombres de columnas en datos de usuarios
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
        RENAME_PERMISSION_NAMES = 'rename_permission_types'
        """
        ### Reasignación de nombres de columnas en tipos de permiso
        Este pipe reasigna nombres de columnas al DataFrame entrante.

        :param records DataFrame: Datos entrantes.
        """
        GET_HOLIDAY_JUSTIFICATIONS = 'get_holiday_justifications'
        """
        ### Obtención de permisos relacionados a días festivos
        Este pipe filtra los registros de incidencias para conservar únicamente los
        registros que tengan permisos relacionados con días festivos.

        :param records DataFrame: Datos entrantes.
        """
        COUNT_HOLIDAY_JUSTIFICATIONS_PER_EMPLOYEE = 'count_holiday_justifications_per_employee'
        """
        ### Contar incidencias de días festivos por empleado
    
        Este pipe toma las incidencias de días festivos, filtra desde los registros
        posteriores a la fecha considerada inicio de conteo y cuenta las incidencias
        existentes para cada empleado.

        :param records DataFrame: Datos entrantes.
        """
        GET_REMAINING_HOLIDAYS = 'get_remaining_holidays'
        """
        ### Obtención de días festivos restantes
        Este pipe computa los días festivos restantes para tomar por el empleado.

        :param records DataFrame: Datos entrantes.
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
            DISCARD_DUPLICATED = 'discard_duplicated'
            """
            ### Descartar duplicados
            Este pipe evalúa registros duplicados en base a la ID de usuario, fecha de
            registro y tipo de registro y etiqueta registros duplicados:
            - Se etiquetan todos los registros de inicio de jornada después del primero
            como duplicado
            - Se etiquetan todos los registros de fin de jordana antes del último como
            duplicado.

            :param records DataFrame: Datos entrantes.
            """
            DISCARD_CORRECTED_RECORDS = 'discard_corrected_records_from_original_data'
            """
            ### Descartar registros corregidos
            Este pipe elimina del conjunto original de registros aquellos elementos que ya
            fueron corregidos. Para ello, realiza una concatenación entre el DataFrame de
            registros originales y el DataFrame de correcciones utilizando como claves el
            identificador de usuario, la fecha y la hora. Los registros presentes en el
            DataFrame de correcciones se marcan mediante una columna auxiliar y
            posteriormente se descartan. El resultado es un DataFrame que conserva
            únicamente los registros originales que no han sido reemplazados o corregidos.

            :param records DataFrame: Datos entrantes.
            """
            CONCAT_CORRECTIONS = 'concat_corrections'
            """
            ### Concatenación de correcciones
            Este pipe toma el conjunto depurado, una vez filtrados los registros descartados
            por correcciones, y lo fusiona con el DataFrame que contiene las correcciones
            válidas. El propósito es reconstruir una secuencia completa donde registros
            originales y correcciones coexisten en un mismo flujo preparado para análisis
            posteriores.

            Durante la concatenación:
            - Se normaliza el indicador de "es corrección" para garantizar que todos los
            valores sean estrictamente booleanos, eliminando cualquier ambigüedad
            proveniente de valores nulos.
            - Se aplica ordenamiento de valores de tipo de registro para reestablecer el
            orden lógico entre tipos de registro, asegurando una estructura coherente.
            - Se ordenan todos los valores por tiempo de registro, lo que produce una línea
            temporal continua.
            - Se restaura el tipo categórico de la columna de dispositivo para preservar su
            semántica y eficiencia.

            El resultado es un DataFrame final que refleja con fidelidad la secuencia
            corregida de eventos, ya depurada y ordenada, listo para cualquier etapa
            posterior de procesamiento o análisis.

            :param records DataFrame: Datos entrantes.
            """
            TAG_VACATION_DAYS = 'tag_vacation_records'
            """
            ### Etiquetado de eventos en vacaciones
            Este pipe busca los eventos con valores de tipo de registro válido que hayan
            sido creados en los días en el usuario tiene incidencia de vacaciones.

            :param records DataFrame: Datos entrantes.
            """
            VACATION_EVENTS_TO_NULL_TYPE = 'vacation_events_to_null_type'
            """
            ### Anulación de eventos en vacaciones del usuario
            Este pipe busca los eventos que hayan sido creados en los días en el usuario
            tiene incidencia de vacaciones y cambia sus valores de tipo de registro a nulo.

            :param records DataFrame: Datos entrantes.
            """
            ASSIGN_DAY_AND_USER_INDEX = 'assign_day_and_user_id_index'
            """
            ### Asignación de índice por usuario y día de registro
            Este pipe concatena los valores de ID de usuario y fecha de registro para
            usarlos como índice.

            Ejemplo:
            >>> records # DataFrame
            >>> #    user_id        date
            >>> # 0        5  2025-11-24
            >>> # 1        5  2025-11-24
            >>> # 2        5  2025-11-24
            >>> # 3        5  2025-11-24
            >>> # 4        5  2025-11-25
            >>> 
            >>> records.pipe(assign_day_and_user_id)
            >>> #    user_id        date  user_date_index
            >>> # 0        5  2025-11-24     5|2025-11-24
            >>> # 1        5  2025-11-24     5|2025-11-24
            >>> # 2        5  2025-11-24     5|2025-11-24
            >>> # 3        5  2025-11-24     5|2025-11-24
            >>> # 4        5  2025-11-25     5|2025-11-25

            :param records DataFrame: Datos entrantes.
            """
            VALIDATE_TODAY_CHECKIN = 'validate_today_check_in'
            GET_DAILY_SCHEDULES = 'get_daily_schedules'
            """
            ### Obtención de horarios laborales por día
            Este pipe asigna los horarios laborales establecidos a los registros del
            DataFrame entrante, en base al día de la semana de éstos.

            :param records DataFrame: Datos entrantes.
            """
            USER_WAREHOUSE = 'user_warehouse'
            """
            ### Obtención de almacén del usuario
            Este pipe obtiene el valor de nombre corto de almacén al que pertenecen los
            usuarios.

            :param records DataFrame: Datos entrantes.
            """
            GET_SCHEDULE_OFFSETS = 'get_schedule_offsets'
            """
            ### Obtención de desfases de horarios para gerentes
            Este pipe asigna los desfases de horarios para gerentes o un desfase en 0 para
            el resto de los empleados.

            :param records DataFrame: Datos entrantes.
            """
            ALLOWED_START_AND_END = 'define_allowed_start_and_end_time'
            """
            ### Definición de horarios de inicio y fin permitidos
            Este pipe combina la fecha del registro con los horarios base de cada día de la
            semana y sus ajustes de desfase por usuario para producir los valores completos
            de inicio y fin permitidos.

            :param records DataFrame: Datos entrantes.
            """
            GET_CUMMULATED_TIME = 'get_cummulated_time'
            """
            ### Obtención de tiempo acumulado de entrada tardía o salida anticipada
            Este pipe calcula para cada registro si hubo entrada tardía o salida
            anticipada, y en esos casos determina cuántos minutos (o el intervalo
            correspondiente) representan esa entrada tardía o salida anticipada.

            :param records DataFrame: Datos entrantes.
            """

            class PIVOTED:
                COUNT_PER_REGISTRY_TYPE = 'count_per_registry_type'
                """
                ### Conteo por tipo de registro
                Este pipe realiza un pivoteo de tabla usando:

                - Índice: Índice de ID de usuario/fecha.
                - Columnas: Tipo de registro.
                - Valores: Conteo de tipos de registro por índice de ID de usuario/fecha.

                :param records DataFrame: Datos entrantes.
                """
                VALIDATE_RECORDS = 'validate_day_pivoted_records'
                """
                ### Validación de registros por usuario/día
                Este pipe recibe un DataFrame pivote por usuario y día, y le inyecta
                validaciones produciendo columnas booleanas que indican si cada día/usuario
                pasa o no cada regla.

                - Validación de que existen los cuatro registros
                - Validación de que conteo de registros de comida son pares

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

    class REPORT:
        GET_EMPLOYEE_DATA_FOR_USER = 'get_employee_data_for_user'
        """
        ### Obtención de datos de empleado
        Este pipe obtiene los datos de empleado de los registros provistos, como la
        fecha de ingreso.onteo y cuenta las incidencias
        existentes para cada empleado.

        :param records DataFrame: Datos entrantes.
        """
        COMPUTE_AVAILABLE_HOLIDAYS = 'compute_available_holidays'
        """
        ### Obtención de días festivos disponibles
        Este pipe obtiene la fecha más reciente entre la fecha de ingreso o la fecha de
        inicio de conteo de días festivos para hacer un correcto cálculo en los días
        que un empleado puede tomar, según su fecha de ingreso y la fecha de inicio de
        conteo y posteriormente realiza el conteo de éstos.

        :param records DataFrame: Datos entrantes.
        """

    class ADAPTER:
        DISPLAY_JUSTIFICATION_ON_REGISTRY_TYPE = 'display_justification_on_registry_type'
        """
        ### Mostrar incidencia en tipo de registro
        Esta función reemplaza los valores `'null'` de la columna `'status'` en
        todos los registros que contengan `True` en la columna `'is_vacation'`.

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
        EMPLOYEES_DATA = 'select_column_employees_data'
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
        VALIDATE_TODAY_CHECKIN = 'select_columns_validate_today_checkin'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        EVALUATE_REGISTRY_TIMES = 'select_columns_evaluate_registry_times'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        HOLIDAYS_SUMMARY = 'select_columns_holidays_summary'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """
        JUSTIFICATIONS_HISTORY = 'select_columns_justifications_history'
        """
        ### Selección de columnas
        Este pipe selecciona las columnas indicadas para controlar la forma del
        DataFrame resultante y modificarlo explícitamente si se desea agregar otra
        columna.

        :param records DataFrame: Datos entrantes.
        """

        class PIVOTED:
            RECORDS = 'select_columns_pivot_records'
            """
            ### Selección de columnas
            Este pipe selecciona las columnas indicadas para controlar la forma del
            DataFrame resultante y modificarlo explícitamente si se desea agregar otra
            columna.

            :param records DataFrame: Datos entrantes.
            """
