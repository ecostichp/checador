import pandas as pd

class _Interface_Pipes:
    """
    `[Submódulo]` Funciones pipe para DataFrame.
    """

    def get_user_names(
        self,
        records: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de nombres de usuarios
        Este método obtiene los nombres de los usuarios en base a su ID de usuario.

        :param records DataFrame: Registros entrantes.
        """
        ...

    def common_operations(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Operaciones comunes
        Operaciones comunes que todos los DataFrames generados necesitan
        ejecutar.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def get_exceeding_lunch_time(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de tiempo excedente en tiempo de comida
        Esta función obtiene el tiempo excedente en los intervalos
        tiempo de comida por cada usuario/día.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def get_user_id(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de ID de usuario
        Esta función se ejecuta para crear un pipe que asigna la ID de
        usuario a los registros del DataFrame provisto.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def get_warehouse_name(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de nombre de almacén
        Este método procesa los datos retornados por la API de Odoo y extrae y renombra
        el valor de almacén designado de los usuarios activos encontrados, desde un
        valor de tipo *many2one*.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def get_job_name(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de nombre de puesto de trabajo
        Este método procesa los datos retornados por la API de Odoo y extrae el nombre del
        puesto de trabajo, desde un valor de tipo *many2one*.
        """
        ...

    def assign_ordered_registry_type(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
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

        :param data DataFrame: Datos entrantes.
        """
        ...

    def total_vacation_days(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Obtención de total de días de vacaciones
        Este método realiza un recuento de días de descanso, días festivos y, en base a
        éstos y a un rango de fechas en cada registro de justificaciones, realiza un
        conteo de total de días de vacaciones en donde las justificaciones sean de tipo
        *Vacaciones*.

        :param data DataFrame: Datos entrantes.
        """
        ...

    def rename_permission_types(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        ### Reasignación de nombres de justificaciones
        Este método realiza una reasignación de nombres de tipos de justificaciones
        para generación de resúmenes de forma correcta y consistente.
        """
        ...
