import pandas as pd
from datetime import timedelta
from attendance_registry._constants import COLUMN as ATTENDANCE_COLUMN
from ..constants import (
    COLUMN,
    PIPE,
    REGISTRY_TYPE,
)
from ..contracts import _CoreRegistryProcessing
from ..contracts.pipes import _Contract_PipeMethods
from ..mapping import (
    ASSIGNED_DTYPES,
    ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES,
    ORDERED_REGISTRY_TYPE,
    WAREHOUSE_RENAME,
)
from ..settings import INPUT
from ..tools import PipelineHub
from ..typing import ColumnAssignation
from ..typing.callables import (
    DataFramePipe,
    SeriesApply,
    SeriesFromDataFrame,
)
from ..typing.interfaces import Many2One

class _Submodule(PipelineHub.Owner):

    def __init__(
        self,
        pipes_m: 'PipeMethods'
    ) -> None:
        self._pipes_m = pipes_m
        self._initialize_pipes()

class PipeMethods(_Contract_PipeMethods):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

        # Inicialización de submódulos de pipes
        self._data = self.Data(self)
        self._processing = self.Processing(self)
        self._format = self.Format(self)

    class Data(_Submodule):

        @PipelineHub.register_method(
            PIPE.DATA.USERS.GET_WAREHOUSE_NAME,
            requires= {
                COLUMN.WAREHOUSE,
            },
        )
        def get_warehouse_name(
            self: PipeMethods.Data,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de nombre de almacén
            Este pipe procesa los datos retornados por la API de Odoo y extrae y renombra
            el valor de almacén designado de los usuarios activos encontrados, desde un
            valor de tipo *many2one*.

            :param records DataFrame: Datos entrantes.
            """

            # Función para extraer la ID del registro many2one de Odoo
            extract_id_fn: SeriesApply[Many2One] = (
                lambda reference: reference[0]
            )

            # Función para asignar nombre a la ID de almacén
            rename_id_fn: SeriesApply[int] = (
                lambda warehouse_id: WAREHOUSE_RENAME[warehouse_id]
            )

            # Función para extraer y renombrar la información de ID de almacén
            process_warehouse_data: ColumnAssignation = {
                COLUMN.WAREHOUSE: (
                    lambda df: (
                        df[COLUMN.WAREHOUSE]
                        .apply(extract_id_fn)
                        .apply(rename_id_fn)
                    )
                )
            }

            return (
                records
                .assign(**process_warehouse_data)
            )

        @PipelineHub.register_method(
            PIPE.DATA.USERS.GET_JOB_NAME,
            requires= {
                COLUMN.JOB,
            },
        )
        def get_job_name(
            self: PipeMethods.Data,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de nombre de puesto de trabajo
            Este pipe procesa los datos retornados por la API de Odoo y extrae el nombre
            del puesto de trabajo, desde un valor de tipo *many2one*.

            :param records DataFrame: Datos entrantes.
            """

            # Función para extraer el nombre del registro many2one de Odoo
            extract_name_fn: SeriesApply[Many2One] = (
                lambda reference: reference[1]
            )

            # Función para reasignar el valor procesado a la misma columna
            reassign_value: ColumnAssignation = {
                COLUMN.JOB: (
                    lambda df: (
                        df[COLUMN.JOB]
                        .apply(extract_name_fn)
                    )
                )
            }

            return (
                records
                .assign(**reassign_value)
            )

        @PipelineHub.register_method(
            PIPE.DATA.CORRECTIONS.ADD_CORRECTION_TAG,
            creates= {
                COLUMN.IS_CORRECTION,
            },
        )
        def add_correction_tag(
            self: PipeMethods.Data,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Adición de etiqueta de corrección
            Este pipe añade una columna booleana en `True` que indica que todos los
            registros del DataFrame entrante son correcciones.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                # Se añade la columna de indicador de corrección con valor en True
                .assign(**{COLUMN.IS_CORRECTION: True})
            )

        @PipelineHub.register_method(
            PIPE.DATA.CORRECTIONS.SORT_BY_DATE,
        )
        def sort_by_date(
            self: PipeMethods.Data,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Ordenamiento por fecha
            Este pipe ordena los datos del DataFrame por su columna de fecha.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                # Ordenamiento de registros por fecha
                .sort_values(COLUMN.DATE)
            )

        @PipelineHub.register_method(
            PIPE.DATA.JUSTIFICATIONS.RENAME_COLUMNS,
            renames= ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES,
            selects= set( ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES.values() ),
        )
        def rename_justifications_columns(
            self: PipeMethods.Data,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Reasignación de nombres de columnas en incidencias
            Este pipe reasigna nombres de columnas al DataFrame entrante.

            :param records DataFrame: Datos entrantes.
            """

            # Declaración de diccionario de reasignación de nombres de columnas
            columns_to_rename = ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES
            # Construcción de iterable de columnas seleccionadas
            selected_columns = ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES.values()

            return (
                records
                # Reasignación de nombres de columnas
                .rename(columns= columns_to_rename)
                # Selección de columnas
                [selected_columns]
            )

    class Processing(_Submodule):

        @PipelineHub.register_method(
            PIPE.PROCESSING.ASSIGN_DTYPES,
        )
        def assign_dtypes(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Asignación de tipos de datos
            Esta función asigna los tipos de datos establecidos para las columnas de un
            DataFrame y ordena los tipos de registro en caso de existir la columna de éstos.

            :param records DataFrame: Datos entrantes.
            """

            # Generación del mapa de tipos de datos
            existing_dtypes = {
                column: dtype
                for ( column, dtype )
                in ASSIGNED_DTYPES.items()
                if column in records.columns
            }

            # Función para ordenar tipos de registro si es que la columna existe
            def reorder_registry_types(df: pd.DataFrame) -> pd.DataFrame:
                # Si existe columna de tipos de registro en el DataFrame...
                if COLUMN.REGISTRY_TYPE in df.columns:
                    return (
                        df
                        # Asignación de ordenamiento de valores de tipo de registro
                        .pipe(self._assign_ordered_registry_type)
                    )
                # Si no existe columna de tipos de registro en el DataFrame...
                else:
                    return df

            # Función para ordenar días numéricos de la semana
            def reorder_weekdays(df: pd.DataFrame) -> pd.DataFrame:
                # Si existe columna de tipos de registro en el DataFrame...
                if COLUMN.WEEKDAY in df.columns:

                    # Creación de función para obtener los días de la semana existentes en los valores
                    def existing_weekdays(s: pd.Series) -> list[int]:
                        # Obtención de los valores únicos de días de la semana
                        unique_days = s.unique()
                        # Generación de las categorías ordenadas en base a los datos existentes
                        filtered_days = [wd for wd in range(7) if wd in unique_days]

                        return filtered_days

                    # Función para ordernar los valores categóricos
                    reorder_categoric_values: dict[str, DataFramePipe] = {
                        COLUMN.WEEKDAY: (
                            lambda df: (
                                df[COLUMN.WEEKDAY]
                                .astype('category')
                                # Ordenamiento de categorías
                                .cat.reorder_categories(
                                    existing_weekdays(df[COLUMN.WEEKDAY]),
                                    ordered= True,
                                )
                            )
                        )
                    }

                    return (
                        df
                        .assign(**reorder_categoric_values)
                    )

                # Si no existe columna de tipos de registro en el DataFrame...
                else:
                    return df

            return (
                records
                # Reasignación de tipos de dato
                .astype(existing_dtypes)
                # Reordenamiento de tipos de registro si es que existen
                .pipe(reorder_registry_types)
                # Reordenamiento de días numéricos de la semana si es que existen
                .pipe(reorder_weekdays)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.GET_USER_NAMES,
            requires= {
                COLUMN.USER_ID,
            },
            creates= {
                COLUMN.NAME,
                COLUMN.WAREHOUSE,
                COLUMN.PAY_FREQUENCY,
                COLUMN.JOB,
            },
        )
        def get_user_names(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de nombres de usuarios
            Este pipe obtiene los nombres de los usuarios en base a su ID de usuario.

            :param records DataFrame: Registros entrantes.
            """

            # Función para descartar la columna de nombre del DataFrame
            discard_name_column: DataFramePipe = (
                lambda df: (
                    df
                    [
                        [col for col in df.columns if col != COLUMN.NAME]
                    ]
                )
            )

            return (
                records
                # Se descarta la columna de nombre desde los registros
                .pipe(discard_name_column)
                # Se usa la ID de usuarios para obtener nombre desde los datos de Odoo
                .pipe(
                    lambda df: (
                        pd.merge(
                            left= self._pipes_m._main._data.users,
                            right= df,
                            on= COLUMN.USER_ID,
                            how= 'right',
                        )
                    )
                )
                # Se filtran los registros con nombres vacíos
                .pipe(lambda df: df[df[COLUMN.NAME].notna()])
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.RECORDS.ADD_DATE_AND_TIME,
            requires= {
                COLUMN.REGISTRY_TIME,
            },
            creates= {
                COLUMN.DATE,
                COLUMN.TIME,
            },
        )
        def add_date_and_time(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Agregar fecha y hora
            Este pipe agrega columnas de fecha y de hora al DataFrame entrante.

            :param records DataFrame: Registros entrantes.
            """

            # Función para convertir cadena de texto en delta de tiempo
            def string_to_timedelta(value: str) -> timedelta:
                # Obtención de los valores
                [ hour, minute, second ] = value.split(':')
                # Inicialización del objeto de delta de tiempo
                value = timedelta(
                    hours= int(hour),
                    minutes= int(minute),
                    seconds= int(second),
                )
                return value

            # Asignación de columnas de fecha y tiempo
            date_and_time: ColumnAssignation = {
                COLUMN.DATE: (
                    lambda df: (
                        df[COLUMN.REGISTRY_TIME]
                        .dt.date
                    )
                ),
                COLUMN.TIME: (
                    lambda df: (
                        df[COLUMN.REGISTRY_TIME]
                        .dt.time
                        .astype('string')
                        .apply(string_to_timedelta)
                    )
                ),
            }

            return (
                records
                # Asignación de columnas de fecha y hora
                .assign(**date_and_time)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.TIME_FIRST_TO_STRING,
            requires= {
                COLUMN.TIME,
            },
            creates= {
                COLUMN.TIME,
            },
        )
        def time_first_to_string(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Conversión de hora a cadena de texto
            Este pipe convierte la columna de hora en cadena de texto.

            :param records DataFrame: Registros entrantes.
            """

            # Función para convertir la columna de tiempo en texto antes de asignar dtype
            time_first_to_string: ColumnAssignation = {
                COLUMN.TIME: (
                    lambda df: (
                        df[COLUMN.TIME].astype('string')
                    )
                )
            }

            return (
                records
                # Se convierte el tipo de dato de la columna de tiempo en cadena de texto para ser convertida a datetime64[ns]
                .assign(**time_first_to_string)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.NULL_BY_JUSTIFICATION,
            requires= {
                COLUMN.REGISTRY_TYPE,
            },
            creates= {
                COLUMN.NULL_BY_JUSTIFICATION,
            },
        )
        def null_by_justification(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Anulado por incidencia
            Este pipe añade una columna que indica si el registro fue anulado por
            incidencia en base a si el tipo de registro fue descrito como incidencia. Además,
            convierte este tipo de registro en `'null'` para desacoplar correctamente la
            información y conservar la integridad de los datos.

            :param records DataFrame: Registros entrantes.
            """

            # Función para calificar descartados por incidencia
            null_by_justification: ColumnAssignation = {
                COLUMN.NULL_BY_JUSTIFICATION: (
                    lambda df: (
                        df[COLUMN.REGISTRY_TYPE] == INPUT.VALUE.JUSTIFICATION
                    )
                ),
            }

            return (
                records
                # Se identifican registros anulados por incidencia
                .assign(**null_by_justification)
                # Se reemplazan los valores indicados como nulos por incidencia
                .replace({
                    COLUMN.REGISTRY_TYPE: {
                        INPUT.VALUE.JUSTIFICATION: REGISTRY_TYPE.NULL,
                    }
                })
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.ASSIGN_ORDERED_REGISTRY_TYPE,
            requires= {
                COLUMN.REGISTRY_TYPE,
            },
            creates= {
                COLUMN.REGISTRY_TYPE,
            },
        )
        def assign_ordered_registry_type(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
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

            :param records DataFrame: Datos entrantes.
            """

            # Obtención de los valores categóricos encontrados en el DataFrame
            available_values = records[COLUMN.REGISTRY_TYPE].cat.categories
            # Generación de los elementos ordenados y filtrados a usar para reordenamiento
            items = [value for value in ORDERED_REGISTRY_TYPE if value in available_values]

            # Función para reasignación y ordenamiento de categorías de tipo de registro
            def _handle_category_integrity(s: pd.Series) -> pd.Series:

                # Si no existe registro de fin de jornada laboral...
                if 'checkOut' not in items:
                    # Se agrega éste a las categorías a asignar (Se espera que el resto vaya)
                    new_items = [
                        REGISTRY_TYPE.UNDEFINED,
                        REGISTRY_TYPE.CHECK_IN,
                        REGISTRY_TYPE.BREAK_OUT,
                        REGISTRY_TYPE.BREAK_IN,
                        REGISTRY_TYPE.CHECK_OUT,
                    ]

                    return (
                        s
                        # Se añade la categoría de fin de jornada laboral
                        .cat.add_categories([REGISTRY_TYPE.CHECK_OUT])
                        # Ordenamiento de categorías
                        .cat.reorder_categories(
                            new_items,
                            ordered= True
                        )
                    )
                else:

                    return (
                        s
                        # Ordenamiento de categorías
                        .cat.reorder_categories(
                            items,
                            ordered= True
                        )
                    )

            # Construcción de función a usar para la reasignación de columna con categorías ordenadas
            categorized_registry_type_assignation: ColumnAssignation = {
                COLUMN.REGISTRY_TYPE: (
                    lambda df: (
                        df[COLUMN.REGISTRY_TYPE]
                        # Se forza la asignación de tipo de dato a categoría
                        .astype('category')
                        # Reasignación y ordenamiento de categorías
                        .pipe(_handle_category_integrity)
                    )
                )
            }

            return (
                records
                # Reasignación de columna
                .assign(**categorized_registry_type_assignation)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.ADD_REGISTRY_TIME,
            requires= {
                COLUMN.DATE,
                COLUMN.TIME,
            },
            creates= {
                COLUMN.REGISTRY_TIME,
            },
        )
        def add_registry_time(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Asignación de fecha y hora de registro
            Esta función concatena fecha y hora  en base a las columnas `'date'` y
            `'time'` de los registros.

            :param records DataFrame: Datos entrantes.
            """

            # Función para extraer los datos de hora desde un texto
            extract_time_from_string: SeriesApply[str] = (
                lambda dt: (
                    dt.split(' ')[2]
                )
            )
            # Función para convertir delta de tiempo en texto
            from_timedelta_to_string_time: SeriesFromDataFrame = (
                lambda df: (
                    df[COLUMN.TIME]
                    .astype('string')
                    .apply(extract_time_from_string)
                )
            )

            # Columna de fecha y hora de registro
            string_registry_time: ColumnAssignation = {
                COLUMN.REGISTRY_TIME: (
                    lambda df: (
                        pd.to_datetime(
                            (
                                df
                                [COLUMN.DATE]
                                .astype('string[python]')
                            )
                            + ' '
                            + from_timedelta_to_string_time(df)
                        )
                    )
                )
            }

            return (
                records
                # Se asigna la columna de fecha y hora de registro
                .assign(**string_registry_time)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.JUSTIFICATIONS.GET_AND_KEEP_BY_USER_ID,
            requires= {
                COLUMN.NAME,
            },
            creates= {
                COLUMN.USER_ID,
            }
        )
        def get_and_keep_by_user_id(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de IDs de usuario
            Este pipe obtiene las IDs de los usuarios y filtra por todos los registros cuya
            ID de usuario fue hallada.

            :param records DataFrame: Datos entrantes.
            """

            # Obtención de categorías de nombres
            user_names_categories = (
                self._pipes_m._main._data.users
                [COLUMN.NAME]
                .unique()
                .tolist()
            )

            # Función para reasignar nombres como categorías
            reassign_name_categories: ColumnAssignation = {
                COLUMN.NAME: (
                    lambda df: (
                        df
                        [COLUMN.NAME]
                        .astype('category')
                        .cat.set_categories(user_names_categories)
                    )
                )
            }

            return (
                records
                # Obtención de la ID de usuario
                .pipe(
                    lambda df: (
                        pd.merge(
                            left= (
                                # Uso de los datos de usuarios
                                self._pipes_m._main._data.users
                                # Selección de columnas
                                [[COLUMN.USER_ID, COLUMN.NAME]]
                            ),
                            right= df,
                            on= COLUMN.NAME,
                            how= 'right'
                        )
                    )
                )
                # Se descartan los usuarios cuya ID no fue encontrada ya que están inactivos
                .pipe(lambda df: df[df[COLUMN.USER_ID].notna()])
                # Se reasignan los nombres como categorías
                .assign(**reassign_name_categories)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.JUSTIFICATIONS.FORMAT_PERMISSION_DATE_STRINGS,
            requires= {
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            },
        )
        def format_permission_date_strings(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Formateo de fechas de incidencias
            Este pipe convierte los valores de cadena de texto de las columnas de fecha de
            permiso en valores de tipo fecha.

            :param records DataFrame: Datos entrantes.
            """

            # Asignación de columnas a formatear
            formatted_days: ColumnAssignation = {
                col: (
                        lambda df, closure_col= col: (
                        pd.to_datetime(
                            df[closure_col],
                            dayfirst= True,
                        )
                    )
                ) for col in [
                    COLUMN.PERMISSION_START,
                    COLUMN.PERMISSION_END,
                ]
            }

            return (
                records
                # Formateo de fechas provenientes de los documentos de Google Sheets
                .assign(**formatted_days)
                # Se ordenan los datos por término de fecha de pérmiso
                .sort_values(COLUMN.PERMISSION_END)
            )

        @PipelineHub.register_method(
            PIPE.PROCESSING.RECORDS.PROCESS_BEFORE_SAVE_IN_DATABASE,
            renames= {
                ATTENDANCE_COLUMN.USER_ID: COLUMN.USER_ID,
                ATTENDANCE_COLUMN.NAME: COLUMN.NAME,
                ATTENDANCE_COLUMN.REGISTRY_TIME: COLUMN.REGISTRY_TIME,
                ATTENDANCE_COLUMN.REGISTRY_TYPE: COLUMN.REGISTRY_TYPE,
                ATTENDANCE_COLUMN.DEVICE: COLUMN.DEVICE,
            },
            creates= {COLUMN.ID},
        )
        def process_before_save_in_database(
            self: PipeMethods.Processing,
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Procesamiento antes de guardar en base de datos
            Este pipe reasigna nombres de columnas del DataFrame entrante para
            acondicionarlo para ser guardado en la base de datos y le crea una columna de
            ID de registro en la base de datos en base a la fecha, hora y el disposivo
            fuente de los registros.

            :param records DataFrame: Datos entrantes.
            """

            # Conversión de fecha a código
            to_code: SeriesApply[str] = (
                lambda value: (
                    value
                    .replace('-', '')
                    .replace(' ', '')
                    .replace(':', '')
                )
            )

            # Asignación de columna de ID
            id_assignation: ColumnAssignation = {
                COLUMN.ID: (
                    lambda df: (
                        (
                            df[COLUMN.DEVICE]
                            .astype(str)
                        ) + (
                            df[COLUMN.REGISTRY_TIME]
                            .astype(str)
                            .apply(to_code)
                        )
                    )
                )
            }

            return (
                records
                # Reasignación de nombres de columnas
                .rename(
                    columns= {
                        ATTENDANCE_COLUMN.USER_ID: COLUMN.USER_ID,
                        ATTENDANCE_COLUMN.NAME: COLUMN.NAME,
                        ATTENDANCE_COLUMN.REGISTRY_TIME: COLUMN.REGISTRY_TIME,
                        ATTENDANCE_COLUMN.REGISTRY_TYPE: COLUMN.REGISTRY_TYPE,
                        ATTENDANCE_COLUMN.DEVICE: COLUMN.DEVICE,
                    }
                )
                # Asignación de columna de ID de registro
                .assign(**id_assignation)
            )

        def _assign_ordered_registry_type(
            self: PipeMethods.Processing,
            data: pd.DataFrame,
        ) -> pd.DataFrame:

            # Obtención de los valores categóricos encontrados en el DataFrame
            available_values = data[COLUMN.REGISTRY_TYPE].cat.categories
            # Generación de los elementos ordenados y filtrados a usar para reordenamiento
            items = [value for value in ORDERED_REGISTRY_TYPE if value in available_values]

            # Construcción de función a usar para la reasignación de columna con categorías ordenadas
            categorized_registry_type_assignation: ColumnAssignation = {
                COLUMN.REGISTRY_TYPE: (
                    lambda df: (
                        df[COLUMN.REGISTRY_TYPE]
                        # Se forza la asignación de tipo de dato a categoría
                        .astype('category')
                        # Ordenamiento de categorías
                        .cat.reorder_categories(
                            items,
                            ordered= True
                        )
                    )
                )
            }

            return (
                data
                # Reasignación de columna
                .assign(**categorized_registry_type_assignation)
            )

    class Format(_Submodule):

        @PipelineHub.register_method(
            PIPE.COLUMNS_SELECTION.ASSISTANCE_RECORDS,
            selects= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.REGISTRY_TIME,
                COLUMN.DATE,
                COLUMN.TIME,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
            },
        )
        def select_columns_assistance_records(
            self: PipeMethods.Format,
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                records
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.REGISTRY_TIME,
                    COLUMN.DATE,
                    COLUMN.TIME,
                    COLUMN.REGISTRY_TYPE,
                    COLUMN.DEVICE,
                ]]
            )

        @PipelineHub.register_method(
            PIPE.COLUMNS_SELECTION.CORRECTIONS,
            selects= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.TIME,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.REGISTRY_TIME,
                COLUMN.IS_CORRECTION,
                COLUMN.NULL_BY_JUSTIFICATION,
            },
        )
        def select_columns_corrections(
            self: PipeMethods.Format,
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                records
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.TIME,
                    COLUMN.DATE,
                    COLUMN.REGISTRY_TYPE,
                    COLUMN.DEVICE,
                    COLUMN.REGISTRY_TIME,
                    COLUMN.IS_CORRECTION,
                    COLUMN.NULL_BY_JUSTIFICATION,
                ]]
            )

        @PipelineHub.register_method(
            PIPE.COLUMNS_SELECTION.JUSTIFICATIONS,
            selects= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.PERMISSION_TYPE,
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            },
        )
        def select_columns_justifications(
            self: PipeMethods.Format,
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                records
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.PERMISSION_TYPE,
                    COLUMN.PERMISSION_START,
                    COLUMN.PERMISSION_END,
                ]]
            )

        @PipelineHub.register_method(
            PIPE.COLUMNS_SELECTION.ASSISTANCE_RECORDS_UPDATE,
            selects= {
                COLUMN.ID,
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.REGISTRY_TIME,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
            },
        )
        def select_columns_assistance_records_update(
            self: PipeMethods.Format,
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                records
                # Selección de columnas
                [[
                    COLUMN.ID,
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.REGISTRY_TIME,
                    COLUMN.REGISTRY_TYPE,
                    COLUMN.DEVICE,
                ]]
            )
