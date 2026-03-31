import pandas as pd
import numpy as np
from datetime import (
    date,
    timedelta,
)
from typing import Callable
from attendance_registry._constants import COLUMN as ATTENDANCE_COLUMN
from ..constants import (
    COLUMN,
    PERMISSION_NAME,
    PIPE,
    REGISTRY_TYPE,
    TIME_DELTA_ON_ZERO,
    VALIDATION,
    NAN_TO_ZERO,
    NAN_TO_TIME_DELTA_ON_ZERO,
)
from ..contracts import _CoreRegistryProcessing
from ..contracts.pipes import _Contract_PipeMethods
from ..mapping import (
    ASSIGNED_DTYPES,
    ATTENDANCE_JUSTIFICATIONS_REASSIGNATION_NAMES,
    ORDERED_REGISTRY_TYPE,
    PERMISSION_TYPE_REASSIGNATION_NAMES,
    USERS_DATA_REASSIGNATION_NAMES,
    WAREHOUSE_RENAME,
)
from ..rules import (
    INITIAL_DATE_FOR_HOLIDAYS,
    VALIDATIONS_PER_DAY_AND_USER_ID,
)
from ..settings import INPUT
from ..core import pipeline_hub
from ..typing import ColumnAssignation
from ..typing.callables import (
    DataFramePipe,
    SeriesApply,
    SeriesFromDataFrame,
)
from ..typing.interfaces import (
    HorizontalSeries,
    Many2One,
)

class _BasePipeMethods():
    _main: _CoreRegistryProcessing

_Submodule = pipeline_hub.PipesOwner(_BasePipeMethods)

class PipeMethods(_Contract_PipeMethods, _BasePipeMethods):

    def __init__(
        self,
        main: _CoreRegistryProcessing,
    ) -> None:

        # Asignación de instancia principal
        self._main = main

        # Inicialización de submódulos de pipes
        self._data = self.Data(self)
        self._processing = self.Processing(self)
        self._processing_pivot = self.Processing.Pivoted(self)
        self._format = self.Format(self)
        self._report = self.Report(self)

    class Data(_Submodule):

        @pipeline_hub.register_method(
            PIPE.DATA.USERS.GET_WAREHOUSE_NAME,
            requires= {
                COLUMN.WAREHOUSE,
            },
        )
        def get_warehouse_name(
            self: 'PipeMethods.Data',
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

        @pipeline_hub.register_method(
            PIPE.DATA.USERS.GET_JOB_NAME,
            requires= {
                COLUMN.JOB,
            },
        )
        def get_job_name(
            self: 'PipeMethods.Data',
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

        @pipeline_hub.register_method(
            PIPE.DATA.CORRECTIONS.ADD_CORRECTION_TAG,
            creates= {
                COLUMN.IS_CORRECTION,
            },
        )
        def add_correction_tag(
            self: 'PipeMethods.Data',
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

        @pipeline_hub.register_method(
            PIPE.DATA.CORRECTIONS.SORT_BY_DATE,
            requires= {
                COLUMN.DATE,
            },
        )
        def sort_by_date(
            self: 'PipeMethods.Data',
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

        @pipeline_hub.register_method(
            PIPE.DATA.JUSTIFICATIONS.RENAME_COLUMNS,
            renames= {
                INPUT.FORM.PERMISSIONS.COLUMN.NAME: COLUMN.NAME,
                INPUT.FORM.PERMISSIONS.COLUMN.PERMISSION_TYPE: COLUMN.PERMISSION_TYPE,
                INPUT.FORM.PERMISSIONS.COLUMN.PERMISSION_START: COLUMN.PERMISSION_START,
                INPUT.FORM.PERMISSIONS.COLUMN.PERMISSION_END: COLUMN.PERMISSION_END,
            },
            selects= {
                COLUMN.NAME,
                COLUMN.PERMISSION_TYPE,
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            },
        )
        def rename_justifications_columns(
            self: 'PipeMethods.Data',
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

        @pipeline_hub.register_method(
            PIPE.DATA.EMPLOYEES_DATA.RENAME_COLUMNS,
            renames= {
                INPUT.FORM.USERS_DATA.COLUMN.USER_ID: COLUMN.USER_ID,
                INPUT.FORM.USERS_DATA.COLUMN.HIRE_DATE: COLUMN.HIRE_DATE,
                INPUT.FORM.USERS_DATA.COLUMN.SALARY_BY_SCHEMA: COLUMN.SALARY_BY_SCHEMA,
            },
        )
        def rename_employees_data_columns(
            self: 'PipeMethods.Data',
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            return (
                records
                # Reasignación de nombres de columna
                .rename(columns= USERS_DATA_REASSIGNATION_NAMES)
            )

    class Processing(_Submodule):

        @pipeline_hub.register_method(
            PIPE.PROCESSING.ASSIGN_DTYPES,
        )
        def assign_dtypes(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.TIME_FIRST_TO_STRING,
            requires= {
                COLUMN.TIME,
            },
            creates= {
                COLUMN.TIME,
            },
        )
        def time_first_to_string(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.NULL_BY_JUSTIFICATION,
            requires= {
                COLUMN.REGISTRY_TYPE,
            },
            creates= {
                COLUMN.NULL_BY_JUSTIFICATION,
            },
        )
        def null_by_justification(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.ASSIGN_ORDERED_REGISTRY_TYPE,
            requires= {
                COLUMN.REGISTRY_TYPE,
            },
            creates= {
                COLUMN.REGISTRY_TYPE,
            },
        )
        def assign_ordered_registry_type(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.JUSTIFICATIONS.GET_AND_KEEP_BY_USER_ID,
            requires= {
                COLUMN.NAME,
            },
            creates= {
                COLUMN.USER_ID,
            }
        )
        def get_and_keep_by_user_id(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.JUSTIFICATIONS.FORMAT_PERMISSION_DATE_STRINGS,
            requires= {
                COLUMN.PERMISSION_START,
                COLUMN.PERMISSION_END,
            },
        )
        def format_permission_date_strings(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.PROCESS_BEFORE_SAVE_IN_DATABASE,
            renames= {
                ATTENDANCE_COLUMN.USER_ID: COLUMN.USER_ID,
                ATTENDANCE_COLUMN.NAME: COLUMN.NAME,
                ATTENDANCE_COLUMN.REGISTRY_TIME: COLUMN.REGISTRY_TIME,
                ATTENDANCE_COLUMN.REGISTRY_TYPE: COLUMN.REGISTRY_TYPE,
                ATTENDANCE_COLUMN.DEVICE: COLUMN.DEVICE,
            },
            creates= {
                COLUMN.ID,
            },
        )
        def process_before_save_in_database(
            self: 'PipeMethods.Processing',
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

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.DISCARD_DUPLICATED,
            requires= {
                COLUMN.USER_ID,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.TIME,
            },
            creates= {
                COLUMN.IS_DUPLICATED,
            },
        )
        def discard_duplicated(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
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

            # Columnas temporales
            _CHECK_IN_DUPLICATED = '_check_in_duplicated'
            _CHECK_OUT_DUPLICATED = '_check_out_duplicated'

            # Columna que etiqueta todo tipo de registros duplicados con los criterios indicados
            is_duplicated: SeriesFromDataFrame = (
                lambda df: (
                    df
                    # Selección de columnas
                    [[
                        COLUMN.USER_ID,
                        COLUMN.DATE,
                        COLUMN.REGISTRY_TYPE,
                    ]]
                    # Se etiquetan registros duplicados
                    .duplicated()
                )
            )
            # El registro es inicio de jornada
            is_check_in: SeriesFromDataFrame = (
                lambda df: (
                    df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN
                )
            )
            # El registro es fin de jornada
            is_check_out: SeriesFromDataFrame = (
                lambda df: (
                    df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_OUT
                )
            )

            # Asignación de duplicados
            tag_duplicated: ColumnAssignation = {
                COLUMN.IS_DUPLICATED: (
                    lambda df: (
                        df[_CHECK_IN_DUPLICATED] | df[_CHECK_OUT_DUPLICATED]
                    )
                )
            }

            # Función para ordenar inicio de jornada duplicado
            def tag_check_in(data: pd.DataFrame) -> pd.DataFrame:

                # Asignación de etiqueta de inicio de jornada duplicado
                check_in_duplicated: ColumnAssignation = {
                    _CHECK_IN_DUPLICATED: (
                        lambda df: (
                            is_duplicated(df) & is_check_in(df)
                        )
                    )
                }

                return (
                    data
                    # Ordenamiento de registros
                    .sort_values(
                        [COLUMN.DATE, COLUMN.TIME],
                        ascending= [True, True],
                    )
                    # Asignación de etiqueta
                    .assign(**check_in_duplicated)
                )

            def tag_check_out(data: pd.DataFrame) -> pd.DataFrame:

                # Asignación de etiqueta de fin de jornada duplicado
                check_out_duplicated: ColumnAssignation = {
                    _CHECK_OUT_DUPLICATED: (
                        lambda df: (
                            is_duplicated(df) & is_check_out(df)
                        )
                    )
                }

                return (
                    data
                    # Ordenamiento de registros
                    .sort_values(
                        [COLUMN.DATE, COLUMN.TIME],
                        ascending= [True, False],
                    )
                    # Asignación de etiqueta
                    .assign(**check_out_duplicated)
                )

            # Columnas seleccionadas
            selected_columns = list(records.columns) + [COLUMN.IS_DUPLICATED]

            return (
                records
                # Se etiquetan los registros duplicados
                .pipe(tag_check_in)
                .pipe(tag_check_out)
                # Asignación de etiqueta de duplicados con todas las evaluaciones
                .assign(**tag_duplicated)
                # Se descartan todos los registros duplicados
                .pipe(lambda df: df[~df[COLUMN.IS_DUPLICATED]])
                # Se ordenan los registros
                .sort_values(
                    [COLUMN.DATE, COLUMN.TIME],
                    ascending= [True, True],
                )
                # Se conservan las columnas originales
                [selected_columns]
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.DISCARD_CORRECTED_RECORDS,
            requires= {
                COLUMN.USER_ID,
                COLUMN.DATE,
                COLUMN.TIME,
            }
        )
        def discard_corrected_records_from_original_data(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
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

            # Literal de indicador para descartar registros
            _TO_DROP = '_to_drop'

            return (
                records
                .pipe(
                    lambda records_: (
                        # Se unen los DataFrames de registros y correcciones
                        pd.merge(
                            left= records_,
                            right= (
                                self._pipes_m._main._data.corrections
                                # Se asigna la columna indicadora de eliminación
                                .assign(**{_TO_DROP: True})
                                # Selección de columnas
                                [[
                                    COLUMN.USER_ID,
                                    COLUMN.DATE,
                                    COLUMN.TIME,
                                    _TO_DROP,
                                ]]
                            ),
                            # Para coincidir, los registros deben ser iguales en ID de usuario, fecha y hora
                            left_on= [COLUMN.USER_ID, COLUMN.DATE, COLUMN.TIME],
                            right_on= [COLUMN.USER_ID, COLUMN.DATE, COLUMN.TIME],
                            how= 'left',
                        )
                        # Se descartan todos los registros que no aparecen reasignados en correcciones
                        .pipe(lambda merged_df: merged_df[ merged_df[_TO_DROP].isna() ] )
                        # Selección de las columnas originales del DataFrame de registros
                        [records_.columns]
                    )
                )
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.CONCAT_CORRECTIONS,
            creates= {
                COLUMN.IS_CORRECTION,
                COLUMN.IS_DUPLICATED,
                COLUMN.NULL_BY_JUSTIFICATION,
                COLUMN.DATE,
            },
        )
        def concat_corrections(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
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

            # Asignación para convertir los valores np.nan a booleano
            force_booleans: ColumnAssignation = {
                COLUMN.IS_CORRECTION: (
                    lambda df: (
                        ( df[COLUMN.IS_CORRECTION] )
                        .where(
                            df[COLUMN.IS_CORRECTION].isin([True, False]),
                            False
                        )
                    )
                ),
                COLUMN.IS_DUPLICATED: (
                    lambda df: (
                        ( df[COLUMN.IS_DUPLICATED] )
                        .where(
                            df[COLUMN.IS_DUPLICATED].isin([True, False]),
                            False
                        )
                    )
                ),
                COLUMN.NULL_BY_JUSTIFICATION: (
                    lambda df: (
                        ( df[COLUMN.NULL_BY_JUSTIFICATION] )
                        .where(
                            df[COLUMN.NULL_BY_JUSTIFICATION].isin([True, False]),
                            False
                        )
                    )
                ),
            }

            # Asignación de columnas en falso
            add_missing_columns_from_corrections: ColumnAssignation = {
                COLUMN.IS_CORRECTION: False,
                COLUMN.IS_DUPLICATED: False,
                COLUMN.NULL_BY_JUSTIFICATION: False,
            }

            # Obtención de DataFrame de correcciones filtrado
            filtered_corrections: pd.DataFrame = (
                self._pipes_m._main._data.corrections
                # Se toman los resultados dentro del rango de fechas de los registros
                .pipe(
                    lambda df: (
                        df[
                            (
                                ( df[COLUMN.DATE] >= records[COLUMN.DATE].min() )
                                & ( df[COLUMN.DATE] <= records[COLUMN.DATE].max() )
                            )
                        ]
                    )
                )
            )

            # Si el DataFrame de correcciones no está vacío...
            if not filtered_corrections.empty:
                # Se concatenan los registros y las correcciones
                concatenated_records = (
                    pd.concat([records, filtered_corrections])
                    # Se forza el tipo de dato a booleano en indicadores de "es corrección" y "es duplicado"
                    .assign(**force_booleans)
                )
            # Si el DataFrame de correcciones está vacío...
            else:
                # Se usan los datos sin correcciones por añadir
                concatenated_records = (
                    records
                    # Asignación de columnas faltantes en falso
                    .assign(**add_missing_columns_from_corrections)
                )

            return (
                concatenated_records
                # Ordenamiento por fecha de registro
                .sort_values(COLUMN.REGISTRY_TIME)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.TAG_VACATION_DAYS,
            requires= {
                COLUMN.USER_ID,
            },
            creates= {
                COLUMN.IS_VACATION,
            },
        )
        def tag_vacation_records(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Etiquetado de eventos en vacaciones
            Este pipe busca los eventos con valores de tipo de registro válido que hayan
            sido creados en los días en el usuario tiene incidencia de vacaciones.

            :param records DataFrame: Datos entrantes.
            """

            # Constante temporal
            _VALUES = '_values'

            # Función para forzar resultado a Pandas Series
            force_series: SeriesFromDataFrame = (
                lambda maybe_series: (
                    maybe_series
                        if isinstance(maybe_series, pd.Series)
                        else pd.Series([])
                )
            )

            # Función para forzar resultado a lista
            force_list: Callable[[pd.DataFrame], list[str]] = (
                lambda df: (
                    (
                        df
                        [_VALUES]
                        .sum()
                    )
                        if not df.empty
                        else []
                )
            )

            # Función para obtener valores de fechas donde existen incidencias de vacaciones del usuario
            get_vacation_dates: ColumnAssignation = {
                _VALUES: (
                    lambda df: (
                        df
                        .apply(
                            lambda s: (
                                pd.date_range(
                                    s[COLUMN.PERMISSION_START],
                                    s[COLUMN.PERMISSION_END],
                                )
                                .tolist()
                            ),
                            axis= 1
                        )
                        .pipe(force_series)
                    )
                )
            }

            # Mapeo para crear columna de registro en vacaciones
            initialize_is_vacation: ColumnAssignation = {
                COLUMN.IS_VACATION: False,
            }

            # Función para crear pipe que etiqueta registros de vacaciones por usuario
            def build_tag_vacation_records_by_user(user_id: int) -> Callable[[pd.DataFrame], pd.Series]:

                fn: SeriesFromDataFrame = (
                    lambda df: (
                        # Búsquedas donde existen incidencias de vacaciones
                        (
                            df
                            ['date']
                            .isin(
                                self._pipes_m._main.data.justifications
                                .pipe(
                                    lambda df: df[df[COLUMN.USER_ID] == user_id]
                                )
                                .pipe(lambda df: df[df[COLUMN.PERMISSION_TYPE] == 'Vacaciones'])
                                .assign(
                                    permission_start = lambda df: df[COLUMN.PERMISSION_START].dt.date,
                                    permission_end = lambda df: df[COLUMN.PERMISSION_END].dt.date,
                                )
                                .assign(**get_vacation_dates)
                                .pipe(force_list)
                            )
                        )
                        # La ID de usuario coincide con el usuario en valicación
                        & ( df[COLUMN.USER_ID] == user_id )
                    )
                )

                return fn

            # Función para etiquetar registros donde existen incidencias de vacaciones
            def tag_vacation_justifications_on_users(df: pd.DataFrame):

                # Obtención de las IDs de los usuarios en los datos
                user_ids: list[int] = df[COLUMN.USER_ID].unique().tolist()
                # Inicialización de la columna de registro en vacaciones
                df = df.assign(**initialize_is_vacation)

                # Iteración por cada ID de usuario
                for user_id in user_ids:

                    # Construcción del pipe de validación para el usuario
                    fn = build_tag_vacation_records_by_user(user_id)

                    # Reasignación de columna reuniendo todos los valores True en valicación
                    tag_vacation_records_by_user: ColumnAssignation = {
                        COLUMN.IS_VACATION: (
                            lambda df: (
                                df[COLUMN.IS_VACATION] | df.pipe(fn)
                            )
                        )
                    }

                    # Se añaden todos los valores en True
                    df = df.assign(**tag_vacation_records_by_user)

                return df

            return (
                records
                # Se etiquetan registros donde existen incidencias de vacaciones
                .pipe(tag_vacation_justifications_on_users)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.VACATION_EVENTS_TO_NULL_TYPE,
            requires= {
                COLUMN.IS_VACATION,
                COLUMN.REGISTRY_TYPE,
            },
        )
        def vacation_events_to_null_type(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Anulación de eventos en vacaciones del usuario
            Este pipe busca los eventos que hayan sido creados en los días en el usuario
            tiene incidencia de vacaciones y cambia sus valores de tipo de registro a nulo.

            :param records DataFrame: Datos entrantes.
            """

            # Función para corrección de tipo de registro en días de vacaciones del usuario
            registry_type_correction: ColumnAssignation = {
                COLUMN.REGISTRY_TYPE: (
                    lambda df: (
                        np.where(
                            df[COLUMN.IS_VACATION],
                            REGISTRY_TYPE.NULL,
                            df[COLUMN.REGISTRY_TYPE],
                        )
                    )
                )
            }

            return (
                records
                # Corrección de tipo de registro en días de vacaciones del usuario
                .assign(**registry_type_correction)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.ASSIGN_DAY_AND_USER_INDEX,
            requires= {
                COLUMN.USER_ID,
                COLUMN.DATE,
            },
            creates= {
                COLUMN.USER_AND_DATE_INDEX,
            }
        )
        def assign_day_and_user_id_index(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
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

            # Conversión de tipos de dato a string
            user_id_column_str: pd.Series = records[COLUMN.USER_ID].astype('string')
            date_column_str: pd.Series = records[COLUMN.DATE].astype('string')

            return (
                records
                .assign(
                    **{
                        COLUMN.USER_AND_DATE_INDEX: user_id_column_str + '|' + date_column_str
                    }
                )
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.VALIDATE_TODAY_CHECKIN,
            requires= {
                COLUMN.REGISTRY_TYPE,
                COLUMN.DATE,
            },
            creates= {
                COLUMN.IS_CURRENT_DAY_CHECKIN,
            }
        )
        def validate_today_check_in(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            # Función para validar si un registro es entrada del día en curso
            is_current_day_checkin: ColumnAssignation = {
                COLUMN.IS_CURRENT_DAY_CHECKIN: (
                    lambda df: (
                        # El tipo de registro es inicio de jornada laboral
                        ( df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN )
                        # La fecha del registro es igual a la fecha del día en curso
                        & ( df[COLUMN.DATE].dt.date == self._pipes_m._main._services.date.today )
                    )
                )
            }

            return (
                records
                # Validación de si es registro de entrada del día en curso
                .assign(**is_current_day_checkin)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.GET_DAILY_SCHEDULES,
            creates= {
                COLUMN.WEEKDAY,
                COLUMN.START_SCHEDULE,
                COLUMN.END_SCHEDULE,
            },
        )
        def get_daily_schedules(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de horarios laborales por día
            Este pipe asigna los horarios laborales establecidos a los registros del
            DataFrame entrante, en base al día de la semana de éstos.

            :param records DataFrame: Datos entrantes.
            """

            # Asignación de día de la semana
            weekday_assignation: ColumnAssignation = {
                COLUMN.WEEKDAY: (
                    lambda df: (
                        (
                            # Se convierte el tipo de dato a fecha de Pandas
                            pd.to_datetime( df[COLUMN.DATE] )
                            # Obtención de valor numérico de día de la semana
                            .dt.weekday
                            # Conversión a tipo de dato categórico
                            .astype('category')
                        )
                    )
                )
            }

            return (
                records
                # Asignación de día de la semana
                .assign(**weekday_assignation)
                # Obtención de horarios laborales por día
                .pipe(
                    lambda df: (
                        pd.merge(
                            left= df,
                            right= self._pipes_m._main._data.schedules,
                            left_on= COLUMN.WEEKDAY,
                            right_on= COLUMN.WEEKDAY,
                            how= 'left',
                        )
                    )
                )
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.GET_SCHEDULE_OFFSETS,
            requires= {
                COLUMN.USER_ID,
                COLUMN.DATE,
                COLUMN.WEEKDAY,
            },
            creates= {
                COLUMN.START_OFFSET,
                COLUMN.END_OFFSET,
            },
        )
        def get_schedule_offsets(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de desfases de horarios para gerentes
            Este pipe asigna los desfases de horarios para gerentes o un desfase en 0 para
            el resto de los empleados.

            :param records DataFrame: Datos entrantes.
            """


            return (
                records
                # Obtención de desfases de horarios para gerentes
                .pipe(
                    lambda df: (
                        pd.merge(
                            left= df,
                            right= self._pipes_m._main._data.schedule_offsets,
                            left_on= [COLUMN.USER_ID, COLUMN.WEEKDAY],
                            right_on= [COLUMN.USER_ID, COLUMN.WEEKDAY],
                            how= 'left',
                        )
                    )
                )
                # Los usuarios que no tienen desfases requieren que sus valores sean 0 y no np.nan
                .replace({
                    COLUMN.START_OFFSET: NAN_TO_TIME_DELTA_ON_ZERO,
                    COLUMN.END_OFFSET: NAN_TO_TIME_DELTA_ON_ZERO,
                })
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.ALLOWED_START_AND_END,
            requires= {
                COLUMN.DATE,
                COLUMN.START_SCHEDULE,
                COLUMN.END_SCHEDULE,
                COLUMN.START_OFFSET,
                COLUMN.END_OFFSET,
            },
            creates= {
                COLUMN.ALLOWED_START,
                COLUMN.ALLOWED_END,
            },
        )
        def define_allowed_start_and_end_time(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Definición de horarios de inicio y fin permitidos
            Este pipe combina la fecha del registro con los horarios base de cada día de la
            semana y sus ajustes de desfase por usuario para producir los valores completos
            de inicio y fin permitidos.

            :param records DataFrame: Datos entrantes.
            """

            # Asignación de tiempo permitido
            allowed_time_assignation: ColumnAssignation = {
                # Inicio de tiempo permitido
                COLUMN.ALLOWED_START: (
                    lambda df: (
                        # Se convierte el tipo de dato a fecha de Pandas
                        pd.to_datetime( df[COLUMN.DATE] )
                        # Se suma el tiempo de inicio general de jornada laboral
                        + df[COLUMN.START_SCHEDULE]
                        # Se suma el tiempo de desfase asignado al usuario
                        + df[COLUMN.START_OFFSET]
                    )
                ),
                # Fin de tiempo permitido
                COLUMN.ALLOWED_END: (
                    lambda df: (
                        # Se convierte el tipo de dato a fecha de Pandas
                        pd.to_datetime(df[COLUMN.DATE])
                        # Se suma el tiempo de fin general de jornada laboral
                        + df[COLUMN.END_SCHEDULE]
                        # Se suma el tiempo de desfase asignado al usuario
                        # (Los valores diferentes a cero son negativos, por eso se suman)
                        + df[COLUMN.END_OFFSET]
                    )
                ),
            }

            return (
                records
                # Asignación de tiempo permitido
                .assign(**allowed_time_assignation)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RECORDS.GET_CUMMULATED_TIME,
            requires= {
                COLUMN.REGISTRY_TYPE,
                COLUMN.REGISTRY_TIME,
                COLUMN.ALLOWED_START,
                COLUMN.ALLOWED_END,
            },
            creates= {
                VALIDATION.IS_LATE_START,
                VALIDATION.IS_EARLY_END,
                COLUMN.LATE_TIME,
                COLUMN.EARLY_TIME,
            },
        )
        def get_cummulated_time(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de tiempo acumulado de entrada tardía o salida anticipada
            Este pipe calcula para cada registro si hubo entrada tardía o salida
            anticipada, y en esos casos determina cuántos minutos (o el intervalo
            correspondiente) representan esa entrada tardía o salida anticipada.

            :param records DataFrame: Datos entrantes.
            """

            # Clasificación de entradas tardías y salidas anticipadas
            is_late_or_early_start: ColumnAssignation = {
                # Validación de entrada tardía
                VALIDATION.IS_LATE_START: (
                    lambda df: (
                        ( df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_IN )
                        & ( df[COLUMN.REGISTRY_TIME] > df[COLUMN.ALLOWED_START] )
                    )
                ),
                # Validación de salida anticipada
                VALIDATION.IS_EARLY_END: (
                    lambda df: (
                        ( df[COLUMN.REGISTRY_TYPE] == REGISTRY_TYPE.CHECK_OUT )
                        & ( df[COLUMN.REGISTRY_TIME] < df[COLUMN.ALLOWED_END] )
                    )
                ),
            }

            # Asignación de minutos de entrada tardía y salida anticipada
            late_and_early_time: ColumnAssignation = {
                # Obtención de minutos de entrada tardía
                COLUMN.LATE_TIME: (
                    lambda df: (
                        ( df[COLUMN.REGISTRY_TIME] - df[COLUMN.ALLOWED_START] )
                        .where(
                            df[VALIDATION.IS_LATE_START],
                            TIME_DELTA_ON_ZERO,
                        )
                    )
                ),
                # Obtención de minutos de salida anticipada
                COLUMN.EARLY_TIME: (
                    lambda df: (
                        ( df[COLUMN.ALLOWED_END] - df[COLUMN.REGISTRY_TIME] )
                        .where(
                            df[VALIDATION.IS_EARLY_END],
                            TIME_DELTA_ON_ZERO,
                        )
                    )
                ),
            }

            return (
                records
                # Clasificación de entradas tardías y salidas anticipadas
                .assign(**is_late_or_early_start)
                # Asignación de minutos de entrada tardía y salida anticipada
                .assign(**late_and_early_time)
            )

        @pipeline_hub.register_method(
            PIPE.COLUMNS_SELECTION.EVALUATE_REGISTRY_TIMES,
            selects= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.TIME,
                COLUMN.DATE,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.IS_DUPLICATED,

                COLUMN.REGISTRY_TIME,
                COLUMN.IS_CORRECTION,
                COLUMN.NULL_BY_JUSTIFICATION,
                VALIDATION.COMPLETE,
                VALIDATION.BREAK_PAIRS,
                VALIDATION.UNIQUE_START_AND_END,
                COLUMN.WEEKDAY,
                COLUMN.ALLOWED_START,
                COLUMN.ALLOWED_END,
                VALIDATION.IS_LATE_START,
                VALIDATION.IS_EARLY_END,
                COLUMN.IS_CURRENT_DAY_CHECKIN,
                COLUMN.IS_CLOSED_CORRECT,
                COLUMN.LATE_TIME,
                COLUMN.EARLY_TIME,
            },
        )
        def select_columns_evaluate_registry_times(
            self: 'PipeMethods.Processing',
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
                    COLUMN.IS_DUPLICATED,

                    COLUMN.REGISTRY_TIME,
                    COLUMN.IS_CORRECTION,
                    COLUMN.NULL_BY_JUSTIFICATION,
                    VALIDATION.COMPLETE,
                    VALIDATION.BREAK_PAIRS,
                    VALIDATION.UNIQUE_START_AND_END,
                    COLUMN.WEEKDAY,
                    COLUMN.ALLOWED_START,
                    COLUMN.ALLOWED_END,
                    VALIDATION.IS_LATE_START,
                    VALIDATION.IS_EARLY_END,
                    COLUMN.IS_CURRENT_DAY_CHECKIN,
                    COLUMN.IS_CLOSED_CORRECT,
                    COLUMN.LATE_TIME,
                    COLUMN.EARLY_TIME,
                ]]
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.RENAME_PERMISSION_NAMES,
            requires= {
                COLUMN.PERMISSION_TYPE,
            },
        )
        def rename_permission_types(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            # Reasignación de columna con los nombres reemplazados
            renamed_permission_types: ColumnAssignation = {
                COLUMN.PERMISSION_TYPE: (
                    lambda df: (
                        # Uso de la columna de tipo de permiso
                        df[COLUMN.PERMISSION_TYPE]
                        # Se convierte el tipo de dato a cadena de texto para perder referencia en categorías
                        .astype('string')
                        # Reemplazo de valores
                        .replace(PERMISSION_TYPE_REASSIGNATION_NAMES)
                        # Reasignación de tipo de dato a categoría
                        .astype('category')
                    )
                )
            }

            return (
                records
                # Reasignación de columna con los nombres reemplazados
                .assign(**renamed_permission_types)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.GET_HOLIDAY_JUSTIFICATIONS,
            requires= {
                COLUMN.PERMISSION_TYPE,
            },
        )
        def get_holiday_justifications(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de permisos relacionados a días festivos
            Este pipe filtra los registros de incidencias para conservar únicamente los
            registros que tengan permisos relacionados con días festivos.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                # Se filtran los registros que solo contengan incidencias relacionadas a días festivos
                .pipe(
                    lambda df: (
                        df[
                            df[COLUMN.PERMISSION_TYPE]
                            .isin([
                                PERMISSION_NAME.HOLIDAY_COMPENSATION,
                                PERMISSION_NAME.HOLIDAY_ABSENCE,
                            ])
                        ]
                    )
                )
                # Se forza el tipo de dato a string
                .astype({
                    COLUMN.PERMISSION_TYPE: 'string[python]',
                })
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.COUNT_HOLIDAY_JUSTIFICATIONS_PER_EMPLOYEE,
            requires= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.PERMISSION_TYPE,
            },
            creates= {
                PERMISSION_NAME.HOLIDAY_ABSENCE,
                PERMISSION_NAME.HOLIDAY_COMPENSATION,
            },
            selects= {
                COLUMN.USER_ID,
                PERMISSION_NAME.HOLIDAY_ABSENCE,
                PERMISSION_NAME.HOLIDAY_COMPENSATION,
            },
        )
        def count_holiday_justifications_per_employee(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Contar incidencias de días festivos por empleado
        
            Este pipe toma las incidencias de días festivos, filtra desde los registros
            posteriores a la fecha considerada inicio de conteo y cuenta las incidencias
            existentes para cada empleado.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                .pipe(
                    lambda df: (
                        df[df[COLUMN.PERMISSION_START].dt.date >= INITIAL_DATE_FOR_HOLIDAYS]
                    )
                )
                # Agrupamiento por ID de usuario y tipo de permiso para conteo
                .groupby([
                    COLUMN.USER_ID,
                    COLUMN.PERMISSION_TYPE,
                ])
                .agg({
                    COLUMN.NAME: 'count',
                })
                # Reseteo de índice
                .reset_index()
                # Pivoteo de tabla para obtener columnas con conteos por empleado
                .pivot_table(
                    values= COLUMN.NAME,
                    index= COLUMN.USER_ID,
                    columns= COLUMN.PERMISSION_TYPE,
                )
                # Reemplazo de valores nulos a 0
                .replace(NAN_TO_ZERO)
                # Conversión de tipos de dato
                .astype({
                    PERMISSION_NAME.HOLIDAY_ABSENCE: 'uint8',
                    PERMISSION_NAME.HOLIDAY_COMPENSATION: 'uint8',
                })
                # Reseteo de índice
                .reset_index()
            )

        def _assign_ordered_registry_type(
            self: 'PipeMethods.Processing',
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            # Obtención de los valores categóricos encontrados en el DataFrame
            available_values = records[COLUMN.REGISTRY_TYPE].cat.categories
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
                records
                # Reasignación de columna
                .assign(**categorized_registry_type_assignation)
            )

        class Pivoted(_Submodule):

            @pipeline_hub.register_method(
                PIPE.PROCESSING.RECORDS.PIVOTED.COUNT_PER_REGISTRY_TYPE,
                requires= {
                    COLUMN.USER_AND_DATE_INDEX,
                    COLUMN.REGISTRY_TYPE,
                },
                creates= {
                    REGISTRY_TYPE.CHECK_IN,
                    REGISTRY_TYPE.BREAK_OUT,
                    REGISTRY_TYPE.BREAK_IN,
                    REGISTRY_TYPE.CHECK_OUT,
                },
                selects= {
                    COLUMN.USER_AND_DATE_INDEX,
                    REGISTRY_TYPE.CHECK_IN,
                    REGISTRY_TYPE.BREAK_OUT,
                    REGISTRY_TYPE.BREAK_IN,
                    REGISTRY_TYPE.CHECK_OUT,
                },
            )
            def count_per_registry_type(
                self: 'PipeMethods.Processing.Pivoted',
                records: pd.DataFrame,
            ) -> pd.DataFrame:
                """
                ### Conteo por tipo de registro
                Este pipe realiza un pivoteo de tabla usando:

                - Índice: Índice de ID de usuario/fecha.
                - Columnas: Tipo de registro.
                - Valores: Conteo de tipos de registro por índice de ID de usuario/fecha.

                :param records DataFrame: Datos entrantes.
                """

                # Declaración de registros catalogados como inválidos
                invalid_registry_types = [
                    REGISTRY_TYPE.NULL,
                    REGISTRY_TYPE.UNDEFINED,
                ]

                return (
                    records
                    .pipe(
                        lambda df: (
                            df
                            # Agrupamiento por índice de ID de usuario/fecha y tipo de registro
                            .groupby(
                                [
                                    COLUMN.USER_AND_DATE_INDEX,
                                    COLUMN.REGISTRY_TYPE,
                                ],
                                observed= False,
                            )
                            # Conteo de registros por índice de ID de usuario/fecha
                            .agg( {COLUMN.USER_AND_DATE_INDEX: COLUMN.COUNT} )
                            # Se renombra la columna de índice de ID de usuario/fecha a 'conteo'
                            .rename(
                                columns= {COLUMN.USER_AND_DATE_INDEX: COLUMN.COUNT},
                            )
                            # Se restablece el índice del DataFrame
                            .reset_index()
                            # Se descartan todos los registros que no tengan tipo de registro especificado
                            .pipe(
                                lambda grouped_df: (
                                    grouped_df[ ~grouped_df[COLUMN.REGISTRY_TYPE].isin(invalid_registry_types) ]
                                )
                            )
                        )
                    )
                    # Pivoteo de tabla para obtención de conteos explícitos
                    .pivot_table(
                        index= COLUMN.USER_AND_DATE_INDEX,
                        columns= COLUMN.REGISTRY_TYPE,
                        values= COLUMN.COUNT,
                        observed= False,
                    )
                    # Se restablece el índice del DataFrame
                    .reset_index()
                    # Se establecen a cero todos los np.nan
                    .replace(NAN_TO_ZERO)
                    # Se establecen los tipos de dato a entero de 8 bits
                    .astype({
                        REGISTRY_TYPE.CHECK_IN: 'uint8',
                        REGISTRY_TYPE.BREAK_OUT: 'uint8',
                        REGISTRY_TYPE.BREAK_IN: 'uint8',
                        REGISTRY_TYPE.CHECK_OUT: 'uint8',
                    })
                )

            @pipeline_hub.register_method(
                PIPE.PROCESSING.RECORDS.PIVOTED.VALIDATE_RECORDS,
                requires= {
                    REGISTRY_TYPE.CHECK_IN,
                    REGISTRY_TYPE.BREAK_OUT,
                    REGISTRY_TYPE.BREAK_IN,
                    REGISTRY_TYPE.CHECK_OUT,
                },
                creates= {
                    VALIDATION.COMPLETE,
                    VALIDATION.BREAK_PAIRS,
                    VALIDATION.UNIQUE_START_AND_END,
                },
            )
            def validate_day_pivoted_records(
                self: 'PipeMethods.Processing.Pivoted',
                records: pd.DataFrame,
            ) -> pd.DataFrame:
                """
                ### Validación de registros por usuario/día
                Este pipe recibe un DataFrame pivote por usuario y día, y le inyecta
                validaciones produciendo columnas booleanas que indican si cada día/usuario
                pasa o no cada regla.

                - Validación de que existen los cuatro registros
                - Validación de que conteo de registros de comida son pares

                :param records DataFrame: Datos entrantes.
                """

                return (
                    records
                    # Se asignan las validaciones por día/ID de usuario
                    .assign(
                        **VALIDATIONS_PER_DAY_AND_USER_ID
                    )
                )

            @pipeline_hub.register_method(
                PIPE.COLUMNS_SELECTION.PIVOTED.RECORDS,
                requires= {
                    COLUMN.USER_AND_DATE_INDEX,
                    VALIDATION.COMPLETE,
                    VALIDATION.BREAK_PAIRS,
                    VALIDATION.UNIQUE_START_AND_END,
                },
            )
            def select_columns_pivot_records(
                self: 'PipeMethods.Processing.Pivoted',
                records: pd.DataFrame,
            ) -> pd.DataFrame:
                """
                ### Validación de registros por usuario/día
                Esta función recibe un DataFrame pivote por usuario y día, y le inyecta
                validaciones produciendo columnas booleanas que indican si cada día/usuario
                pasa o no cada regla.

                - Validación de que existen los cuatro registros
                - Validación de que conteo de registros de comida son pares

                :param records DataFrame: Datos entrantes.
                """

                # Obtención de los nombres de las validaciones en lista
                validation_names = list( VALIDATIONS_PER_DAY_AND_USER_ID.keys() )
                # Se usa el índice usuario/día y las columnas generadas por las validaciones
                selected_columns = [COLUMN.USER_AND_DATE_INDEX] + validation_names

                return (
                    records
                    # Selección de columnas
                    [selected_columns]
                )

    class Report(_Submodule):

        @pipeline_hub.register_method(
            PIPE.REPORT.GET_EMPLOYEE_DATA_FOR_USER,
            requires= {
                COLUMN.USER_ID,
            },
            creates= {
                COLUMN.HIRE_DATE,
                COLUMN.SALARY_BY_SCHEMA,
            },
        )
        def get_employee_data_for_user(
            self: 'PipeMethods.Report',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de datos de empleado
            Este pipe obtiene los datos de empleado de los registros provistos, como la
            fecha de ingreso.onteo y cuenta las incidencias
            existentes para cada empleado.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                .merge(
                    right= self._pipes_m._main.data.employees_data,
                    on= COLUMN.USER_ID,
                    how= 'left',
                )
            )

        @pipeline_hub.register_method(
            PIPE.REPORT.COMPUTE_AVAILABLE_HOLIDAYS,
            creates= {
                COLUMN.INITIAL_DATE_FOR_HOLIDAYS,
            },
        )
        def add_initial_date_for_holidays(
            self: 'PipeMethods.Report',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de días festivos disponibles
            Este pipe obtiene la fecha más reciente entre la fecha de ingreso o la fecha de
            inicio de conteo de días festivos para hacer un correcto cálculo en los días
            que un empleado puede tomar, según su fecha de ingreso y la fecha de inicio de
            conteo y posteriormente realiza el conteo de éstos.

            :param records DataFrame: Datos entrantes.
            """

            # Diccionario para añadir la columna de fecha inicial para días festivos
            initial_date_for_holidays: ColumnAssignation = {
                COLUMN.INITIAL_DATE_FOR_HOLIDAYS: INITIAL_DATE_FOR_HOLIDAYS,
            }

            # Diccionario para añadir inicio y final de rango para buscar días festivos
            add_range_dates: ColumnAssignation = {
                COLUMN.START_RANGE: (
                    lambda df: (
                        df[[COLUMN.HIRE_DATE, COLUMN.INITIAL_DATE_FOR_HOLIDAYS]].max(axis= 1)
                    )
                ),
                COLUMN.END_RANGE: date.today(),
            }

            def get_available_holidays_by_employee(s: HorizontalSeries) -> int:

                # Obtención de valores correspondientes
                start = s[COLUMN.START_RANGE].date()
                end = s[COLUMN.END_RANGE].date()

                # Obtención de lista de días festivos
                total_available_holidays = (
                    self._pipes_m._main._data.holidays
                    # Se filtran los días festivos que se encuentren dentro del rango
                    .pipe(
                        lambda df: (
                            df[
                                (df[COLUMN.HOLIDAY_DATE].dt.date >= start)
                                & (df[COLUMN.HOLIDAY_DATE].dt.date <= end)
                            ]
                        )
                    )
                    # Selección de la columna
                    [COLUMN.HOLIDAY_DATE]
                    # Conversión a lista
                    .to_list()
                )

                return (
                    # Construcción de rango de fechas
                    pd.date_range(start, end)
                    # Validación de qué días son festivos
                    .isin(total_available_holidays)
                    # Suma de días festivos encontrados
                    .sum()
                )

            # Diccionario para asignar cantidad de días festivos disponibles para el empleado
            available_holidays: ColumnAssignation = {
                COLUMN.AVAILABLE_HOLIDAYS: (
                    lambda df: (
                        df[[COLUMN.START_RANGE, COLUMN.END_RANGE]]
                        .apply(
                            get_available_holidays_by_employee,
                            axis= 1,
                        )
                    )
                )
            }

            return (
                records
                # Se añade la columna de fecha inicial para días festivos
                .assign(**initial_date_for_holidays)
                # Asignación de tipo de dato
                .astype({
                    COLUMN.INITIAL_DATE_FOR_HOLIDAYS: 'datetime64[s]',
                })
                # Se añaden las fechas de inicio y final de rango para buscar días festivos
                .assign(**add_range_dates)
                # Asignación de tipo de dato
                .astype({
                    COLUMN.START_RANGE: 'datetime64[s]',
                    COLUMN.END_RANGE: 'datetime64[s]',
                })
                
                # Asignación de conteo de días festivos disponibles
                .assign(**available_holidays)
            )

        @pipeline_hub.register_method(
            PIPE.PROCESSING.GET_REMAINING_HOLIDAYS,
            requires= {
                COLUMN.AVAILABLE_HOLIDAYS,
                PERMISSION_NAME.HOLIDAY_ABSENCE,
                PERMISSION_NAME.HOLIDAY_COMPENSATION,
            },
            creates= {
                COLUMN.REMAINING_HOLIDAYS,
            },
        )
        def get_remaining_holidays(
            self: 'PipeMethods.Report',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Obtención de días festivos restantes
            Este pipe computa los días festivos restantes para tomar por el empleado.

            :param records DataFrame: Datos entrantes.
            """

            # Función para calcular los días festivos restantes para tomar por el empleado
            remaining_holidays: ColumnAssignation = {
                COLUMN.REMAINING_HOLIDAYS: (
                    lambda df: (
                        df[COLUMN.AVAILABLE_HOLIDAYS]
                        - df[PERMISSION_NAME.HOLIDAY_ABSENCE]
                        - df[PERMISSION_NAME.HOLIDAY_COMPENSATION]
                    )
                )
            }

            return (
                records
                # Se reemplazan los np.nan por ceros
                .replace({
                    PERMISSION_NAME.HOLIDAY_ABSENCE: NAN_TO_ZERO,
                    PERMISSION_NAME.HOLIDAY_COMPENSATION: NAN_TO_ZERO,
                })
                # Corrección de tipos de dato
                .astype({
                    PERMISSION_NAME.HOLIDAY_ABSENCE: 'uint8',
                    PERMISSION_NAME.HOLIDAY_COMPENSATION: 'uint8',
                })
                # Cálculo los días festivos restantes para tomar por el empleado
                .assign(**remaining_holidays)
            )

    class Format(_Submodule):

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Format',
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

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Format',
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

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Format',
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

        @pipeline_hub.register_method(
            PIPE.COLUMNS_SELECTION.EMPLOYEES_DATA,
            selects= {
                COLUMN.USER_ID,
                COLUMN.HIRE_DATE,
                COLUMN.SALARY_BY_SCHEMA,
            },
        )
        def select_column_employees_data(
            self: 'PipeMethods.Format',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            """
            ### Selección de columnas
            Este pipe selecciona las columnas indicadas para controlar la forma del
            DataFrame resultante y modificarlo explícitamente si se desea agregar otra
            columna.

            :param records DataFrame: Datos entrantes.
            """

            return (
                records
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.HIRE_DATE,
                    COLUMN.SALARY_BY_SCHEMA,
                ]]
            )

        @pipeline_hub.register_method(
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
            self: 'PipeMethods.Format',
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

        @pipeline_hub.register_method(
            PIPE.COLUMNS_SELECTION.VALIDATE_TODAY_CHECKIN,
            requires= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.REGISTRY_TIME,
                COLUMN.DATE,
                COLUMN.TIME,
                COLUMN.REGISTRY_TYPE,
                COLUMN.DEVICE,
                COLUMN.IS_DUPLICATED,
                COLUMN.IS_CORRECTION,
                COLUMN.NULL_BY_JUSTIFICATION,
                COLUMN.IS_VACATION,
                VALIDATION.COMPLETE,
                VALIDATION.BREAK_PAIRS,
                VALIDATION.UNIQUE_START_AND_END,
            },
        )
        def select_columns_validate_today_checkin(
            self: 'PipeMethods.Format',
            records: pd.DataFrame,
        ) -> pd.DataFrame:

            # Obtención de los nombres de las validaciones en lista
            validation_names = list( VALIDATIONS_PER_DAY_AND_USER_ID.keys() )

            # Selección de columnas
            selected_columns = (
                [
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.REGISTRY_TIME,
                    COLUMN.DATE,
                    COLUMN.TIME,
                    COLUMN.REGISTRY_TYPE,
                    COLUMN.DEVICE,
                    COLUMN.IS_DUPLICATED,
                    COLUMN.IS_CORRECTION,
                    COLUMN.NULL_BY_JUSTIFICATION,
                    COLUMN.IS_CURRENT_DAY_CHECKIN,
                    COLUMN.IS_VACATION,
                ]
                + validation_names
            )

            return (
                records
                # Selección de columnas
                [selected_columns]
            )

        @pipeline_hub.register_method(
            PIPE.COLUMNS_SELECTION.HOLIDAYS_SUMMARY,
            selects= {
                COLUMN.USER_ID,
                COLUMN.NAME,
                COLUMN.WAREHOUSE,
                COLUMN.PAY_FREQUENCY,
                COLUMN.HIRE_DATE,
                COLUMN.AVAILABLE_HOLIDAYS,
                PERMISSION_NAME.HOLIDAY_ABSENCE,
                PERMISSION_NAME.HOLIDAY_COMPENSATION,
                COLUMN.REMAINING_HOLIDAYS,
            }
        )
        def select_columns_holidays_summary(
            self: 'PipeMethods.Format',
            records: pd.DataFrame,
        ) -> pd.DataFrame:
            
            return (
                records
                # Selección de columnas
                [[
                    COLUMN.USER_ID,
                    COLUMN.NAME,
                    COLUMN.WAREHOUSE,
                    COLUMN.PAY_FREQUENCY,
                    COLUMN.HIRE_DATE,
                    COLUMN.AVAILABLE_HOLIDAYS,
                    PERMISSION_NAME.HOLIDAY_ABSENCE,
                    PERMISSION_NAME.HOLIDAY_COMPENSATION,
                    COLUMN.REMAINING_HOLIDAYS,
                ]]
            )
