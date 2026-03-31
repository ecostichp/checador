from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    TypeVar,
    get_type_hints,
)
import pandas as pd
from ...typing.callables import (
    DataFramePipe,
    PipeRegistryDecorator,
)
from ...contexts import ColumnsContext
from ...resources import (
    PipeMetadata,
    PipesExecutionMetadata,
)

_ExtOwner = TypeVar('_Extension')

class PipelineHub:

    _pipelines: dict[str, PipeMetadata]
    """
    Pipelines disponibles para ser ejecutados.
    """

    def __init__(
        self,
    ) -> None:

        # Inicialización de pipelines
        self._pipelines = {}

    def register_method(
        self,
        name: str,
        /,
        requires: Optional[set[str]] = None,
        creates: Optional[set[str]] = None,
        selects: Optional[set[str]] = None,
        renames: Optional[dict[str, str]] = None,
    ) -> PipeRegistryDecorator:

        def decorator(pipe_fn: DataFramePipe) -> DataFramePipe:

            # Regustro de la función en la clase
            self._register_pipe_function(
                name= name,
                pipe_fn= pipe_fn,
                requires= requires,
                creates= creates,
                selects= selects,
                renames= renames,
            )

            return pipe_fn

        return decorator

    def run_pipe_flow(
        self,
        df: pd.DataFrame,
        pipe_flow: list[str],
        debug: bool = False,
        test: bool = False
    ) -> pd.DataFrame:

        # Validación del pipe
        self._validate_pipe(df, pipe_flow)

        # Inicialización de lista de metadatos
        execution_metadata = PipesExecutionMetadata()

        # Iteración por cada pipe
        for pipe_name in pipe_flow:
            # Obtención de los metadatos del pipe
            pipe = self._pipelines[pipe_name]
            # Obtención de la función del pipe
            pipe_fn = pipe.fn
            # Obtención de los metadatos de entrada
            input_df_metadata = execution_metadata.get_dataframe_metadata(df)

            # Intento de ejecución del pipe
            try:
                if test:
                    self._test_real_columns(df, pipe, execution_metadata)
                # Ejecución del pipe y obtención del resultado
                df = pipe_fn(df)

            # Si la ejecución falla...
            except Exception as e:
                # Se imprime el traceback de la ejecución
                print(execution_metadata)
                # Se lanza el error
                raise e

            # Obtención de los metadatos de salida
            output_df_metadata = execution_metadata.get_dataframe_metadata(df)
            # Registro de los metadatos de la ejecución del pipe
            in_out_metadata = execution_metadata.get_io_metadata(pipe_name, input_df_metadata, output_df_metadata)
            # Si el modo debug está activado...
            if debug:
                # Impresión de los metadatos de entrada/salida
                print(in_out_metadata)

        return df

    def _test_real_columns(
        self,
        df: pd.DataFrame,
        pipe: PipeMetadata,
        execution_metadata: PipesExecutionMetadata,
    ) -> None:

        # Obtención de las columnas de entrada y salida
        input_columns = pipe.specs.input_columns
        output_columns = pipe.specs.output_columns

        df = df.copy()

        # Obtención de la función del pipe
        pipe_fn = pipe.fn

        # Intento de ejecución del pipe
        try:
            # Ejecución únicamente con las columnas requeridas
            if input_columns:
                pipe_fn(df[input_columns])

            if output_columns:
                # Ejecución y selección de las columnas especificadas
                pipe_fn(df)[output_columns]

        # Si la ejecución falla...
        except Exception as e:
            # Se imprime el traceback de la ejecución
            print(execution_metadata)
            # Se lanza el error
            raise e

    def _register_pipe_function(
        self,
        *,
        name: str,
        pipe_fn: DataFramePipe,
        requires: Optional[set[str]] = None,
        creates: Optional[set[str]] = None,
        selects: Optional[set[str]] = None,
        renames: Optional[dict[str, str]] = None,
    ) -> None:

        # Inicialización de conjuntos y diccionarios vacíos de ser necesario
        requires = requires or set()
        creates = creates or set()
        selects = selects or set()
        renames = renames or {}

        # Construcción de objeto de metadatos de función pipe
        pipeline_metadata = PipeMetadata(pipe_fn, name, requires, creates, selects, renames)
        # Registro de función en la clase
        self._pipelines[name] = pipeline_metadata

    def _validate_pipe(
        self,
        df: pd.DataFrame,
        pipe_flow: list[str],
    ) -> None:

        # Inicialización de objeto de columnas
        columns_ctx = ColumnsContext(df)

        # Iteración por cada pipe
        for pipe_name in pipe_flow:
            # Obtención de las especificaciones del pipe
            specs = self._pipelines[pipe_name].specs

            # Validación de requirimientos de columnas
            columns_ctx.validate_flow_step_columns_requirements(
                pipe_name,
                specs.requires,
                specs.renames,
                specs.creates,
                specs.selects,
            )

    def PipesOwner(
        self,
        _: type[_ExtOwner]
    ) -> type['PipelineHub._BaseOwner[_ExtOwner]']:

        # Alias para poder introducir dentro de alcance local
        hub = self

        # Creación de clase de propietario de instancia propietaria de métodos de pipes
        class _Owner(Generic[_ExtOwner]):

            def __init__(
                self,
                pipes_m: _ExtOwner,
            ) -> None:

                # Asignación de instancia propietaria
                self._pipes_m = pipes_m
                # Inicialización de pipes
                self._initialize_pipes(hub)

            def _initialize_pipes(
                self,
                hub: PipelineHub,
            ) -> None:

                # Iteración por cada atributo de la clase
                for maybe_method_name in self._valid_existing_attributes:

                    # Se busca el nombre del método en la colección de pipes
                    for pipe_metadata in hub._pipelines.values():
                        # Obtención del nombre real del método desde las especificaciones
                        method_real_name = pipe_metadata.specs.real_name

                        # Si el nombre real registrado coincide con el registrado como pipe...
                        if maybe_method_name == method_real_name:

                            # Obtención del objeto del atributo
                            value_object: Any = getattr(self, maybe_method_name)
                            # Obtención del método desde las especificaciones
                            method_executable = pipe_metadata.fn

                            # Obtención del tipado del método
                            hints = get_type_hints(method_executable)

                            # Obtención de las clases propietarias de los objetos
                            registered_method_owner: type[_Owner] = hints.get('self')
                            maybe_method_executable_owner: type[_Owner] = type(value_object.__self__)

                            # Validación de que pertenecen a la misma clase
                            if maybe_method_executable_owner is registered_method_owner:
                                # Reasignación del método implementado correctamente
                                pipe_metadata.fn = value_object

            @property
            def _valid_existing_attributes(
                self,
            ) -> Generator[str, Any, None]:

                # Iteración por cada atributo de la clase
                for maybe_method_name in self.__dir__():
                    # Se descartan los builtins
                    if maybe_method_name.startswith('__'):
                        continue
                    # Se usa el nombre del método
                    yield maybe_method_name

        return _Owner[_ExtOwner]

    class _BaseOwner(Generic[_ExtOwner]):
        _pipes_m: _ExtOwner
