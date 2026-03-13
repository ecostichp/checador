from dataclasses import dataclass
from typing import LiteralString
import pandas as pd
from ..constants import PIPE_VALIDATION_ERROR_MESSAGE_ARGS
from ..templates.errors import PIPE_VALIDATION_ERROR_MESSAGE
from ..typing.literals import PipeValidationStage

class ColumnsContext:

    # Mensajes de error en columnas faltantes
    _error_messages: dict[PipeValidationStage, LiteralString] = {
        'require': PIPE_VALIDATION_ERROR_MESSAGE.REQUIRE,
        'rename': PIPE_VALIDATION_ERROR_MESSAGE.RENAME,
        'select': PIPE_VALIDATION_ERROR_MESSAGE.SELECT,
    }

    def __init__(
        self,
        data_in_initial_state: pd.DataFrame,
    ) -> None:

        # Obtención de columnas iniciales
        self.existing_columns = set(data_in_initial_state.columns)
        # Inicialización de traceback
        self.traceback = self._Traceback([])
        # Inicialización de estado inicial de columnas
        initial_columns_state = self._State('[Estado inicial]', frozenset(self.existing_columns))
        # Se añade éste al traceback
        self.traceback.add(initial_columns_state)

    def validate_flow_step_columns_requirements(
        self,
        pipe_name: str,
        required_columns: set[str],
        columns_to_rename: dict[str, str],
        columns_to_create: set[str],
        columns_to_select: set[str],
    ) -> None:

        # Validación en columnas requeridas faltantes
        self._validate_required_missing_columns(required_columns, pipe_name)
        # Validación de columnas a renombrar
        self._validate_required_columns_to_rename(columns_to_rename, pipe_name)
        # Adición de columnas a crear
        self._add_columns_to_create(columns_to_create)
        # Validación de columnas a seleccionar
        self._validate_required_columns_to_select(columns_to_select, pipe_name)

        # Se guarda el estado actual del contexto
        self._record_current_state(pipe_name)

    def _validate_required_missing_columns(
        self,
        required_columns: set[str],
        pipe_name: str,
    ) -> None:

        # Se buscan columnas requeridas faltantes
        missing_required_columns = required_columns - self.existing_columns
        # Si existen columnas requeridas faltantes...
        if missing_required_columns:
            # Se lanza error de columnas faltantes
            self._display_error('require', missing_required_columns, pipe_name)

    def _validate_required_columns_to_rename(
        self,
        columns_to_rename: dict[str, str],
        pipe_name: str,
    ) -> None:

        # Si existen columnas a renombrar...
        if columns_to_rename:
            # Obtención de conjunto de mapeo de columnas
            column_old_names = set( columns_to_rename.keys() )
            column_new_names = set( columns_to_rename.values() )

            # Se buscan columnas requeridas faltantes
            missing_columns_to_rename = column_old_names - self.existing_columns

            # Si existen columnas a renombrar faltantes...
            if missing_columns_to_rename:
                # Se lanza error de columnas faltantes
                self._display_error('rename', missing_columns_to_rename, pipe_name)

            # Extracción de las columnas cuyo nombre va a reasignarse
            self.existing_columns -= column_old_names
            # Inserción de las columnas con nombre reasignado
            self.existing_columns |= column_new_names

    def _add_columns_to_create(
        self,
        columns_to_create: set[str],
    ) -> None:

            # Se añaden las columnas creadas a las existentes
            self.existing_columns |= columns_to_create

    def _validate_required_columns_to_select(
        self,
        columns_to_select: set[str],
        pipe_name: str,
    ) -> None:

        # Si se especificaron columnas seleccionadas...
        if columns_to_select:
            # Se buscan columnas seleccionadas faltantes
            missing_selected_columns = columns_to_select - self.existing_columns
            # Si existen columnas seleccionadas faltantes...
            if missing_selected_columns:
                # Se lanza error de columnas faltantes
                self._display_error('select', missing_selected_columns, pipe_name)

            # Se reasignan las columnas disponibles
            self.existing_columns = columns_to_select.copy()

    def _display_error(
        self,
        stage: PipeValidationStage,
        missing_columns: set[str],
        pipe_name: str,
    ) -> None:

        # Construcción del mensaje a mostrar
        message_to_display = self._error_messages[stage].format(
            **{
                PIPE_VALIDATION_ERROR_MESSAGE_ARGS.MISSING_COLUMNS: list(missing_columns),
                PIPE_VALIDATION_ERROR_MESSAGE_ARGS.PIPE_NAME: pipe_name,
            }
        )

        # Se imprime el rastro de columnas
        print(self.traceback)
        # Se lanza el error
        raise AssertionError(message_to_display)

    def _record_current_state(
        self,
        pipe_name: str,
    ) -> None:

        # Inicialización de objeto de estado de columnas
        columns_state = self._State(pipe_name, frozenset( self.existing_columns ))
        # Se añade el estado al traceback
        self.traceback.add(columns_state)

    @dataclass
    class _State:
        pipe_name: str
        columns: frozenset[str]

        def __repr__(
            self,
        ) -> str:

            # Construcción de representación
            repr_ = '\n'.join([
                (f'Pipe: {self.pipe_name}'),
                (f'Columnas: {self.columns}'),
                ('=' * 30),
            ])

            return repr_

    @dataclass
    class _Traceback:
        states: list[ColumnsContext._State]

        def __repr__(
            self,
        ) -> str:
            
            # Construcción de representación
            repr_ = '\n\n'.join([str(state) for state in self.states])

            return repr_



        def add(
            self,
            new_state: ColumnsContext._State,
        ) -> None:

            # Se añade un nuevo estado a la lista de estados
            self.states.append(new_state)
