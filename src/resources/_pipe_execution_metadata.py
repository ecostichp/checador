from dataclasses import dataclass
import pandas as pd

class PipesExecutionMetadata:

    def __init__(
        self,
    ) -> None:

        # Inicialización de lista de metadatos
        self._io_metadata: list[PipesExecutionMetadata.InOut] = []

    def get_dataframe_metadata(
        self,
        df: pd.DataFrame,
    ) -> 'PipesExecutionMetadata.DataFrame':

        # Se obtienen los metadatos del DataFrame entrante
        df_metadata = self.DataFrame(df)

        return df_metadata

    def get_io_metadata(
        self,
        pipe_name: str,
        input_metadata: 'PipesExecutionMetadata.DataFrame',
        output_metadata: 'PipesExecutionMetadata.DataFrame',
    ) -> 'PipesExecutionMetadata.InOut':

        # Creación de objeto de metadatos de entrada y salida
        io_metadata = self.InOut(pipe_name, input_metadata, output_metadata)
        # Se añade éste a la lista de metadatos
        self._io_metadata.append(io_metadata)

        return io_metadata

    def __repr__(
        self,
    ) -> str:

        # Conversión de metadatos de entrada y salida a cadena de texto
        string_metadata = [str(io) for io in self._io_metadata]
        # Construcción de vista de metadatos en proceso
        metadata_process = '\n\n'.join(string_metadata)

        return metadata_process

    class DataFrame:

        def __init__(
            self,
            df: pd.DataFrame,
        ) -> None:

            # Asignación de atributos
            self.columns = list(df.columns)
            self.shape = df.shape
            self.dtypes: dict[str, str] = (
                df
                .dtypes
                .astype('string')
                .to_dict()
            )

        def __repr__(
            self,
        ) -> str:

            # Construcción de la representación de la instancia
            repr_ = f'col: {self.columns}, shape: {self.shape}, dtypes: {self.dtypes}'

            return repr_

    @dataclass(slots= True)
    class InOut:
        pipe_name: str
        input_metadata: 'PipesExecutionMetadata.DataFrame'
        output_metadata: 'PipesExecutionMetadata.DataFrame'

        def __repr__(
            self,
        ) -> str:

            # Construcción de representación
            repr_ = '\n'.join([
                (f'Pipe: {self.pipe_name}'),
                (f'Input: {self.input_metadata}'),
                (f'Output: {self.output_metadata}'),
                ('=' * 30),
            ])

            return repr_
