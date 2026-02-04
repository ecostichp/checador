from pathlib import Path

DROPBOX_PATH = 'Dropbox/La Casa Del Carpintero/Departamento de Programación/data_projects_git'
PROJECT_NAME = 'checador'

def path_from_dropbox(file_path: str) -> str:

    file_path = (
        Path
        .home()
        .joinpath(
            f'{DROPBOX_PATH}/data_{PROJECT_NAME}'
        )
        .joinpath(file_path)
        .__str__()
    )

    return file_path
