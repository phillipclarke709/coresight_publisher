from pathlib import Path
import os
try:
    import docker
except ImportError:
    docker = None

from loguru import logger
import shutil

from constants import SHARED_VOLUME_PATH, GDAL_CONTAINER_NAME


def run_docker_command(args: list) -> None:
    #TODO: Could use client.containers.run instead of exec_run, then the container doesn't have to be running all the time (see feature_tracking_ft2.py for an example)
    if docker is None:
        raise ImportError("The 'docker' Python package is required for raster conversion commands.")
    client = docker.from_env()
    container = client.containers.get(GDAL_CONTAINER_NAME)
    exit_code, output = container.exec_run(args)
    if exit_code != 0:
        raise RuntimeError(f"Command failed with exit code {exit_code}:\n{output.decode()}")
    else:
        logger.info(output.decode())


def clear_shared_docker_volume() -> None:
    """Clears all the contents inside the shared volume with the gdal container.
    """
    logger.debug("Clearing container shared volume")
    if not Path(SHARED_VOLUME_PATH).exists():
        raise FileNotFoundError("Shared volume: gdal310_volume/ does not exist")

    for filename in os.listdir(SHARED_VOLUME_PATH):
        file_path = os.path.join(SHARED_VOLUME_PATH, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            logger.error(f"Failed to delete {file_path}. The reason: {e}")
            raise


def copy_into_container(source_path: Path, shared_volume_path: Path = SHARED_VOLUME_PATH) -> Path:
    """Copies a file into the shared container volume. Meant to copy files into gdal docker container.

    Args:
        source_path (Path): Path to file to copy.
        shared_volume_path (Path, optional): Path to shared container volume. Defaults to SHARED_VOLUME_PATH.

    Returns:
        Path: Returns path to file in container volume
    """
    logger.debug("Copying file into container")
    destination_path = shared_volume_path / source_path.name
    try:
        shutil.copy(source_path, destination_path)
    except PermissionError as e:
        logger.error(f"Failed to copy due to permission error: {e}. If cause is gdal shared volume, please make sure ownership is set correctly.")
        raise
    except IOError as e:
        logger.error(f"Failed to copy {source_path} to {destination_path}")
        raise
    
    return destination_path

