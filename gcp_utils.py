import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/cmorgan/.credentials/holmes-bucket-creds.json"
from pathlib import Path
import sys

from coresight_processingchain.sentinel_pairs.coresight_publisher.constants import DEFAULT_TIMEOUT, BUCKET_NAME
from cloudpathlib import CloudPath
from google.cloud import storage
from loguru import logger
from typing import Union, List, Dict

def upload_to_bucket(
    file: str | Path, collection_id: str, filename: str, timeout: int = DEFAULT_TIMEOUT
) -> str:
    """# Upload the file on disk to the Holmes GCP Bucket. 

    Args:
        file (str | Path): Path to file.
        collection_id (str): Name of collection to upload to.
        filename (str): Filename.
        timeout (int, optional): The amount of time before timeing out the connection to GCP bucket. 
        Uploading files to GCP takes time. Especially for large files. Defaults to 300.

    Returns:
        str: The url of the file on GCP.
    """
    logger.debug("Uploading to GCP Bucket")
    object_name =f"{collection_id}/{filename}"
    gs_uri = f"gs://{BUCKET_NAME}/{object_name}"
        
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    try:
        if isinstance(file, str) and "gs://" in file:
            data = CloudPath(file).read_bytes()
            blob.upload_from_string(data, timeout=timeout)
        else:
            blob.upload_from_filename(str(file), timeout=timeout)
    except Exception as e:
        logger.error(f"Failed to upload {file} to {gs_uri}: {e}")
        raise
        
    return gs_uri


def upload_single_pm_tiles_file( #Likely not needed - this is an alternative to upload_to_bucket but designed specifically for PMTiles files.
    pmtiles_file: Union[str, Path],
    collection_id: str,
    bbox: List[float],
    convex_hull: Dict,
    file_size: int
    ) -> Dict:

    """Upload a single PMTiles file to GCS and return asset info"""
    if isinstance(pmtiles_file, str):
        pmtiles_file = Path(pmtiles_file)
    filename = pmtiles_file.name
    try:
        object_name = f"{collection_id}/{filename}"
        gs_uri = f"gs://{BUCKET_NAME}/{object_name}"
        
        # Upload using cloudpathlib
        storage_client = storage.Client()
        gs_client = GSClient(storage_client=storage_client, timeout=300)
        with GSPath(gs_uri, client=gs_client).open("wb") as f:
            f.write(pmtiles_file.read_bytes())
        
        return {
            'success': True,
            'filename': filename,
            'set_id':pmtiles_file.stem,
            'bbox': bbox,
            'convex_hull': convex_hull,  # True convex hull geometry
            'gcs_path': gs_uri,
            'bucket_name': BUCKET_NAME,  # Required for signed URL generation
            'object_name': object_name,       # Required for signed URL generation
            'size': file_size
        }
    except Exception as e:
        return {'success': False, 'filename': filename, 'error': str(e)}


def does_item_exist_in_bucket(collection_id: str, filename: str) -> bool:
    """Verify if file is in GCP bucket.

    Args:
        collection_id (str): Name of collection.
        filename (str): Filename.

    Returns:
        bool: True or False if file exists on GCP bucket
    """
    object_name = f"{collection_id}/{filename}"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    exists = blob.exists()
    if exists:
        blob.reload()
        logger.debug(f"File: {object_name} exists in GCP bucket, size: {blob.size}")
    return exists

def remove_from_bucket(collection_id: str, filename: str) -> None:
    """Remove a file from the GCP bucket.

    Args:
        collection_id (str): Name of collection.
        filename (str): Filename.
    """
    object_name = f"{collection_id}/{filename}"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.delete()
    logger.debug(f"File: {object_name} removed from GCP bucket")

