import backoff
import csv
import click
import geojson_pydantic
import geopandas as gpd
import os
import pystac
import random
import re
import rasterio
import rio_stac
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from cloudpathlib import CloudPath, GSPath
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from loguru import logger
from multiprocessing import Process
try:
    from osgeo import gdal
except ImportError:
    gdal = None
from pathlib import Path
from pydantic import BaseModel
from rasterio.errors import RasterioIOError
from shapely.geometry import mapping as shapely_geometry_mapping
from typing import Tuple, Generator, Union, List, Dict, Optional
from constants import (
    BASE_PATH, STAC_API_URL, STAC_API_BEARER_TOKEN, 
    BUCKET_NAME, SHARED_VOLUME_PATH, GDAL_CONTAINER_NAME, 
    CONTAINER_BASE_PATH, DEFAULT_TIMEOUT, MAKE_STAC_ITEM_TIMEOUT, 
    PRODUCT_TO_COLLECTION, FEATURE_API_TOKEN, FEATURE_API_URL, COLLECTION_TO_LAYER_NAME
)
from docker_utils import run_docker_command, clear_shared_docker_volume, copy_into_container
from gcp_utils import upload_to_bucket, does_item_exist_in_bucket, remove_from_bucket, list_bucket_asset_names
from holmes.client.holmes_feature_api_client import client, create_items, get_page_of_items_from_collection
from holmes.client.stac_api_client import (
    stac_api_client,
    upload_collection,
    upload_item,
    read_item,
    delete_item,
    check_if_item_exists,
    check_if_collection_exists
)
from utils import bbox_to_polygon, geojson_to_pmtiles

#os.environ["CPL_VSIL_CURL_NON_CACHED"] = "YES" # Supposedly this would fix a Rasterio error I was getting while creating a stac item.
os.environ["PROJ_LIB"] = str(Path(sys.prefix) / "lib" / "python3.10" / "site-packages" / "rasterio" / "proj_data" )

DELETED_PRODUCTS_CSV = BASE_PATH / "deleted_products_Hudson_Bay_2024.csv"


def configure_logging(verbose: bool) -> None:
    if not logger._core.handlers:
        logger.add(
            sys.stderr if verbose else sys.stdout, 
            level="DEBUG" if verbose else "INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )

def publish_geojson_as_pmtiles(
    geojson_path: Union[str, Path],
    collection_id: str,
    datetime: Optional[datetime] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    forecast_hour: Optional[int|str] = None,
    verbose: bool = False,
    clean: bool = True
) -> bool:
    """Publishes a geoJSON file as a PMTiles file to the Google Cloud Bucket and inserts it into the Holmes STAC.

    Args:
        geojson_path (str | Path): Path to geoJSON file to publish.
        collection_id (str): Collection name to publish to.
        datetime (datetime, optional): Datetime of the geoJSON file. Either this or start_datetime and end_datetime must be provided.
        start_datetime (datetime, optional): Start datetime of the geoJSON file. Either this and end_datetime or datetime must be provided.
        end_datetime (datetime, optional): End datetime of the geoJSON file. Either this and start_datetime or datetime must be provided.
        forecast_hour (int | str, optional): Forecast hour of the geoJSON file.
        verbose (bool, optional): Whether to print verbose output.
        clean (bool, optional): Whether to clean the PMTiles files after publishing.
    Returns:
        bool: True if successfully published, False otherwise
    """
    configure_logging(verbose)

    try:
        geojson_path = validate_path(geojson_path, extensions=['.geojson'])
        validate_collection_id_exists(collection_id)
    except ValueError as e:
        logger.error(e)
        raise

    #Don't need this check - geojson_to_pmtiles will extract the start and end datetimes from the geojson file if they exist
    # #Either end_datetime ot datetime must be set. start_datetime is optional
    # if end_datetime is not None:
    #     pass #Use end_datetime and start_datetime (if set) as the datetimes
    # elif datetime is not None:
    #     end_datetime = datetime #Use datetime and start_datetime (if set) as the datetimes
    # else:
    #     raise ValueError("Either datetime or start_datetime and end_datetime must be provided.")

    layer_name = COLLECTION_TO_LAYER_NAME[collection_id]
    pmtiles_dicts = geojson_to_pmtiles(geojson_path, layer_name=layer_name, start_datetime=start_datetime, end_datetime=end_datetime, clean=clean)
    success = True
    for pmtiles_dict in pmtiles_dicts:
        success = success and publish_pmtiles(pmtiles_dict['output'], collection_id, pmtiles_dict['bbox'], pmtiles_dict['convex_hull'], pmtiles_dict['start_datetime'], pmtiles_dict['end_datetime'], forecast_hour=forecast_hour)
        
    if clean:
        for pmtiles_dict in pmtiles_dicts:
            if os.path.exists(pmtiles_dict['output']):
                os.remove(pmtiles_dict['output'])

    return success

def publish_pmtiles(
    pmtiles_path: Union[str, Path],
    collection_id: str,
    bbox: List[float],
    convex_hull: Dict,
    start_datetime: datetime,
    end_datetime: datetime,
    forecast_hour: Optional[int|str] = None,
    verbose: bool = False
) -> bool:
    """Uploads a PMTiles file to the Google Cloud Bucket and inserts it into the Holmes STAC.

    Args:
        pmtiles_path (str | Path): Path to PMTiles file to publish.
        collection_id (str): Collection name to publish to.
        datetime (datetime, optional): Datetime of the PMTiles file.
        start_datetime (datetime, optional): Start datetime of the PMTiles file.
        end_datetime (datetime, optional): End datetime of the PMTiles file.
        forecast_hour (int | str, optional): Forecast hour of the PMTiles file.
        verbose (bool, optional): Whether to print verbose output.
    Returns:
        bool: True if successfully published, False otherwise
    """

    configure_logging(verbose)

    try:
        pmtiles_path = validate_path(pmtiles_path, extensions=['.pmtiles'])
        validate_collection_id_exists(collection_id)
    except ValueError as e:
        logger.error(e)
        raise

    if isinstance(pmtiles_path, str):
        pmtiles_path = Path(pmtiles_path)
    pmtiles_filename = pmtiles_path.name
    if does_item_exist_in_bucket(collection_id, pmtiles_filename):
        logger.info(f"File: {pmtiles_filename} already exists in bucket. Skipping upload.")
        return False
    
    href = upload_to_bucket(pmtiles_path, collection_id, pmtiles_filename)

    while not does_item_exist_in_bucket(collection_id, pmtiles_filename): #Wait to ensure the file is fully uploaded to the bucket before creating the STAC item.
        time.sleep(0.5)
    time.sleep(0.5)
    try:
        upload_pmtiles_stac_item(href, pmtiles_filename, collection_id, bbox=bbox, convex_hull=convex_hull, start_datetime=start_datetime, end_datetime=end_datetime, forecast_hour=forecast_hour)
        return True
    except Exception as e:
        logger.error(f"An error occurred creating the STAC item. {pmtiles_filename} will be removed from the bucket. {e}")
        remove_from_bucket(collection_id, pmtiles_filename)
        return False


def publish_geotiff(
    geotiff_path: Union[str, Path],
    collection_id: str,
    datetime: Optional[datetime] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    forecast_hour: Optional[int|str] = None,
    verbose: bool = False
) -> bool:
    """Uploads a geoTIFF file to the Google Cloud Bucket and inserts it into the Holmes STAC.

    Args:
        geotiff_path (str | Path): Path to geoTIFF file to publish. 
        collection_id (str): Collection name to publish to.
        datetime (datetime, optional): Datetime of the geoTIFF file. Either this or start_datetime and end_datetime must be provided.
        start_datetime (datetime, optional): Start datetime of the geoTIFF file. Either this and end_datetime or datetime must be provided.
        end_datetime (datetime, optional): End datetime of the geoTIFF file. Either this and start_datetime or datetime must be provided.
        forecast_hour (int | str, optional): Forecast hour of the geoTIFF file.
        verbose (bool, optional): Whether to print verbose output.
    Returns:
        bool: True if successfully published, False otherwise
    """
    configure_logging(verbose)
    
    #Either end_datetime ot datetime must be set. start_datetime is optional
    if end_datetime is not None:
        pass #Use end_datetime and start_datetime (if set) as the datetimes
    elif datetime is not None:
        end_datetime = datetime #Use datetime and start_datetime (if set) as the datetimes
    else:
        raise ValueError("Either datetime or start_datetime and end_datetime must be provided.")

    try:
        geotiff_path = validate_path(geotiff_path, extensions=['.tif', '.tiff'])    
        validate_collection_id_exists(collection_id)
    except ValueError as e:
        logger.error(e)
        raise

    cog_filename = geotiff_path.with_suffix(".cog.tif").name
    if does_item_exist_in_bucket(collection_id, cog_filename):
        logger.info(f"File: {cog_filename} already exists in bucket. Skipping upload.")
        return False
   
    cog_path = convert_tiff_to_cog(geotiff_path)
    cog_item_id = cog_path.name.split('.')[0]
    logger.debug(f"Conversion complete. COG path: {cog_path} and item id: {cog_item_id}")     
    
    href = upload_to_bucket(cog_path, collection_id, cog_filename)
    logger.debug(f"href = {href}") 
    logger.debug(does_item_exist_in_bucket(collection_id, cog_filename))
    
    while not does_item_exist_in_bucket(collection_id, cog_filename): #Wait to ensure the file is fully uploaded to the bucket before creating the STAC item.
        time.sleep(0.5)

    try:
        #upload_stac_item can generate intermittent errors when trying to generate multiple stac+items in a single process. So we use a child process to create and upload the stac item.
        p = Process(target=upload_cog_stac_item, args=(href, cog_item_id, cog_filename, collection_id, start_datetime, end_datetime, forecast_hour))
        p.start()
        p.join()
        return True
    except Exception as e:
        logger.error(f"An error occurred creating the STAC item. {cog_filename} will be removed from the bucket. {e}")
        remove_from_bucket(collection_id, cog_filename)
        return False


def remove_product(
    collection_id: str,
    asset_name: str,
    item_id: Optional[str] = None,
    verbose: bool = False,
    require_manual_confirmation: bool = True,
    clear_terminal: bool = True,
) -> bool:
    """Safely remove a published product from STAC and the GCP bucket.

    Deletion order is intentional:
    1. Validate STAC item + bucket asset existence.
    2. Back up the STAC item in memory.
    3. Delete STAC item first.
    4. Pause for manual confirmation.
    5. Delete bucket asset, or restore STAC item on cancellation.
    """
    configure_logging(verbose)
    validate_collection_id_exists(collection_id)

    target_item_id = item_id or infer_item_id_from_asset_name(asset_name)

    bucket_exists = does_item_exist_in_bucket(collection_id, asset_name)
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        stac_exists = check_if_item_exists(client, collection_id, target_item_id, url=STAC_API_URL)

    # Safety gate: we only perform deletion when both sides are present and aligned.
    if not stac_exists:
        logger.error(
            f"STAC item '{target_item_id}' was not found in collection '{collection_id}'. "
            "Deletion aborted to avoid removing the wrong asset."
        )
        return False
    if not bucket_exists:
        logger.error(
            f"Bucket asset '{asset_name}' was not found under collection '{collection_id}'. "
            "Deletion aborted to avoid STAC-only deletion."
        )
        return False

    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        # Keep a full in-memory copy so we can restore the item if manual verification fails.
        original_item = read_item(client, collection_id, target_item_id, url=STAC_API_URL)
        if original_item is None:
            logger.error(
                f"Failed to read STAC item '{target_item_id}' before delete; cannot guarantee rollback."
            )
            return False
        # Clone before editing so rollback uses a clean copy of the original item payload.
        item_backup = original_item.clone()
        # Let upload_item rebuild links on restore instead of reusing stale STAC links.
        item_backup.links = []

        # Delete STAC first so the product should disappear from the site before the asset is removed.
        delete_item(client, collection_id, target_item_id, url=STAC_API_URL)

    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        if check_if_item_exists(client, collection_id, target_item_id, url=STAC_API_URL):
            logger.error(f"STAC item '{target_item_id}' still exists after delete request; aborting.")
            return False

    if require_manual_confirmation:
        click.echo(
            "\nSTAC item deleted. Check Coresight now to verify the correct product disappeared."
        )
        # Human confirmation is the safeguard before the irreversible bucket delete.
        confirmed = click.confirm("Proceed with deleting the Google Cloud asset?", default=False)
        # Keep each confirmation cycle clean when operators process many deletions in sequence.
        if clear_terminal:
            click.clear()
        if not confirmed:
            # Roll back immediately if manual verification fails.
            with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
                upload_item(client, collection_id, item_backup, url=STAC_API_URL)
            logger.info("Deletion cancelled by user. STAC item has been restored.")
            if clear_terminal:
                click.clear()
            return False

    # Delete the actual file only after metadata removal was verified.
    remove_from_bucket(collection_id, asset_name)
    logger.info(f"Deleted bucket asset: {collection_id}/{asset_name}")

    deletion_record = {
        "collection_id": collection_id,
        "item_id": target_item_id,
        "asset_name": asset_name,
        "deleted_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    # Persist successful deletions so operators can review what changed across runs.
    append_deleted_product_to_csv(deletion_record)
    logger.info(f"Deletion complete for item '{target_item_id}'.")
    if clear_terminal:
        click.clear()
    return True


def remove_batch(
    collection_id: str,
    asset_pattern: str,
    verbose: bool = False,
) -> List[Dict[str, str]]:
    """Remove multiple products whose asset filenames match a user-provided regex.

    Purpose:
        Batch-delete products from one STAC collection when their asset names match
        a regex pattern, while reusing the existing single-product safety checks.

    How it works:
        1. Lists asset filenames from the collection's GCP bucket prefix.
        2. Applies the user-provided regex to those asset filenames.
        3. Prints the full matched list for operator visibility.
        4. Backs up every matching STAC item in memory before deleting anything.
        5. Deletes all matching STAC items so the batch disappears from the website.
        6. Pauses once for manual website verification.
        7. If confirmed, deletes all matching bucket assets and logs them to CSV.
        8. If cancelled, restores every STAC item from backup so the website view returns
           to its previous state.

    Input requirements:
        collection_id must be a valid STAC collection id.
        asset_pattern must be a valid Python regex, not a glob pattern.
        Example regex: r"20250701.*DT00.*\\.pmtiles$"
    """
    configure_logging(verbose)
    validate_collection_id_exists(collection_id)

    try:
        compiled_pattern = re.compile(asset_pattern)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern: {asset_pattern}. {e}") from e

    matches = find_matching_products(collection_id, compiled_pattern)
    if not matches:
        logger.info(f"No products matched pattern: {asset_pattern}")
        return []

    click.echo("\nProducts matched for deletion:")
    for match in matches:
        click.echo(f"- asset={match['asset_name']} item_id={match['item_id']}")

    stac_backups = []
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        for match in matches:
            asset_name = match["asset_name"]
            item_id = match["item_id"]

            # Safety gate: batch deletion requires both bucket asset and STAC item.
            if not does_item_exist_in_bucket(collection_id, asset_name):
                logger.error(
                    f"Bucket asset '{asset_name}' was not found under collection '{collection_id}'. "
                    "Batch deletion aborted before any STAC item was removed."
                )
                return []

            original_item = read_item(client, collection_id, item_id, url=STAC_API_URL)
            if original_item is None:
                logger.error(
                    f"STAC item '{item_id}' was not found in collection '{collection_id}'. "
                    "Batch deletion aborted before any STAC item was removed."
                )
                return []

            item_backup = original_item.clone()
            item_backup.links = []
            stac_backups.append(
                {
                    "match": match,
                    "item_backup": item_backup,
                }
            )

        deleted_stac_backups = []
        try:
            for backup in stac_backups:
                match = backup["match"]
                delete_item(client, collection_id, match["item_id"], url=STAC_API_URL)
                deleted_stac_backups.append(backup)
        except Exception:
            logger.exception("Batch STAC delete failed; restoring any STAC items already deleted.")
            for backup in deleted_stac_backups:
                upload_item(client, collection_id, backup["item_backup"], url=STAC_API_URL)
            return []

        for backup in stac_backups:
            match = backup["match"]
            if check_if_item_exists(client, collection_id, match["item_id"], url=STAC_API_URL):
                logger.error(
                    f"STAC item '{match['item_id']}' still exists after batch delete request; "
                    "restoring deleted STAC items and aborting."
                )
                for restore_backup in deleted_stac_backups:
                    upload_item(client, collection_id, restore_backup["item_backup"], url=STAC_API_URL)
                return []

    click.echo(
        "\nAll matching STAC items were deleted. Check Coresight now to verify the correct products disappeared."
    )
    confirmed = click.confirm(
        "Proceed with deleting the matching Google Cloud assets?",
        default=False,
    )
    if not confirmed:
        with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
            for backup in stac_backups:
                upload_item(client, collection_id, backup["item_backup"], url=STAC_API_URL)
        logger.info("Batch deletion cancelled by user. All STAC items have been restored.")
        return []

    deleted_products = []
    for backup in stac_backups:
        match = backup["match"]
        remove_from_bucket(collection_id, match["asset_name"])
        logger.info(f"Deleted bucket asset: {collection_id}/{match['asset_name']}")

        deletion_record = {
            "collection_id": collection_id,
            "item_id": match["item_id"],
            "asset_name": match["asset_name"],
            "deleted_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        append_deleted_product_to_csv(deletion_record)
        deleted_products.append(match)

    logger.info(f"Batch deletion complete. Deleted {len(deleted_products)} product(s).")
    return deleted_products


def find_matching_products(collection_id: str, compiled_pattern: re.Pattern) -> List[Dict[str, str]]:
    """Find bucket assets in a collection whose filenames match a regex."""
    matches = []
    for asset_name in list_bucket_asset_names(collection_id):
        if compiled_pattern.search(asset_name):
            matches.append(
                {
                    "item_id": infer_item_id_from_asset_name(asset_name),
                    "asset_name": asset_name,
                }
            )

    return matches


def infer_item_id_from_asset_name(asset_name: str) -> str:
    """Infer the usual STAC item id by removing all file extensions from an asset name."""
    inferred_item_id = asset_name
    while Path(inferred_item_id).suffix:
        inferred_item_id = Path(inferred_item_id).stem
    return inferred_item_id


def append_deleted_product_to_csv(deletion_record: Dict[str, str]) -> None:
    """Append a successful deletion record to a CSV log in the repo root."""
    file_exists = DELETED_PRODUCTS_CSV.exists()
    fieldnames = ["collection_id", "item_id", "asset_name", "deleted_at_utc"]

    with DELETED_PRODUCTS_CSV.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(deletion_record)


def validate_path(file_path: Union[str, Path], extensions: List[str]) -> Path:
    """Check if path passed is a valid path to a file.

    Args:
        file_path: (str | Path): Path to validate.

    Returns:
        Path: The validated path.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    if not file_path or str(file_path).strip() == "":
        raise ValueError("Path cannot be empty or ''.")

    if not file_path.exists():
        raise ValueError(f"The path: {file_path}, does not exist.")

    if not file_path.is_file():
        raise ValueError(f"The path: {file_path} needs to point to a file, not a directory.")

    if file_path.suffix.lower() not in extensions:
        raise ValueError(f"The file: {file_path}, is not a valid file (must have one of the extensions: {extensions})")

    return file_path
  
 
def validate_collection_id_exists(collection_id: str) -> None:
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        if not check_if_collection_exists(client, collection_id, url=STAC_API_URL):
            raise ValueError(f"A collection with the name: {collection_id}, does not exist.") 


def convert_tiff_to_cog(geotiff_path: Path) -> Path:
    """Converts a geoTIFF file to a Cloud Optimized GeoTIFF file.

    Args:
        geotiff_path (Path): Path to geoTIFF file to convert.

    Returns:
        Path: The output path to the Cloud Optimized GeoTIFF created.
    """
    clear_shared_docker_volume()
    logger.debug("Converting geoTIFF to COG")
    source_path = copy_into_container(geotiff_path)
    output_path = source_path.with_suffix(".cog.tif")
    
    fix_tifftag_datetime_to_iso(source_path) 
  
    source_path_container = CONTAINER_BASE_PATH / source_path.name 
    output_path_container = CONTAINER_BASE_PATH / output_path.name

    run_docker_command(
        [
            "gdal_translate",
            str(source_path_container),
            str(output_path_container),
            "-of","COG",
            "-co","COMPRESS=ZSTD",
            "-co","PREDICTOR=2",
            "-co","NUM_THREADS=ALL_CPUS",
            "-co","TILING_SCHEME=GoogleMapsCompatible",
            "-co","BIGTIFF=YES"
        ]
    ) 
    
    return output_path


def fix_tifftag_datetime_to_iso(geotiff_path: Path) -> None:
    """Makes ISO compliant the TIFFTAG_DATETIME in some geoTIFF files, if it exists.

    Args:
        geotiff_path (Path): Path to geoTIFF file.
    """
    geotiff_path_container = CONTAINER_BASE_PATH / geotiff_path.name
    tags = get_metadata_tags(geotiff_path)
    if 'TIFFTAG_DATETIME' in tags:
        logger.debug("Fixing TIFFTAG_DATETIME format")
        tiff_datetime = tags['TIFFTAG_DATETIME']
        iso_tiff_datetime = tiff_datetime.replace(':', '-', 2).replace(' ', 'T')
        run_docker_command(
            [
                "gdal_edit",
                "-mo", f"TIFFTAG_DATETIME={iso_tiff_datetime}",
                geotiff_path_container
            ]
        )
        

def get_metadata_tags(geotiff_path: Path) -> Dict[str, str]:
    """Gets the tags of a geoTIFF file. 

    Args:
        geotiff_path (Path): Path to geoTIFF file.

    Returns:
        dict: Tags of geoTIFF file.
    """
    with rasterio.open(geotiff_path, 'r') as src:
        tags = src.tags()
        return tags
   
def upload_pmtiles_stac_item(
    href: str, 
    pmtiles_file: Union[str, Path], 
    collection_id: str, 
    bbox: List[float],
    convex_hull: Dict,
    start_datetime: datetime,
    end_datetime: datetime,
    forecast_hour: Optional[int|str] = None
    ) -> None:
    if isinstance(pmtiles_file, str):
        pmtiles_file = Path(pmtiles_file)
    set_id = pmtiles_file.stem
    filename = pmtiles_file.name
    
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        if check_if_item_exists(client, collection_id, set_id, url=STAC_API_URL):
            logger.info(f"Item: {set_id} already exists in STAC. Skipping upload.")
            return

    # Get object info for signed URL generation
    object_name = f"{collection_id}/{filename}"
    
    if bbox is None:
        print(f"  Warning: No bbox for {filename}, using global extent")
        bbox = [-180, -90, 180, 90]
    
    # Use true convex hull geometry, fallback to bbox polygon if not available
    if convex_hull:
        hull_geometry = convex_hull
    else:
        hull_geometry = bbox_to_polygon(bbox)
    
    if isinstance(forecast_hour, int):
        forecast_hour = str(forecast_hour)

    # Create STAC item with convex hull geometry
    # Use end_datetime as primary datetime (for calendar queries)
    # Keep start_datetime for reference
    item = {
        "stac_version": "1.0.0",
        "type": "Feature",
        "id": set_id,
        "geometry": hull_geometry,  # Convex hull of all features
        "bbox": bbox,
        "properties": {
            "datetime": end_datetime.isoformat() + 'Z',  # Primary datetime (end) for calendar queries
            "start_datetime": start_datetime.isoformat() + 'Z' if start_datetime else None,  # Start time #Only include start and end datetime if both are present
            "end_datetime": end_datetime.isoformat() + 'Z' if start_datetime else None,  # End time #Only include start and end datetime if both are present
            "created": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "filename": filename,
            "forecast_hour": forecast_hour if forecast_hour else None
        },
        "assets": {
            "pmtiles": {
                "href": href,
                "type": "application/vnd.pmtiles",
                "title": "PMTiles Vector Tiles",
                "roles": ["data"],
                "bucket_name": BUCKET_NAME,  # Required for signed URL generation
                "object_name": object_name,  # Required for signed URL generation
            }
        },
        "links": [{"rel": "collection", "href": "./collection.json"}],
        "collection": collection_id
    }
    item = pystac.Item.from_dict(item)
    logger.info("Uploading item to STAC Catalogue")
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        upload_item(client, collection_id, item, url=STAC_API_URL)

def upload_cog_stac_item(href: str, cog_item_id: str, cog_filename: str, collection_id: str, start_datetime: datetime, end_datetime: datetime, forecast_hour: Optional[int|str] = None) -> None:
    """Adds relevant data about file to STAC.

    Args:
        href (str): Url of file in GCP bucket.
        cog_item_id (str): Unique identifier for file on STAC (recommend using filename without extension).
        cog_filename (str): Filename of Cloud Optimized GeoTIFF.
        collection_id (str): Collection name.
        forecast_hour (int | str, optional): Forecast hour of the geoTIFF file.
    """
    #@backoff.on_exception(backoff.expo, RasterioIOError, max_time=MAKE_STAC_ITEM_TIMEOUT, jitter=backoff.full_jitter)        
    def make_stac_item():
        logger.debug(f"Creating STAC Item: {href}")
        return rio_stac.create_stac_item(
            source=href, with_raster=True, id=cog_item_id, collection=collection_id
        )

    try:
        item = make_stac_item()
        if end_datetime is not None:
            item.datetime = end_datetime
    except RasterioIOError as e:
        logger.error(f"An error occurred while creating a stac item for the file: {cog_filename}. {e}")
        raise

    if start_datetime is not None:
        item.properties['start_datetime'] = start_datetime.isoformat() + 'Z'
        item.properties['end_datetime'] = end_datetime.isoformat() + 'Z'

    if forecast_hour is not None:
        if isinstance(forecast_hour, int):
            forecast_hour = str(forecast_hour)
        item.properties['forecast_hour'] = forecast_hour
    item = update_asset_metadata(item, cog_filename, collection_id)

    logger.debug("Uploading item to STAC Catalogue")
    with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
        upload_item(client, collection_id, item, url=STAC_API_URL)


def update_asset_metadata(
    item: pystac.item.Item, cog_filename: str, collection_id: str, bucket_name: str = BUCKET_NAME
) -> pystac.item.Item:
    """Places explicitly the bucket name / object name combo into STAC asset to ensure the Coresight frontend / backend can work with the asset 

    Args:
        item (pystac.item.Item): STAC item to update.
        cog_filename (str): Filename of Cloud Optimized GeoTIFF.
        collection_id (str): Collection name.
        bucket_name (str, optional): GCP bucket name. 

    Returns:
        pystac.item.Item: Updated STAC item.
    """
    asset = item.assets.get("asset")
    band_min, band_max = get_band_min_max(asset)
    colormap = "turbo"
        
    asset.extra_fields.update(
        {
            "object_name": f"{collection_id}/{cog_filename}",
            "bucket_name": bucket_name,
            "titiler_params": {
                "colormap_name": colormap,
                "rescale": f"{band_min},{band_max}",
                "nodata": 0
            },
        }
    )

    return item


def get_band_min_max(asset: pystac.Asset) -> Tuple[int, int]:
    """Gets the band min and max of a STAC item. Helper function for update_asset_metadata.

    Args:
        asset (pystac.Asset): Asset metadata.

    Returns:
        Tuple[int, int]: Band min and max.
    """
    raster_bands = asset.extra_fields.get("raster:bands")
    band_statistics = raster_bands[0].get("statistics")
    band_min = int(band_statistics.get("minimum"))
    band_max = int(band_statistics.get("maximum"))

    return band_min, band_max


def publish_geojson(file_path: Union[str, Path], collection_id: str) -> None:
    """Intended for floe edge polygons and ft, ft2, and pm ice drift.
    """
    try:
        file_path = validate_path(file_path, extensions=['.geojson'])    
    except ValueError as e:
        logger.error(e)
        raise


    if collection_id == "features.floe_edge_ice_tracking" or collection_id == "features.floe_edge_ice_tracking_ft" or collection_id == "features.floe_edge_ice_tracking_pm":
        last_id = fetch_highest_valid_id(collection_id, limit=1)
        payload_generator = build_ice_tracking_payload(file_path, last_id)

    elif collection_id == "features.floe_edge_polygons":
        payload_generator = build_floe_edge_payload(file_path)

    elif collection_id == "features.floe_edge_ice_tracking_forecast":
        last_id = fetch_highest_valid_id(collection_id, limit=1)
        payload_generator = build_ice_tracking_forecast_payload(file_path, last_id)

    else:
        raise ValueError("Collection ID does not exist.")

    
    with client(api_key=FEATURE_API_TOKEN) as c:
        for payload_chunk in payload_generator:
            res = create_items(c, collection_id, payload_chunk, FEATURE_API_URL)
            logger.info(res)
            logger.debug(res.json())


def fetch_highest_valid_id(collection_id: str, limit: int, skip_ids: List[int] = []) -> int:
    """
    Retrieves the highest valid ID from a collection, excluding null and skipped IDs.

    Args:
        collection_id (str): The collection to query.
        limit (int): Number of IDs to fetch for validation.
        skip_ids (List[int], optional): IDs to exclude from consideration. Defaults to an empty list.

    Returns:
        int: The highest valid ID in the collection.
    """
    with client(api_key=FEATURE_API_TOKEN) as c:
        last_n_items = get_page_of_items_from_collection(c, collection_id, limit=limit, offset=0, other_params={'sortby': '-id'}, api_url=FEATURE_API_URL).json()    
        last_n_ids = [item['properties']['id'] for item in last_n_items['features']]       

    valid_ids = [id for id in last_n_ids if id is not None] # Remove null ids
    skip_set = set(skip_ids)
    valid_ids = [id for id in valid_ids if id not in skip_set] # Remove skipped ids
     
    last_used_id = max(valid_ids) if valid_ids else 0 
    
    return last_used_id


def id_generator(starting_id: int, skip_ids: List[int] = []) -> Generator[int, None, None]:
    """
    Yields sequential IDs starting from starting_id, skipping those in skip_ids.
    More memory-efficient than pre-defined lists.
    NOTE: Id generation is needed for ice tracking, eventhough the coresight table could in theory auto generate ids
          however, it does not work. Floe edge polygons table got updated so there is no need to generate ids here anymore,
          they are autoincremented on the database level.
    """
    current_id = starting_id
    skip_set = set(skip_ids)
    while True:
        if current_id not in skip_set:
            yield current_id
        current_id += 1


class LineString(BaseModel):
    id: Optional[int] = None
    geometry: geojson_pydantic.LineString
    start_timestamp: datetime
    end_timestamp: datetime
    correlation_coefficient: float
    speed_ms: float
    displacement_m: float
    deltatime_hrs: float
    
    
class CreateLineStrings(BaseModel):
    ice_trackings: list[LineString]


def build_ice_tracking_payload(geojson_path: Union[str, Path], last_id: int, chunk_size: int = 50) -> Generator[str, None, None]:
    """
    Generates JSON payloads for ice tracking data with randomized IDs to avoid predictable increments.

    Args:
        geojson_path (Union[str, Path]): Path to the GeoJSON file.
        last_id (int): Starting ID for generation.
        chunk_size (int, optional): Number of entries per payload. Defaults to 50.

    Yields:
        str: JSON payload of ice tracking data.
    """
    id_gen = id_generator(last_id + random.randint(1, 20))
    gdf: gpd.GeoDataFrame = gpd.read_file(geojson_path)
    ice_trackings: list[LineString] = []
        
    for (i, row) in gdf.iterrows():
        attributes = row.index
        line_string = LineString(
            id = next(id_gen),
            geometry = shapely_geometry_mapping(row['geometry']),
            start_timestamp = row['timestamp1'],
            end_timestamp = row['timestamp2'],
            correlation_coefficient = row['r'] if 'r' in attributes else -2, #ice_drift_ft does not have a correlation coefficient. Set to -2 as a temporary placeholder. TODO: Modify ft table on Coresight to remove correlation coefficient column.
            speed_ms = row['speed_ms'],
            displacement_m = row['disp_m'],
            deltatime_hrs = row['deltatime_hrs']
        )
        ice_trackings.append(line_string)
    
        if (i + 1) % chunk_size == 0:
            yield CreateLineStrings(ice_trackings=ice_trackings).model_dump_json()
            ice_trackings = []

    if ice_trackings:
        yield CreateLineStrings(ice_trackings=ice_trackings).model_dump_json()  

def build_ice_tracking_forecast_payload(geojson_path: Union[str, Path], last_id: int, chunk_size: int = 50) -> Generator[str, None, None]:
    """
    Generates JSON payloads for ice tracking forecast data with randomized IDs to avoid predictable increments.

    Args:
        geojson_path (Union[str, Path]): Path to the GeoJSON file.
        last_id (int): Starting ID for generation.
        chunk_size (int, optional): Number of entries per payload. Defaults to 50.

    Yields:
        str: JSON payload of ice tracking data.
    """
    id_gen = id_generator(last_id + random.randint(1, 20))
    gdf: gpd.GeoDataFrame = gpd.read_file(geojson_path)
    ice_trackings: list[LineString] = []
        
    for (i, row) in gdf.iterrows():
        attributes = row.index
        line_string = LineString(
            id = next(id_gen),
            geometry = shapely_geometry_mapping(row['geometry']),
            start_timestamp = row['timestamp'],
            end_timestamp = row['timestamp'],
            correlation_coefficient = row['r'] if 'r' in attributes else -2, #ice_drift_ft does not have a correlation coefficient. Set to -2 as a temporary placeholder. TODO: Modify ft table on Coresight to remove correlation coefficient column.
            speed_ms = row['speed_m_s'],
            displacement_m = row['disp_m'] if 'disp_m' in attributes else -1,
            deltatime_hrs = row['delta_time_hrs'] if 'delta_time_hrs' in attributes else 0,
        )
        ice_trackings.append(line_string)
    
        if (i + 1) % chunk_size == 0:
            yield CreateLineStrings(ice_trackings=ice_trackings).model_dump_json()
            ice_trackings = []

    if ice_trackings:
        yield CreateLineStrings(ice_trackings=ice_trackings).model_dump_json()  

class FloeEdgePolygon(BaseModel):
    id: Optional[int] = None
    geometry: geojson_pydantic.Polygon
    start_timestamp: datetime
    end_timestamp: datetime


class CreateFloeEdgePolygons(BaseModel):
    floe_edge_polygons: list[FloeEdgePolygon]


def build_floe_edge_payload(geojson_path: Union[str, Path], chunk_size: int = 50) -> Generator[str, None, None]:
    """
    Generates JSON payloads for floe edge polygons, skipping large test IDs to minimize wasted space.

    Args:
        geojson_path (Union[str, Path]): Path to the GeoJSON file.
        chunk_size (int, optional): Number of entries per payload. Defaults to 50.

    Yields:
        str: JSON payload of floe edge polygon data.
    """
    gdf: gpd.GeoDataFrame = gpd.read_file(geojson_path)
    floe_edge_polygons: list[FloeEdgePolygon] = []
    
    for (i, row) in gdf.iterrows(): 
        floe_edge_polygon = FloeEdgePolygon(
            geometry = shapely_geometry_mapping(row['geometry']),
            start_timestamp = row['timestamp1'],
            end_timestamp = row['timestamp2']
        )
        floe_edge_polygons.append(floe_edge_polygon)
    
        if (i + 1) % chunk_size == 0:
            yield CreateFloeEdgePolygons(floe_edge_polygons=floe_edge_polygons).model_dump_json()
            floe_edge_polygons = []

    if floe_edge_polygons:
        yield CreateFloeEdgePolygons(floe_edge_polygons=floe_edge_polygons).model_dump_json() 
