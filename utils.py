import json
import os
import re
import shutil
import subprocess

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path
from shapely.geometry import mapping, shape
from shapely.ops import unary_union
try:
    from tqdm.notebook import tqdm
except ImportError:
    def tqdm(iterable=None, *args, **kwargs):
        return iterable
from typing import Optional, List, Dict, Union

from constants import POSSIBLE_TIMESTAMP_FIELD_PAIRS

def configure_logging(verbose: bool) -> None:
    if not logger._core.handlers:
        logger.add(
            sys.stderr if verbose else sys.stdout, 
            level="DEBUG" if verbose else "INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

def bbox_to_polygon(bbox):
    """Convert [minx, miny, maxx, maxy] to GeoJSON Polygon geometry."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [bbox[0], bbox[1]],
            [bbox[2], bbox[1]],
            [bbox[2], bbox[3]],
            [bbox[0], bbox[3]],
            [bbox[0], bbox[1]]
        ]]
    }
    
def calculate_bbox_and_convex_hull(features_list):
    """Calculate bounding box and convex hull from a list of GeoJSON features.
    
    Returns:
        tuple: (bbox, convex_hull_geojson)
        - bbox: [min_lon, min_lat, max_lon, max_lat]
        - convex_hull_geojson: GeoJSON geometry dict of the convex hull
    """
    geometries = []
    
    for feature in features_list:
        geom_dict = feature.get('geometry')
        if geom_dict:
            try:
                geom = shape(geom_dict)
                if geom.is_valid and not geom.is_empty:
                    geometries.append(geom)
            except Exception:
                pass
    
    if not geometries:
        return None, None
    
    # Union all geometries and compute convex hull
    combined = unary_union(geometries)
    convex_hull = combined.convex_hull
    
    # Get bbox from convex hull bounds
    bounds = convex_hull.bounds  # (minx, miny, maxx, maxy)
    bbox = [bounds[0], bounds[1], bounds[2], bounds[3]]
    
    # Convert convex hull to GeoJSON
    convex_hull_geojson = mapping(convex_hull)
    
    return bbox, convex_hull_geojson

def parse_datetime(dt_value):
    """Parse datetime from string or return as-is if already datetime"""
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        return dt_value
    if isinstance(dt_value, str):
        # Try common formats
        dt_value = re.sub(r'(\+00)+$', '', dt_value) #Hotfix for some incorrect datetime strings that end in +00
        formats = [
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y%m%dT%H%M%S",
            "%Y%m%d_%H%M%S",
            "%Y%m%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(dt_value, fmt)
            except ValueError:
                continue
        raise ValueError(f"Could not parse datetime: {dt_value}")
    return dt_value

def generate_set_id(dt):
    """Generate consistent set ID from a datetime"""
    return dt.strftime('%Y%m%d_%H%M%S')

def convert_to_pmtiles(
    geojson_path, 
    output_dir: Optional[Path] = None,
    drop_densest_as_needed: Optional[bool] = True,
    extend_zooms_if_still_dropping: Optional[bool] = True,
    minimum_zoom: Optional[int] = 0,
    maximum_zoom: Optional[int] = 14,
    layer_name: Optional[str] = None):
    """Convert a single GeoJSON file to PMTiles"""
    filename = geojson_path.stem
    if output_dir:
        output_file = output_dir / f"{filename}.pmtiles"
    else:
        output_file = geojson_path.parent / f"{filename}.pmtiles"
    
    # Build tippecanoe command
    cmd = [
        'tippecanoe',
        '-o', str(output_file),
        f'--minimum-zoom={minimum_zoom}',
        f'--maximum-zoom={maximum_zoom}',
        f'--layer={layer_name}',
        '--force',  # Overwrite existing
    ]
    
    if drop_densest_as_needed:
        cmd.append('--drop-densest-as-needed')
    if extend_zooms_if_still_dropping:
        cmd.append('--extend-zooms-if-still-dropping')
    
    cmd.append(str(geojson_path))
    
    # Run tippecanoe
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return {
            'success': True,
            'filename': filename,
            'output': output_file,
            'size': output_file.stat().st_size if output_file.exists() else 0
        }
    except subprocess.CalledProcessError as e:
        return {
            'success': False,
            'filename': filename,
            'error': e.stderr
        }

def convert_single(
    item,
    layer_name: Optional[str] = None,
    minimum_zoom: Optional[int] = 0,
    maximum_zoom: Optional[int] = 14,
    drop_densest_as_needed: Optional[bool] = True,
    extend_zooms_if_still_dropping: Optional[bool] = True,
    ) -> Dict:
    """Convert a single GeoJSON file and return result with bbox and convex hull."""
    result = convert_to_pmtiles(item['file'], layer_name=layer_name, minimum_zoom=minimum_zoom, maximum_zoom=maximum_zoom, drop_densest_as_needed=drop_densest_as_needed, extend_zooms_if_still_dropping=extend_zooms_if_still_dropping)
    result['bbox'] = item.get('bbox')
    result['convex_hull'] = item.get('convex_hull')
    result['start_datetime'] = item.get('start_datetime')
    result['end_datetime'] = item.get('end_datetime')
    return result

def geojson_to_pmtiles(
    geojson_path: Union[str, Path],
    layer_name: Optional[str] = None,
    minimum_zoom: Optional[int] = 0,
    maximum_zoom: Optional[int] = 14,
    drop_densest_as_needed: Optional[bool] = True,
    extend_zooms_if_still_dropping: Optional[bool] = True,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    verbose: Optional[bool] = False,
    clean: Optional[bool] = False
    ) -> List[Dict]:
    """Converts a GeoJSON file to a PMTiles file.

    Args:
        geojson_path (Union[str, Path]): Path to the GeoJSON file.
        layer_name (Optional[str]): Name of the layer to create in the PMTiles file.

    Returns:
        List[Dict]: List of dictionaries containing the result of the conversion of the structure: 
        {'success': bool, 
        'filename': str, 
        'output': Path, 
        'size': int,
        'bbox': List[float],
        'convex_hull': Dict,
        'start_datetime': datetime,
        'end_datetime': datetime
        }
        - success: True if the conversion was successful, False otherwise.
        - filename: Name of the PMTiles file.
        - output: Path to the PMTiles file.
        - size: Size of the PMTiles file.
        - bbox: Bounding box of the GeoJSON file.
        - convex_hull: Convex hull of the GeoJSON file.
        - start_datetime: Start datetime of the GeoJSON file.
        - end_datetime: End datetime of the GeoJSON file.
    """

    configure_logging(verbose)

    #Load geojson file
    if isinstance(geojson_path, str):
        geojson_path = Path(geojson_path)
    
    with open(geojson_path) as f:
        geojson = json.load(f)
    
    working_dir = geojson_path.parent

    # Validate geojson structure
    if geojson.get('type') != 'FeatureCollection':
        raise ValueError("GeoJSON must be a FeatureCollection")

    logger.info(f"Loaded GeoJSON: {geojson_path}")
    logger.info(f"  Type: {geojson['type']}")

    #Group features by datetime
    logger.info(f"Grouping features by datetime...")

    features = geojson.get('features', [])
    datetime_groups = defaultdict(list)
    datetime_ranges = {}  # Store start/end datetimes per set_id for STAC items
    skipped_features = 0

    for feature in features:
        properties = feature.get('properties', {})
        
        # Get end datetime (primary grouping field)
        if end_datetime: #Manually set in args
            end_ts = end_datetime
            start_ts = start_datetime if start_datetime else None
        else:
            for timestamp_field_pair in POSSIBLE_TIMESTAMP_FIELD_PAIRS:
                if timestamp_field_pair[0] in properties and timestamp_field_pair[1] in properties:
                    start_ts = properties[timestamp_field_pair[0]]
                    end_ts = properties[timestamp_field_pair[1]]
                    if start_ts == end_ts: #If only one datetime field is set, set the other to None
                        start_ts = None
                    break
            else:
                raise ValueError(f"Timestamp fields not found in feature properties")
        
        # Use end_datetime for grouping; fall back to start if end is missing
        group_ts = end_ts or start_ts
        
        if group_ts is None:
            skipped_features += 1
            continue
        
        try:
            group_dt = parse_datetime(group_ts)
            set_id = f"{geojson_path.stem}_{generate_set_id(group_dt)}"
            
            # Parse start/end datetimes
            start_dt = parse_datetime(start_ts) if start_ts else None
            end_dt = parse_datetime(end_ts) if end_ts else group_dt
            
            # Track datetime range per set_id (min start, max end)
            if set_id not in datetime_ranges:
                datetime_ranges[set_id] = {'start': start_dt, 'end': end_dt}
            else:
                if start_dt and datetime_ranges[set_id]['start']: #Do not attempt to compare None values
                    datetime_ranges[set_id]['start'] = min(datetime_ranges[set_id]['start'], start_dt)
                datetime_ranges[set_id]['end'] = max(datetime_ranges[set_id]['end'], end_dt)
            
            # Add set_id to feature properties
            feature['properties']['set_id'] = set_id
            datetime_groups[set_id].append(feature)
        except Exception as e:
            logger.warning(f"Warning: Could not process feature: {e}")
            skipped_features += 1

    logger.info("\nGrouping complete")
    logger.info(f"  Unique datetime sets: {len(datetime_groups)}")
    logger.info(f"  Total features grouped: {sum(len(g) for g in datetime_groups.values()):,}")
    if skipped_features > 0:
        logger.info(f"  Skipped features (missing datetime): {skipped_features}")

    #Export individual GeoJSON files per datetime set
    logger.info(f"Exporting individual GeoJSON files per datetime set...")
    exported_geojson = []
    total_geojson_size = 0

    for set_id, features_list in sorted(datetime_groups.items()):
        output_file = working_dir / f"{set_id}.geojson"
        
        # Calculate bbox and convex hull from features
        bbox, convex_hull = calculate_bbox_and_convex_hull(features_list)
        
        # Create FeatureCollection
        feature_collection = {
            "type": "FeatureCollection",
            "features": features_list
        }
        
        # Write to file
        with open(output_file, 'w') as f:
            json.dump(feature_collection, f)
        
        file_size = output_file.stat().st_size
        total_geojson_size += file_size
        
        exported_geojson.append({
            'set_id': set_id,
            'file': output_file,
            'features': len(features_list),
            'size': file_size,
            'start_datetime': datetime_ranges[set_id]['start'],
            'end_datetime': datetime_ranges[set_id]['end'],
            'bbox': bbox,
            'convex_hull': convex_hull  # Store actual convex hull geometry
        })

    logger.info(f"\nExported {len(exported_geojson)} GeoJSON files")

    #Convert GeoJSONs to PMTiles
    NUM_WORKERS = min(os.cpu_count() or 4, 8)
    logger.info(f"Converting GeoJSON to PMTiles using {NUM_WORKERS} parallel workers...")

    pmtiles_results = []
    failed = []

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(
            convert_single,
            item,
            layer_name=layer_name,
            minimum_zoom=minimum_zoom,
            maximum_zoom=maximum_zoom,
            drop_densest_as_needed=drop_densest_as_needed,
            extend_zooms_if_still_dropping=extend_zooms_if_still_dropping
            ): item for item in exported_geojson}
        
        for future in as_completed(futures):
            result = future.result()
            pmtiles_results.append(result)
            if not result['success']:
                failed.append(result)

    successful = [r for r in pmtiles_results if r['success']]
    total_pmtiles_size = sum(r['size'] for r in successful)

    logger.info("\nConversion complete")
    logger.info(f"  Successful: {len(successful)}")
    logger.info(f"  Failed: {len(failed)}")

    if failed:
        logger.info("\nFailed conversions:")
        for f in failed:
            logger.error(f"  - {f['filename']}: {f['error'][:100]}...")

    if clean:
        for geojson_dict in exported_geojson:
            if os.path.exists(geojson_dict['file']):
                os.remove(geojson_dict['file'])

    logger.debug("=" * 60)
    logger.debug("Conversion Complete!")
    logger.debug("=" * 60)
    logger.debug("\nInput:")
    logger.debug(f"  File: {geojson_path.name}")
    logger.debug(f"  Features: {len(features):,}")
    logger.debug("\nOutput:")
    logger.debug(f"  Datetime groups: {len(datetime_groups)}")
    logger.debug(f"  GeoJSON files: {working_dir}")
    logger.debug(f"  PMTiles files: {working_dir}")
    logger.debug("=" * 60)

    return successful
