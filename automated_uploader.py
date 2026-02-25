import os
import re
from datetime import datetime
import json
from osgeo import gdal
from coresight_processingchain.sentinel_pairs.coresight_publisher.holmes.client.stac_api_client import stac_api_client, check_if_item_exists
from coresight_processingchain.sentinel_pairs.coresight_publisher.publisher import publish_geotiff, publish_geojson_as_pmtiles
from coresight_processingchain.sentinel_pairs.coresight_publisher.constants import STAC_API_BEARER_TOKEN, STAC_API_URL, POSSIBLE_TIMESTAMP_FIELD_PAIRS
from coresight_processingchain.utils.tif_utils import landmask_tif

###USER DEFINED INPUT PARAMETERS###
dir_collection_pairs = [
    # ('/ice-mapnav/cmems/age', 'cmems-sea-ice-age'),
    # ('/ice-mapnav/cmems/concentration', 'cmems-sea-ice-concentration'),
    # ('/ice-mapnav/cmems/thickness', 'cmems-sea-ice-thickness'),
    # ('/ice-mapnav/cmems/salinity', 'cmems-sea-water-salinity'),
    # ('/ice-mapnav/cmems/temp', 'cmems-sea-water-temp'),
    # ('/ice-mapnav/riops/concentration', 'riops-sea-ice-concentration'),
    # ('/ice-mapnav/riops/thickness', 'riops-sea-ice-thickness'),
    # ('/ice-mapnav/giops/strength', 'giops-sea-ice-strength'),
    # ('/ice-mapnav/giops/pressure', 'giops-sea-ice-pressure'),
    # ('/ice-mapnav/cis/concentration/test_files', 'cis-ice-concentration'),
    # ('/ice-mapnav/cis/fast_ice/test_files', 'cis-fast-ice'),
    # ('/ice-mapnav/cis/old_ice/test_files', 'cis-old-ice'),
    # ('/ice-mapnav/cis/polaris/test_files', 'cis-polaris'),
    # ('/ice-mapnav/cis/stage_of_development/test_files', 'cis-sod'),
    # ('/ice-mapnav/forecasts/concentration', 'fems-sea-ice-concentration-forecast'),
    # ('/ice-mapnav/forecasts/thickness', 'fems-sea-ice-thickness-forecast'),
    # ('/ice-mapnav/forecasts/pressure', 'fems-sea-ice-pressure'),
    # ('/ice-mapnav/forecasts/drift', 'fems-ice-drift-forecast'),
    #('/output/deformation/divergence_upload_tests', 'fems-divergence'),
    #('/ice-mapnav/old_ice', 'fems-old-ice'),
    #('/output/ft2_to_upload', 'fems-ice-tracking'),
    #('/output/ft_to_upload', 'fems-ice-tracking-ft'),
    #('/output/pm_to_upload', 'fems-ice-tracking-pm'),
    ('/output/floe_edge_to_upload', 'fems-floe-edge-area'),
]
landmask_file = '/input_anc/ne_10m_ocean_valid.shp'
cutfile = '/input_anc/ne_10m_land.geojson'

DATETIME_PATTERNS = [
    (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "%Y-%m-%dT%H:%M:%S"),
    (r"\d{8}T\d{6}", "%Y%m%dT%H%M%S"),
    (r"\d{8}_\d{6}", "%Y%m%d_%H%M%S"),
    (r"\d{14}", "%Y%m%d%H%M%S"),
    (r"\d{4}-\d{2}-\d{2}_\d{6}", "%Y-%m-%d_%H%M%S"),
    (r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"),
    (r"\d{8}", "%Y%m%d"),
]

def mask_out_vectors_overlapping_cutfile(input_file, cut_file, output_file, boundary_polygon=None):
    #Read files
    vectors_gdf = gpd.read_file(input_file)
    num_vectors_before = len(vectors_gdf)
    if len(vectors_gdf) == 0:
        log.info(f"Input to mask_out_vectors_overlapping_cutfile is empty: {input_file}")
        vectors_gdf.to_file(output_file, driver="GeoJSON")
        return 
    cutfile_gdf = gpd.read_file(cut_file)
    if cutfile_gdf.crs != vectors_gdf.crs:
        cutfile_gdf = cutfile_gdf.to_crs(vectors_gdf.crs)

    #Clip the cutfile to the extent of the vectors to reduce processing time
    minx, miny, maxx, maxy = vectors_gdf.total_bounds
    clip_box = box(minx, miny, maxx, maxy)
    clipped_cutfile_gdf = gpd.clip(cutfile_gdf, clip_box)

    #Get vectors not on land and save to output_file
    vectors_overlapping_cutfile = gpd.sjoin(vectors_gdf, clipped_cutfile_gdf, predicate="intersects")
    vectors_not_overlapping_cutfile = vectors_gdf[~vectors_gdf.index.isin(vectors_overlapping_cutfile.index)]
    if boundary_polygon is not None: 
        vectors_not_overlapping_cutfile = vectors_not_overlapping_cutfile[vectors_not_overlapping_cutfile.within(boundary_polygon)] #Also ensure the vectors are fully within the provided boundary
    num_vectors_after = len(vectors_not_overlapping_cutfile)
    log.info(f"Removed {num_vectors_before - num_vectors_after} of {num_vectors_before} vectors overlapping cutfile: {input_file} -> {output_file}")
    vectors_not_overlapping_cutfile.to_file(output_file, driver="GeoJSON")

def extract_latest_datetime_from_filename(filename: str) -> datetime:
    found = []
    for regex, fmt in DATETIME_PATTERNS:
        for m in re.finditer(regex, filename):
            try:
                dt = datetime.strptime(m.group(), fmt)
                found.append(dt)
            except ValueError:
                pass
    if not found:
        raise ValueError(f"No valid datetime found in filename: {filename}")
    return max(found)

def ensure_timestamp_is_set(file: str) -> None:
    dt = extract_latest_datetime_from_filename(file)
    dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt is None:
        raise ValueError(f"No valid datetime found in filename: {file}")
    with open(file, 'r') as f:
        data = json.load(f)
    for feature in data['features']:
        if any(fields[0] in feature['properties'] for fields in POSSIBLE_TIMESTAMP_FIELD_PAIRS):
            continue
        else:
            feature['properties']['timestamp'] = dt
    with open(file, 'w') as f:
        json.dump(data, f)

for input_dir, collection_id in dir_collection_pairs:
    print(f"Processing {input_dir} for collection {collection_id}")
    print(f"Found {len(os.listdir(input_dir))} files")
    success_count = 0
    skipped_count = 0
    failed_count = 0
    for file in os.listdir(input_dir):
        if file.endswith('.tif'):
            #Check if item exists in STAC
            item_id = file.replace('.tif', '') if 'landmasked' in file.lower() else file.replace('.tif', '_landmasked')
            with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
                if check_if_item_exists(client, collection_id, item_id, url=STAC_API_URL):
                    print(f"Item {item_id} already exists in STAC. Skipping upload.")
                    skipped_count += 1
                    continue
            #Apply Landmask if required
            if 'landmasked' not in file.lower():
                landmasked_file = landmask_tif(os.path.join(input_dir, file), landmask_file=landmask_file)
                file = landmasked_file
            #Publish
            dt = extract_latest_datetime_from_filename(file)
            success = publish_geotiff(os.path.join(input_dir, file), collection_id, dt)
            if success:
                success_count += 1
            else:
                failed_count += 1
        elif file.endswith('.geojson'):
            #Check if item exists in STAC
            item_id = file.replace('.geojson', '')
            with stac_api_client(bearer_token=STAC_API_BEARER_TOKEN) as client:
                if check_if_item_exists(client, collection_id, item_id, url=STAC_API_URL):
                    print(f"Item {item_id} already exists in STAC. Skipping upload.")
                    skipped_count += 1
                    continue
            ensure_timestamp_is_set(os.path.join(input_dir, file))
            success = publish_geojson_as_pmtiles(os.path.join(input_dir, file), collection_id)
            if success:
                success_count += 1
            else:
                failed_count += 1
    print(f"Successfully published {success_count} files for collection {collection_id}")
    print(f"Skipped {skipped_count} files for collection {collection_id}")
    print(f"Failed to publish {failed_count} files for collection {collection_id}")
