from pathlib import Path

BASE_PATH = Path(__file__).resolve().parent
STAC_API_URL = "https://stac-api.coresight.app"
STAC_API_BEARER_TOKEN = "dFqHGruFe99A96bspLYCN8wCT63frd"
BUCKET_NAME = "ccore-holmes.appspot.com"
SHARED_VOLUME_PATH =  BASE_PATH / "gdal310_volume"
GDAL_CONTAINER_NAME = "gdal310_container"
CONTAINER_BASE_PATH = Path("/files")
DEFAULT_TIMEOUT = 300
MAKE_STAC_ITEM_TIMEOUT = 10
GCP_RETRIES = 3
POSSIBLE_TIMESTAMP_FIELD_PAIRS = [
    ("start_timestamp", "end_timestamp"),
    ("start_datetime", "end_datetime"),
    ("timestamp1", "timestamp2"),
    ("datetime1", "datetime2"),
    ("timestamp", "timestamp"),
    ("datetime", "datetime")
]
PRODUCT_TO_COLLECTION = {
    "spo": "fems-spo",
    "coherence": "fems-coherence",
    "divergence": "fems-divergence",
    "shear": "fems-shear",
    "strain": "fems-strain",
    "sea_ice_concentration": "fems-sea-ice-concentration",
    "ice_pressure": "fems-sea-ice-pressure",
    "floe_edge_polygons": "fems-floe-edge-area", 
    "ice_drift_ft": "fems-ice-tracking-ft", 
    "ice_drift_ft2": "fems-ice-tracking", 
    "ice_drift_pm": "fems-ice-tracking-pm", 
    "ice_drift_forecast": "fems-ice-drift-forecast",
}

COLLECTION_TO_LAYER_NAME = {
    "fems-sea-ice-concentration": "Ice Concentration",
    "fems-old-ice": "Old and Deformed Ice",
    "fems-icebergs": "Icebergs",
    "fems-ice-edge": "Sea Ice Edge",
    "fems-polynya": "Polynya",
    "fems-floe-edge-area": "Floe Edge Area",
    "fems-spo": "Land-Fast Ice Motion",
    "fems-coherence": "Tidal Crack",
    "fems-ice-tracking": "Ice Tracking",
    "fems-ice-tracking-ft": "FT Ice Motion",
    "fems-ice-tracking-pm": "PM Ice Motion",
    "fems-divergence": "Ice Divergence",
    "fems-shear": "Ice Shear",
    "fems-strain": "Ice Strain",    
    "fems-ice-drift-forecast": "Ice Drift Forecast",
    "fems-sea-ice-pressure": "Ice Pressure Forecast",
    "fems-sea-ice-thickness_forecast": "Ice Thickness Forecast",
    "fems-sea-ice-concentration_forecast": "Ice Concentration Forecast",
    "cis-ice-concentration": "CIS Ice Concentration",
    "cis-old-ice": "CIS Old Ice",
    "cis-sod": "CIS Stage of Development",
    "cis-fast-ice": "CIS Fast Ice",
    "cis-polaris": "CIS Polaris",
    "riops-sea-ice-concentration": "RIOPs Ice Concentration",
    "cmems-sea-ice-concentration": "CMEMS Ice Concentration",
    "riops-sea-ice-thickness": "RIOPs Ice Thickness",
    "cmems-sea-ice-thickness": "CMEMS Ice Thickness",
    "giops-sea-ice-pressure": "GIOPs Ice Pressure",
    "giops-sea-ice-strength": "GIOPs Ice Strength",
    "cmems-sea-water-salinity": "CMEMS Sea Water Salinity",
    "cmems-sea-water-temp": "CMEMS Sea Water Temperature",
    "cmems-sea-ice-age": "CMEMS Ice Age",
}

FEATURE_API_URL = "https://feature-api.coresight.app"
FEATURE_API_TOKEN = "dFqHGruFe99A96bspLYCN8wCT63frd"


