import json

from dotenv import load_dotenv
from loguru import logger
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from holmes.settings.prefect import (
    PrefectBlockSettingsSource,
    PrefectGcpSettingsSource,
    is_running_in_cloud_prefect_flow,
    is_running_in_dalo_prefect_flow,
    is_running_in_local_prefect_flow,
    is_running_in_prefect_flow,
)

load_dotenv()


class Settings(BaseSettings):
    development: bool = False

    project_id: str = "ccore-holmes"

    # NOTE this isn't using pydantics PostgresDsn, due to the production
    # connection string lacking a hostname due to cloudsql weirdness
    pg_dsn: str = "postgresql://username:password@localhost:5432/holmes"

    use_minio: bool = True
    minio_endpoint: str = "localhost"
    minio_port: int = 9000
    minio_use_ssl: bool = False
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"

    use_local_auth: bool = False

    local_auth_jwt_secret: str = (
        "local-jwt-secret-that-is-hopefully-more-than-32-characters-thanks-hasura"
    )

    service_account: str | None = None

    postmark_api_key: str | None = None

    gom_api_key: str = "gom-platforms-api-key"
    api_key: str = "holmes-api-key"

    stac_api_url: str = "http://localhost:8050"
    # prod -> https://stac-api.coresight.app
    stac_api_bearer_token: str = "cheeto-auth-token"

    feature_api_url: str = "http://localhost:8060"
    # prod -> https://feature-api.coresight.app
    feature_api_bearer_token: str = "cheeto-auth-token"

    api_url: str = "http://localhost:8040"
    api_key: str = "holmes-api-key"

    api_hasura_secret: str = "hasura-secret"

    compare_iceberg_management_account_name: str | None = None
    compare_iceberg_management_connection_host: str | None = None
    compare_iceberg_management_connection_port: int = 5800
    compare_iceberg_management_key_location: str | None = None
    compare_iceberg_management_key_password: str | None = None

    eodag_cloud_storage: str | None = None
    eodag_provider_username: str | None = None
    eodag_provider_password: str | None = None

    # TODO don't leave this in git, change the underlying key once we do
    pc_sdk_subscription_key: str = "c0095acdea3f479fbd881430c1c5c530"

    sentinel_hub_client_id: str | None = None
    sentinel_hub_client_secret: str | None = None
    sentinel_hub_catalog_api_url: str = (
        "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
    )
    copernicus_token_url: str = (
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    )
    copernicus_s3_access_key: str | None = None
    copernicus_s3_secret_key: str | None = None

    usgs_username: str | None = None
    usgs_password: str | None = None

    opendata_aws_access_key: str | None = None
    opendata_aws_secret_key: str | None = None

    eodms_username: str | None = None
    eodms_password: str | None = None

    visual_crossing_api_key: str | None = None

    # this likely aren't needed anymore, slated for removal
    spire_faroe_eez_token: str | None = None
    spire_greenland_eez_token: str | None = None
    spire_messages_token: str | None = None

    ottawa_aws_pg_dsn: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="HOLMES_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        default_sources = (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

        if is_running_in_cloud_prefect_flow:
            return (*default_sources, PrefectGcpSettingsSource(settings_cls))

        if is_running_in_local_prefect_flow or is_running_in_dalo_prefect_flow:
            return (*default_sources, PrefectBlockSettingsSource(settings_cls))

        return default_sources


settings = Settings()

# Local auth should only be done when we're running the app, well, locally
if settings.use_local_auth and settings.use_minio is False:
    raise ValueError("Local auth requires Minio to be enabled")

if (
    settings.use_local_auth
    and is_running_in_prefect_flow
    and not is_running_in_local_prefect_flow
):
    raise ValueError("Local auth cannot be used in a non-local Prefect flow")

gs_creds = None

if settings.service_account is not None:
    logger.info("Loading GCP service account from settings")
    import google.oauth2.service_account

    service_account_dict = json.loads(settings.service_account)
    gs_creds = google.oauth2.service_account.Credentials.from_service_account_info(
        service_account_dict
    )
elif is_running_in_cloud_prefect_flow:
    # If running in Prefect flow, get GCP service account from Prefect
    from prefect_gcp import GcpCredentials

    try:
        gcp_credentials_block = GcpCredentials.load("c-core-labs")
        gs_creds = gcp_credentials_block.get_credentials_from_service_account()
    except Exception as e:
        logger.warning("Failed to load GCP credentials from Prefect Blocks: {e}", e=e)


def no_op():
    pass


def settings_as_env_dict():
    settings_dict = settings.model_dump()

    env_dict = {}
    for key, value in settings_dict.items():
        env_dict[f"HOLMES_{key.upper()}"] = value

    return env_dict


if __name__ == "__main__":
    import typer

    app = typer.Typer()

    @app.command()
    def settings_as_json():
        print(settings.model_dump_json(indent=4))

    @app.command()
    def settings_as_env_json():
        env_dict = settings_as_env_dict()
        print(json.dumps(env_dict, indent=4))

    app()
