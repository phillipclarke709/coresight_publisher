import json
from os import getenv
from typing import Any

from dotenv import load_dotenv
from pydantic.fields import FieldInfo
from pydantic_settings import PydanticBaseSettingsSource

load_dotenv()

PROJECT_ID = "ccore-holmes"
SECRET_ID = "prefect-holmes-settings"

flow_run_id = getenv("PREFECT__FLOW_RUN_ID")

# When within a Prefect flow, PREFECT__FLOW_RUN_ID is set
is_running_in_prefect_flow = flow_run_id is not None

is_running_in_local_prefect_flow = (
    is_running_in_prefect_flow and getenv("LOCAL_PREFECT") is not None
)

is_running_in_dalo = getenv("DALO_PREFECT") is not None

is_running_in_dalo_prefect_flow = is_running_in_prefect_flow and is_running_in_dalo

is_running_in_cloud_prefect_flow = is_running_in_prefect_flow and (
    not is_running_in_local_prefect_flow and not is_running_in_dalo_prefect_flow
)


# requires prefect decorator
def requiers_prefect(func):
    def wrapper(*args, **kwargs):
        if is_running_in_prefect_flow:
            return func(*args, **kwargs)

        raise Exception(
            "This function can only be run within the context of a Prefect flow"
        )

    return wrapper


def json_to_settings_dict(json_str: str) -> dict[str, Any]:
    """Convert a JSON string to a compatible settings dictionary."""
    settings_dict = json.loads(json_str)
    settings_dict = {k.lower(): v for k, v in settings_dict.items()}
    settings_dict = {k.replace("holmes_", ""): v for k, v in settings_dict.items()}
    return settings_dict


class PrefectGcpSettingsSource(PydanticBaseSettingsSource):
    """Load Prefect settings from GCP Secret Manager."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        # Nothing to do here. Only implement the return statement to make typing happy
        return None, "", False

    @requiers_prefect
    def __call__(self) -> dict[str, Any]:
        from holmes.gcp.secret import access_secret_version

        settings_secret = access_secret_version(
            project_id=PROJECT_ID,
            secret_id=SECRET_ID,
        )

        return json_to_settings_dict(settings_secret)


class PrefectBlockSettingsSource(PydanticBaseSettingsSource):
    """Load Prefect settings from a Prefect secret block."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        # Nothing to do here. Only implement the return statement to make typing happy
        return None, "", False

    @requiers_prefect
    def __call__(self) -> dict[str, Any]:
        from prefect.blocks.system import Secret

        settings_secret = Secret.load("holmes-settings").get()

        return json_to_settings_dict(settings_secret)
