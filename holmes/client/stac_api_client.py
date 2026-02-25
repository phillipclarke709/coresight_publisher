from collections.abc import Generator, Iterable
from contextlib import contextmanager

import backoff
import httpcore
import httpx
import pystac
from loguru import logger

from coresight_processingchain.sentinel_pairs.coresight_publisher.holmes.settings import settings

MAX_RETRIES = 10


http_errors = (
    httpx.HTTPStatusError,
    httpx.ReadTimeout,
    httpx.ConnectError,
    httpcore.RemoteProtocolError,
)


def handle_backoff(details):
    tries = details["tries"]
    wait = details["wait"]
    exception = details["exception"]
    logger.info(f"Backing off {wait:0.1f} seconds after {tries} tries.")
    logger.info(str(type(exception)), exception)


def does_backoff_giveup(e: Exception):
    if isinstance(e, httpx.HTTPStatusError):
        if e.response.status_code >= 400 < 500:
            # user error, don't retry
            return True

    return False


@contextmanager
def stac_api_client(
    bearer_token: str = settings.stac_api_bearer_token,
    timeout: int = 360,
) -> Generator[httpx.Client, None, None]:
    """Yields a STAC API client."""
    headers = {}

    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    with httpx.Client(headers=headers, timeout=timeout) as client:
        yield client


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def check_if_collection_exists(
    client: httpx.Client, collection_id: str, url: str = settings.stac_api_url
) -> bool:
    """Check if the STAC Collection exists."""
    try:
        logger.debug(f"Checking existence of collection: {collection_id}")
        resp = client.get(f"{url}/collections/{collection_id}")

        if resp.status_code == 200:
            logger.debug(f"Collection {collection_id} exists.")
            return True
        elif resp.status_code == 404:
            logger.debug(f"Collection {collection_id} does not exist.")
            return False
        else:
            logger.error(f"Unexpected status code {resp.status_code}: {resp.text}")
            resp.raise_for_status()

    except httpx.RequestError as e:
        logger.error(f"Failed to make the request: {e}")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e}")

    return False


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def upload_collection(
    client: httpx.Client,
    collection: pystac.Collection,
    url: str = settings.stac_api_url,
):
    """Uploads the STAC Collection."""
    logger.debug(f"Putting collection {collection.id}")
    collection.links = [link for link in collection.links if link.get_href() is not None]
    resp = client.put(f"{url}/collections/{collection.id}", json=collection.to_dict())
    try:
        if resp.status_code == 404:
            logger.debug("404, posting collection instead")
            resp = client.post(f"{url}/collections", json=collection.to_dict())
            resp.raise_for_status()
            logger.debug("Post successfull")
        else:
            resp.raise_for_status()
            logger.debug("Put successfull")
    except httpx.HTTPStatusError as e:
        logger.error(resp.text)
        raise e


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def upload_item(
    client: httpx.Client,
    collection_id: str,
    item: pystac.Item,
    url: str = settings.stac_api_url,
    transform_hrefs: bool = True,
):
    """
    Uploads a singular STAC Item.

    NOTE: If you are uploading multiple items, use `upload_items` instead.
    """
    logger.debug(f"Putting item {item.id} to collection {collection_id}")

    item.links = [link for link in item.links if link.get_href() is not None]
    resp = client.put(
        f"{url}/collections/{collection_id}/items/{item.id}",
        json=item.to_dict(transform_hrefs=transform_hrefs),
    )
    try:
        if resp.status_code == 404:
            logger.debug("404, posting item instead")
            resp = client.post(
                f"{url}/collections/{collection_id}/items",
                json=item.to_dict(transform_hrefs=transform_hrefs),
            )
            resp.raise_for_status()
            logger.debug("Post successfull")
        else:
            resp.raise_for_status()
            logger.debug("Put successfull")
    except httpx.HTTPStatusError as e:
        logger.error(resp.text)
        raise e


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def upload_items(
    client: httpx.Client,
    collection_id: str,
    items: list[pystac.Item],
    url: str = settings.stac_api_url,
    *,
    transform_hrefs: bool = False,
):
    """Uploads the STAC Items in bulk."""

    logger.debug(f"Bulk inserting {len(items)} items to collection {collection_id}")

    payload = {
        "items": {
            item.id: item.to_dict(transform_hrefs=transform_hrefs) for item in items
        },
        "method": "upsert",
    }

    resp = client.post(
        f"{url}/collections/{collection_id}/bulk_items",
        json=payload,
    )
    resp.raise_for_status()

    logger.debug("Bulk insertion successfull")


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def put_item(
    client: httpx.Client,
    collection_id: str,
    item: pystac.Item,
    url: str = settings.stac_api_url,
) -> None:
    """Puts the STAC Item."""
    logger.debug(f"Putting item {item.id} to collection {collection_id}")
    resp = client.put(
        f"{url}/collections/{collection_id}/items/{item.id}",
        json=item.to_dict(transform_hrefs=False),
    )
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(resp.text)
        raise e
    logger.debug("Put successfull")


@backoff.on_exception(
    backoff.expo,
    http_errors,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
    on_backoff=handle_backoff,
)
def read_item(
    client: httpx.Client,
    collection_id: str,
    item_id: str,
    url: str = settings.stac_api_url,
) -> pystac.Item | None:
    """Reads the STAC Item."""
    logger.debug(f"Getting item {item_id} from collection {collection_id}")
    resp = client.get(f"{url}/collections/{collection_id}/items/{item_id}")

    if resp.status_code == 404:
        return None

    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(resp.text)
        raise e
    logger.debug("Get successfull")
    return pystac.Item.from_dict(resp.json())


def check_if_item_exists(
    client: httpx.Client,
    collection_id: str,
    item_id: str,
    url: str = settings.stac_api_url,
) -> bool:
    """Check if the STAC Item exists."""
    try:
        logger.debug(f"Checking existence of item: {item_id}")
        resp = client.get(f"{url}/collections/{collection_id}/items/{item_id}")

        if resp.status_code == 200:
            logger.debug(f"Item {item_id} exists.")
            return True
        elif resp.status_code == 404:
            logger.debug(f"Item {item_id} does not exist.")
            return False
        else:
            logger.error(f"Unexpected status code {resp.status_code}: {resp.text}")
            resp.raise_for_status()

    except httpx.RequestError as e:
        logger.error(f"Failed to make the request: {e}")
        raise e
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise e

    raise Exception("Unexpected state reached")


@backoff.on_exception(
    backoff.expo,
    httpx.HTTPStatusError,
    max_tries=MAX_RETRIES,
    jitter=backoff.full_jitter,
    giveup=does_backoff_giveup,
)
def read_items(
    client: httpx.Client,
    collection_id: str,
    parameters: dict[str, str | list[float]] | None = None,
    url: str = settings.stac_api_url,
) -> Iterable[pystac.Item]:
    """Reads the STAC Items."""
    logger.debug(f"Getting items from collection {collection_id}")

    next_url = f"{url}/collections/{collection_id}/items"

    while True:
        resp = client.get(next_url, params=parameters)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(resp.text)
            raise e
        logger.debug("Get successfull")

        data = resp.json()

        items_in_json = resp.json()["features"]
        for item_in_json in items_in_json:
            yield pystac.Item.from_dict(item_in_json)

        links = data.get("links", [])
        next_link = next((link for link in links if link["rel"] == "next"), None)

        if next_link is None or next_link["href"] == next_url:
            break

        next_url = next_link["href"]


if __name__ == "__main__":
    with stac_api_client() as client:
        import typer

        app = typer.Typer()

        @app.command()
        def items(collection_id: str):
            for item in read_items(client, collection_id):
                print(item)

        app()
