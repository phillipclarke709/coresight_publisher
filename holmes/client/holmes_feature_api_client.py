import json
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import Any

import httpx

from holmes.client.shared import resp_handler_wrapper
from holmes.settings import settings


@contextmanager
def client(
    api_key: str | None = settings.feature_api_bearer_token,
    firebase_token: str | None = None,
    timeout: (
        int | None
    ) = 180,  # TODO make less, atm feature api running on cloud run is VERY slow to start
) -> Generator[httpx.Client, None, None]:
    headers = {}

    if api_key:
        headers["X-Api-Key"] = api_key

    if firebase_token:
        headers["Authorization"] = f"Bearer {firebase_token}"

    with httpx.Client(headers=headers, timeout=timeout) as client:
        yield client


@resp_handler_wrapper
def create_items(client: httpx.Client, collection_id: str, items: str, url: str = settings.feature_api_url):
    return client.post(
        f"{url}/collections/{collection_id}/items",
        content=items,
        headers={"Content-Type": "application/json"},
    )


@resp_handler_wrapper
def update_item(client: httpx.Client, collection_id: str, item_id: str, item: str):
    return client.put(
        f"{settings.feature_api_url}/collections/{collection_id}/items/{item_id}",
        content=item,
        headers={"Content-Type": "application/json"},
    )


@resp_handler_wrapper
def update_items(client: httpx.Client, collection_id: str, items: str):
    return client.put(
        f"{settings.feature_api_url}/collections/{collection_id}/items",
        content=items,
        headers={"Content-Type": "application/json"},
    )


@resp_handler_wrapper
def delete_items(client: httpx.Client, collection_id: str, items: str, url = settings.feature_api_url):
    delete_url = f"{url}/collections/{collection_id}/items/delete"
    return client.request(
        method="DELETE",
        url=delete_url,
        json=items,
        headers={"Content-Type": "application/json"},
    )


@resp_handler_wrapper
def get_collections(client: httpx.Client):
    return client.get(f"{settings.feature_api_url}/collections")


@resp_handler_wrapper
def get_item(client: httpx.Client, collection_id: str, item_id: str):
    return client.get(
        f"{settings.feature_api_url}/collections/{collection_id}/items/{item_id}"
    )


@resp_handler_wrapper
def get_page_of_items_from_collection(
    client: httpx.Client,
    collection_id: str,
    *,
    bbox: list[float] | None = None,
    limit: int = 10,
    offset: int = 0,
    filter: dict[Any, Any] = {},
    other_params: dict[str, str | int | float | bool] = {},
    api_url: str = settings.feature_api_url,
):
    params = {
        "bbox": ",".join([str(coord) for coord in bbox]) if bbox else None,
        "limit": limit,
        "offset": offset,
        **other_params,
    }

    if filter:
        params["filter"] = json.dumps(filter)
        params["filter-lang"] = "cql2-json"

    if params["bbox"] is None:
        params.pop("bbox")

    return client.get(
        f"{api_url}/collections/{collection_id}/items",
        params=params,
    )


def get_items_from_collection(
    client: httpx.Client,
    collection_id: str,
    *,
    page_size: int = 10,
    offset: int = 0,
    bbox: list[float] | None = None,
    filter: dict[Any, Any] = {},
    other_params: dict[str, str | int | float | bool] = {},
    api_url: str = settings.feature_api_url,
) -> Iterator[Any]:  # TODO make other things account for this iterable change
    limit = page_size
    while True:
        resp = get_page_of_items_from_collection(
            client=client,
            collection_id=collection_id,
            bbox=bbox,
            limit=limit,
            offset=offset,
            filter=filter,
            other_params=other_params,
            api_url=api_url,
        )
        data = resp.json()

        yield data

        links = data.get("links", [])

        next_link = next((link for link in links if link["rel"] == "next"), None)

        if next_link:
            # should we make use of next_link, or just increment offset?
            # here, we're just incrementing offset
            # probably doesn't matter, just making a note
            offset += limit
        else:
            break


def get_all_collection_ids(client: httpx.Client):
    collections = get_collections(client).json()["collections"]
    return [collection["id"] for collection in collections]


if __name__ == "__main__":
    from holmes.features.poi_detector import (
        CreatePoiItems,
        create_test_platform_and_detection,
        platforms_full_table_name,
    )

    with client() as c:
        platform, detection_history = create_test_platform_and_detection()
        poi_items = CreatePoiItems(
            platforms_and_detections=[(platform, detection_history)]
        )
        res = create_items(c, platforms_full_table_name, poi_items.json())
