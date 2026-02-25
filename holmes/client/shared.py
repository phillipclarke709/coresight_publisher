from collections.abc import Callable

import httpx
from loguru import logger


def resp_handler_wrapper(func: Callable[..., httpx.Response]):
    def resp_handler(*args, **kwargs):
        resp = None
        try:
            resp = func(*args, **kwargs)
            resp.raise_for_status()
        except httpx.HTTPStatusError as err:
            if resp is not None:
                logger.error(f"Error executing {func.__name__}: {resp.text}")
            raise err

        return resp

    return resp_handler
