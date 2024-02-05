from typing import Any

import httpx


async def webhook_request(url: str, body: Any) -> Any:
    """
    Make a POST request to the given URL with the given body and returns the response.

    Both the request and response bodies are expected to be JSON and are converted to
    and from Python values using the json module.
    """
    async with httpx.AsyncClient() as client:
        return (await client.post(url, json=body)).raise_for_status().json()
