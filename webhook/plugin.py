from typing import Any

import httpx


async def webhook_request(url: str, body: dict[str, Any]) -> Any:
    async with httpx.AsyncClient() as client:
        return await client.post(url, json=body).raise_for_status().json()
