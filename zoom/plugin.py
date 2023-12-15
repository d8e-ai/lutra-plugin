import httpx
from datetime import datetime
from typing import Any


from lutraai.augmented_request_client import AugmentedTransport


def zoom_list_meetings() -> dict[str, Any]:
    """List all Zoom meetings."""
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_zoom)
    ) as client:
        # TODO: Add pagination support.
        return (
            client.get("https://api.zoom.us/v2/users/me/meetings")
            .raise_for_status()
            .json()
        )


def zoom_create_meeting(topic: str, start_time: datetime) -> str:
    """Create a Zoom meeting and return the join URL."""
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_zoom)
    ) as client:
        data = (
            client.post(
                "https://api.zoom.us/v2/users/me/meetings",
                json={"topic": topic, "start_time": start_time.isoformat()},
            )
            .raise_for_status()
            .json()
        )
        if "join_url" not in data:
            raise ValueError("The 'join_url' is not present in the response data.")
        return data["join_url"]
