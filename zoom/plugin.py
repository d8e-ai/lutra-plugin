from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx

from lutraai.augmented_request_client import AsyncAugmentedTransport


@dataclass
class ZoomMeeting:
    uuid: str
    id: str
    created_at: datetime
    host_id: str
    start_time: datetime
    join_url: str
    topic: str
    agenda: Optional[str]
    duration_minutes: int
    meeting_type: int


def _to_zoom_meeting(meeting: dict[str, Any]) -> ZoomMeeting:
    return ZoomMeeting(
        uuid=meeting["uuid"],
        id=meeting["id"],
        created_at=datetime.fromisoformat(meeting["created_at"]),
        host_id=meeting["host_id"],
        start_time=(
            datetime.fromisoformat(meeting["start_time"]).astimezone(
                tz=ZoneInfo(meeting["timezone"])
            )
        ),
        join_url=meeting["join_url"],
        topic=meeting["topic"],
        agenda=meeting.get("agenda"),
        duration_minutes=meeting["duration"],
        meeting_type=meeting["type"],
    )


async def zoom_list_meetings() -> list[ZoomMeeting]:
    """List all Zoom meetings."""
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_zoom)
    ) as client:
        # TODO: Add pagination support.
        response = await client.get("https://api.zoom.us/v2/users/me/meetings")
        response.raise_for_status()
        await response.aread()
        data = response.json()
    return [_to_zoom_meeting(meeting) for meeting in data["meetings"]]


async def zoom_create_meeting(topic: str, start_time: datetime) -> ZoomMeeting:
    """Create a Zoom meeting and return it."""
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_zoom)
    ) as client:
        response = await client.post(
            "https://api.zoom.us/v2/users/me/meetings",
            json={"topic": topic, "start_time": start_time.isoformat()},
        )
        response.raise_for_status()
        await response.aread()
        data = response.json()
    return _to_zoom_meeting(data)
