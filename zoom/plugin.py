from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import httpx


from lutraai.augmented_request_client import AugmentedTransport


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


def zoom_list_meetings() -> list[ZoomMeeting]:
    """List all Zoom meetings."""
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_zoom)
    ) as client:
        # TODO: Add pagination support.
        data = (
            client.get("https://api.zoom.us/v2/users/me/meetings")
            .raise_for_status()
            .json()
        )
    return [_to_zoom_meeting(meeting) for meeting in data["meetings"]]


def zoom_create_meeting(topic: str, start_time: datetime) -> ZoomMeeting:
    """Create a Zoom meeting and return it."""
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
    return _to_zoom_meeting(data)
