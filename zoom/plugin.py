from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo


from lutraai.dependencies import AuthenticatedAsyncClient
from lutraai.dependencies.authentication import (
    InternalAllowedURL,
    InternalOAuthSpec,
    InternalRefreshTokenConfig,
    InternalAuthenticatedClientConfig,
)


zoom_client = AuthenticatedAsyncClient(
    InternalAuthenticatedClientConfig(
        allowed_urls=(
            InternalAllowedURL(
                scheme=b"https",
                domain_suffix=b"api.zoom.us",
                add_auth=True,
            ),
        ),
        auth_spec=InternalOAuthSpec(
            auth_name="Zoom",
            auth_group="Zoom",
            auth_type="oauth2",
            access_token_url="https://zoom.us/oauth/token",
            authorize_url="https://zoom.us/oauth/authorize",
            api_base_url="https://api.zoom.us/v2",
            userinfo_endpoint="https://api.zoom.us/v2/users/me",
            scopes_spec={
                "meeting:read": "To view and list meetings.",
                "meeting:write": "To create and manage meetings.",
                "user:read": "To get Zoom IDs of users.",
            },
            scope_separator=",",
            jwks_uri="",
            prompt="consent",
            server_metadata_url="",
            access_type="offline",
            profile_id_field="id",
            logo="/assets/logos/zoom.svg",
            header_auth={
                "Authorization": "Bearer {api_key}",
            },
            refresh_token_config=InternalRefreshTokenConfig(
                auth_refresh_type="basic",
                header_fields={
                    "Host": "zoom.us",
                },
                body_fields={
                    "grant_type": "refresh_token",
                    "refresh_token": "{refresh_token}",
                },
            ),
        ),
    ),
    provider_id="b5e0d545-06d1-4551-9239-47aeb8b56bba",
)


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
        # TODO: Add pagination support.
    response = await zoom_client.get("https://api.zoom.us/v2/users/me/meetings")
    response.raise_for_status()
    await response.aread()
    data = response.json()
    return [_to_zoom_meeting(meeting) for meeting in data["meetings"]]


async def zoom_create_meeting(topic: str, start_time: datetime) -> ZoomMeeting:
    """Create a Zoom meeting and return it."""
    response = await zoom_client.post(
        "https://api.zoom.us/v2/users/me/meetings",
        json={"topic": topic, "start_time": start_time.isoformat()},
    )
    response.raise_for_status()
    await response.aread()
    data = response.json()
    return _to_zoom_meeting(data)
