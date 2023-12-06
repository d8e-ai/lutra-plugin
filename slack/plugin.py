from dataclasses import dataclass
from typing import Optional

import httpx

from lutraai.augmented_request_client import AugmentedTransport


_BOT_NAME = "Lutra Slack Bot"


def slack_send_message_to_channel(channel: str, message: str) -> None:
    """
    Send a message to a channel by channel name or ID.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json={
                    "channel": channel,
                    "text": message,
                },
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            if data.get("error", "") == "not_in_channel":
                raise RuntimeError(
                    f"bot is not in channel; please add `{_BOT_NAME}` "
                    f"by running `/invite @{_BOT_NAME}` in {channel}"
                )
            raise RuntimeError(f"sending message: {data}")


def slack_send_message_to_user(user: str, message: str) -> None:
    """
    Send a message to a user by user name or ID.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        user_list = (
            client.get(
                "https://slack.com/api/users.list",
            )
            .raise_for_status()
            .json()
        )
        channel_id = None
        found_members = []
        for member in user_list["members"]:
            found_members += [member["name"]]
            if member["name"] == user:
                channel_id = member["id"]
                break
        if channel_id is None:
            raise ValueError(f"could not find {user}, members: {found_members}")
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json={
                    "channel": channel_id,
                    "text": message,
                },
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            raise RuntimeError(f"sending message: {data}")


def _get_self_user_id() -> str:
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack_as_user)
    ) as client:
        data = client.get("https://slack.com/api/auth.test").raise_for_status().json()
        if not data.get("ok", False):
            raise RuntimeError(f"getting user ID: {data}")
        return data["user_id"]


def slack_send_message_to_self(message: str) -> None:
    """
    Send a message to my own user.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        user_id = _get_self_user_id()
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json={
                    "channel": user_id,
                    "text": message,
                },
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            raise RuntimeError(f"sending message: {data}")


def _find_channel_by_name(client: httpx.Client, channel_name: str) -> str | None:
    cursor = None
    while True:
        params = {}
        if cursor is not None:
            params["cursor"] = cursor
        data = (
            client.get(
                "https://slack.com/api/conversations.list",
                params=params,
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            raise RuntimeError(f"listing channels: {data}")
        for channel in data["channels"]:
            if (candidate_name := channel.get("name")) is None:
                continue
            if channel_name in {candidate_name, f"#{candidate_name}"}:
                return channel["id"]
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor in {None, ""}:
            return None


@dataclass
class SlackMessage:
    type: str
    user: str
    text: str
    ts: str


def slack_conversations_history(
    channel: str,
    oldest: Optional[str] = None,
    latest: Optional[str] = None,
    cursor: Optional[str] = None,
    limit: int = 100,
) -> tuple[list[SlackMessage], str]:
    """
    Fetches a page of conversation history from a Slack channel.

    :param channel: The name of the Slack channel.
    :param oldest: Only messages after this Unix timestamp will be included in results.
        Default is None, which means the beginning of time.
    :param latest: Only messages before this Unix timestamp will be included in results.
        Default is the None, which means the current time.
    :param cursor: Cursor for pagination.
    :param limit: The maximum number of items to return. May return fewer than the
        limit, even if there are more items.
    :return: A tuple containing a list of SlackMessage dataclass instances and the next
        cursor for pagination. If the next cursor is the empty string, all of the
        requested items have been returned.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack_as_user)
    ) as client:
        channel_id = _find_channel_by_name(client, channel)
        if channel_id is None:
            # `channel` does not match any name, so assume that it is an ID.
            channel_id = channel
        params = {
            "channel": channel_id,
            "limit": limit,
        }
        if oldest:
            params["oldest"] = oldest
        if latest:
            params["latest"] = latest
        if cursor:
            params["cursor"] = cursor
        data = (
            client.get(
                "https://slack.com/api/conversations.history",
                params=params,
            )
            .raise_for_status()
            .json()
        )
    if not data.get("ok", False):
        raise RuntimeError(f"fetching history: {data}")
    messages = [
        SlackMessage(
            type=msg["type"],
            user=msg.get("user"),
            text=msg.get("text"),
            ts=msg["ts"],
        )
        for msg in data.get("messages", [])
    ]
    next_cursor = data.get("response_metadata", {}).get("next_cursor", "")
    return messages, next_cursor
