from dataclasses import dataclass
from datetime import datetime
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


@dataclass
class SlackMessage:
    type: str
    user: str
    text: str
    ts: datetime


def slack_conversations_history(
    channel: str, cursor: Optional[str] = None, limit: int = 100
) -> tuple[list[SlackMessage], str]:
    """
    Fetches a page of conversation history from a Slack channel.

    :param channel: The name of the Slack channel.
    :param cursor: Cursor for pagination.
    :param limit: Number of messages to fetch. Default is 100.
    :return: A tuple containing a list of SlackMessage dataclass instances and the next cursor.
    """
    params = {
        "channel": channel,
        "limit": limit,
    }
    if cursor:
        params["cursor"] = cursor
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack_as_user)
    ) as client:
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
            ts=datetime.fromtimestamp(float(msg["ts"])),
        )
        for msg in data.get("messages", [])
    ]
    next_cursor = data.get("response_metadata", {}).get("next_cursor")
    return messages, next_cursor
