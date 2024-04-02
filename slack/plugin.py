from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import re

import httpx

from lutraai.augmented_request_client import AugmentedTransport


_BOT_NAME = "Lutra Slack Bot"


@dataclass
class _SlackUser:
    id: str
    display_name: str


def _with_mentions(users: list[_SlackUser], message: str) -> str:
    display_name_to_id = {}
    for user in users:
        if user.display_name not in display_name_to_id:
            display_name_to_id[user.display_name] = []
        display_name_to_id[user.display_name].append(user.id)

    def replace_with_id(match):
        display_name = match.group(1)
        if display_name in display_name_to_id:
            if len(display_name_to_id[display_name]) == 1:
                return f"<@{display_name_to_id[display_name][0]}>"
            else:
                # If more than one user has the same display name
                raise ValueError(
                    f"Ambiguous display name in mention: '{display_name}' is shared by multiple users."
                )
        else:
            return match.group(0)  # If no user found, return the original mention

    return re.sub(r"<@([^>]+)>", replace_with_id, message)


def slack_send_message_to_channel(
    channel: str, message: str, thread_ts: Optional[str]
) -> None:
    """
    Send a message to a channel by channel name or ID.

    The message may mention users by their display name by wrapping it in "<@" and ">".
    For example, to mention a user named "Alice", use "<@Alice>".

    Set thread_ts to the timestamp of a message to reply to that message's thread.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        users = _list_users(client)
        body = {
            "channel": channel,
            "text": _with_mentions(users, message),
        }
        if thread_ts is not None:
            body["thread_ts"] = thread_ts
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json=body,
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            if data.get("error", "") == "not_in_channel":
                raise RuntimeError(
                    f"bot is not in channel; please add `{_BOT_NAME}` "
                    f"by running `/invite @{_BOT_NAME}` in {channel}; "
                    "also double-check that you have authorized the correct workspace"
                )
            raise RuntimeError(f"sending message: {data}")


def slack_send_message_to_user(user_display_name: str, message: str) -> None:
    """
    Send a message to a user by user name or ID.

    The message may mention users by their display name by wrapping it in "<@" and ">".
    For example, to mention a user named "Alice", use "<@Alice>".
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        all_users = _list_users(client)
        users = [user for user in all_users if user.display_name == user_display_name]
        match len(users):
            case 0:
                raise ValueError(f"could not find {user_display_name}: {users}")
            case 1:
                channel_id = users[0].id
            case _:
                raise ValueError(
                    f"found multiple users named {user_display_name}: "
                    f"{[user.id for user in users]}"
                )
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json={
                    "channel": channel_id,
                    "text": _with_mentions(all_users, message),
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

    The message may mention users by their display name by wrapping it in "<@" and ">".
    For example, to mention a user named "Alice", use "<@Alice>".
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_slack)
    ) as client:
        user_id = _get_self_user_id()
        users = _list_users(client)
        data = (
            client.post(
                "https://slack.com/api/chat.postMessage",
                json={
                    "channel": user_id,
                    "text": _with_mentions(users, message),
                },
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            raise RuntimeError(f"sending message: {data}")


def _list_users(client: httpx.Client) -> list[_SlackUser]:
    users = []
    cursor = None
    while True:
        params = {}
        if cursor is not None:
            params["cursor"] = cursor
        data = (
            client.get(
                "https://slack.com/api/users.list",
                params=params,
            )
            .raise_for_status()
            .json()
        )
        if not data.get("ok", False):
            raise RuntimeError(f"listing users: {data}")
        for member in data["members"]:
            users.append(
                _SlackUser(
                    id=member["id"],
                    display_name=member["profile"]["display_name"],
                )
            )
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor in {None, ""}:
            return users


def _conversation_ids_by_name(client: httpx.Client) -> dict[str, str]:
    ids = {}
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
        ids.update(
            {
                channel["name"]: channel["id"]
                for channel in data["channels"]
                if "name" in channel
            }
        )
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor in {None, ""}:
            return ids


def _find_conversation_by_name(
    conversation_ids: dict[str, str], name: str
) -> str | None:
    canonical_name = name.lstrip("#")
    return conversation_ids.get(canonical_name)


@dataclass
class SlackMessage:
    type: str
    user: str
    text: str
    ts: str


def slack_conversations_history(
    channel: str,
    oldest: Optional[datetime] = None,
    latest: Optional[datetime] = None,
    cursor: Optional[str] = None,
    limit: int = 100,
) -> tuple[list[SlackMessage], str]:
    """
    Fetches a page of conversation history from a Slack channel.

    :param channel: The name or ID of the Slack channel.
    :param oldest: Only messages after this datetime will be included in results.
        Default is None, which means the beginning of time.
    :param latest: Only messages before this datetime will be included in results.
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
        conversation_ids = _conversation_ids_by_name(client)
        conversation_id = _find_conversation_by_name(conversation_ids, channel)
        if conversation_id is None:
            # `channel` does not match any name, so assume that it is an ID.
            conversation_id = channel
        params = {
            "channel": conversation_id,
            "limit": limit,
        }
        if oldest:
            params["oldest"] = str(oldest.timestamp())
        if latest:
            params["latest"] = str(latest.timestamp())
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
        if data.get("error") == "channel_not_found":
            # Avoid making the error message absurdly long.
            available_channels = (
                f"available channels: {sorted(list(conversation_ids.keys()))}; "
                if len(conversation_ids) < 256
                else ""
            )
            raise RuntimeError(
                f"channel `{channel}` not found; {available_channels}"
                "double-check that you have authorized the correct workspace"
            )
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
