import httpx

from lutraai.augmented_request_client import AugmentedTransport


_BOT_NAME = "Lutra Slack Bot"


def slack_send_message_to_channel(channel: str, message: str) -> None:
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
