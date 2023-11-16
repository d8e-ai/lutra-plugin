import httpx
from dataclasses import dataclass
from typing import Any, List, Optional

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class PullRequest:
    id: int
    url: str
    title: str
    state: str
    draft: bool
    body: Optional[str]
    user_id: int
    user_login: str


def github_pulls(owner: str, repo: str) -> List[PullRequest]:
    """
    Returns the results of the GitHub `pulls` API call.

    Returns a listing of pull requests as documented at
    https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/pulls",
                params={"state", "all"},
            )
            .raise_for_status()
            .json()
        )
    pull_requests = [
        PullRequest(
            id=obj["id"],
            url=obj["url"],
            title=obj["title"],
            state=obj["state"],
            draft=obj["draft"],
            body=obj.get("body"),
            user_id=obj["user"]["id"],
            user_login=obj["user"]["login"],
        )
        for obj in response_json
    ]
    return pull_requests
