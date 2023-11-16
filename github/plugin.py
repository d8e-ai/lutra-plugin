import httpx
from dataclasses import dataclass
from typing import Any, List

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class PullRequest:
    title: str
    url: str


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
            client.get(f"https://api.github.com/repos/{owner}/{repo}/pulls")
            .raise_for_status()
            .json()
        )
    pull_requests = [
        PullRequest(title=obj["title"], url=obj["url"]) for obj in response_json
    ]
    return pull_requests
