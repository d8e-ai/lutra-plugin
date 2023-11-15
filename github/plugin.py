import httpx
from typing import Any

from lutraai.augmented_request_client import AugmentedTransport


def github_pulls(owner: str, repo: str) -> Any:
    """
    Returns the results of the GitHub `pulls` API call.

    Returns a listing of pull requests as documented at
    https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        return (
            client.get(f"https://api.github.com/repos/{owner}/{repo}/pulls")
            .raise_for_status()
            .json()
        )
