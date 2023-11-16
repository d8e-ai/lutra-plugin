import httpx
from dataclasses import dataclass
from typing import Any, List, Optional

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class PullRequest:
    id: int
    html_url: str
    title: str
    state: str
    draft: bool
    body: Optional[str]
    user_id: int
    user_login: str


def github_pulls(
    owner: str, repo: str, state: str = "open", page: int = 1
) -> List[PullRequest]:
    """
    Returns a page of at most 30 results of the GitHub `pulls` API call.

    Returns a listing of pull requests in the given `repo` owned by `owner`.  Each page
    has at most 30 results.  To get more results, increment the `page` and call this
    function again.

    Parameters:
        state: may be one of {"open", "closed", "all"}.
        page: the page of results to return.

    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/pulls",
                params={"state": state, "page": page},
            )
            .raise_for_status()
            .json()
        )
    pull_requests = [
        PullRequest(
            id=obj["id"],
            html_url=obj["html_url"],
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
