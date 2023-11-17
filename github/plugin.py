import httpx
from dataclasses import dataclass

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class PullRequest:
    id: int
    html_url: str
    title: str
    state: str
    draft: bool
    body: str
    user_id: int
    user_login: str


def github_pulls(
    owner: str,
    repo: str,
    state: str = "open",
    sort: str = "created",
    sort_direction: str | None = None,
    page: int = 1,
) -> list[PullRequest]:
    """
    Returns results of a GitHub `pulls` API call.

    Returns a paginated listing of pull requests in the given `repo` owned by `owner`.
    Each page has at most 30 results.  To get more results, increment the `page` and
    call this function again.

    Parameters:
        owner: the owner of the repository.
        repo: the repository name.
        state: may be one of {"open","closed","all"}.
        sort: by what to sort results.
            One of {"created","updated","popularity","long-running"}.
        sort_direction: the direction of the sort.  One of {"asc","desc"}.  If None,
            "desc" for sort by "created", "asc" otherwise.
        page: the page of results to return. Each page has at most 30 results.

    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        params = {"state": state, "page": page, "sort": sort}
        if sort_direction is not None:
            params["direction"] = sort_direction
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/pulls", params=params
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
            body=obj.get("body", ""),
            user_id=obj["user"]["id"],
            user_login=obj["user"]["login"],
        )
        for obj in response_json
    ]
    return pull_requests
