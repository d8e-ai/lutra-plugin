from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

import httpx

from lutraai.augmented_request_client import AugmentedTransport


@dataclass
class GitHubPullRequest:
    id: int
    html_url: str
    created_at: datetime
    updated_at: datetime
    merged_at: Optional[datetime]
    closed_at: Optional[datetime]
    title: str
    state: str
    draft: bool
    body: str
    user_id: int
    user_login: str


def github_pulls(
    owner: str,
    repo: str,
    state: Literal["open", "closed", "all"] = "open",
    sort: Literal["created", "updated", "popularity", "long-running"] = "created",
    sort_direction: Literal["asc", "desc"] = "desc",
    page: int = 1,
) -> list[GitHubPullRequest]:
    """
    Returns results of a GitHub `pulls` API call.

    Returns a paginated listing of pull requests in the given `repo` owned by `owner`.
    Each page has at most 30 results.  To get more results, increment the `page` and
    call this function again.

    Parameters:
        owner: the owner of the repository.
        repo: the repository name.
        state: the state of the pull requests to fetch from GitHub.
        sort: by what to sort results.
        sort_direction: the direction of the sort.
        page: the page of results to return. Each page has at most 30 results.

    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github),
    ) as client:
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/pulls",
                params={
                    "state": state,
                    "page": page,
                    "sort": sort,
                    "direction": sort_direction,
                },
            )
            .raise_for_status()
            .json()
        )
    pull_requests = [
        GitHubPullRequest(
            id=obj["id"],
            html_url=obj["html_url"],
            created_at=datetime.fromisoformat(obj["created_at"]),
            updated_at=datetime.fromisoformat(obj["updated_at"]),
            merged_at=(
                datetime.fromisoformat(obj["merged_at"])
                if obj.get("merged_at")
                else None
            ),
            closed_at=(
                datetime.fromisoformat(obj["closed_at"])
                if obj.get("closed_at")
                else None
            ),
            title=obj["title"],
            state=obj["state"],
            draft=obj["draft"],
            body=obj.get("body") or "",
            user_id=obj["user"]["id"],
            user_login=obj["user"]["login"],
        )
        for obj in response_json
    ]
    return pull_requests


@dataclass
class GitHubIssue:
    id: int
    issue_number: int
    issue_type: Literal["issue", "pull_request"]
    html_url: str
    created_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime]
    title: str
    state: str
    body: str
    user_id: int
    user_login: str
    assignee_id: Optional[int]
    assignee_login: Optional[str]
    labels: list[str]


def github_issues(
    owner: str,
    repo: str,
    state: Literal["open", "closed", "all"] = "open",
    sort: Literal["created", "updated", "comments"] = "created",
    sort_direction: Literal["asc", "desc"] = "desc",
    page: int = 1,
) -> list[GitHubIssue]:
    """
    Returns results of a GitHub `issues` API call.

    Issues can be either issues or pull requests. This function returns both.

    Returns a paginated listing of issues in the given `repo` owned by `owner`.
    Each page has at most 30 results. To get more results, increment the `page` and
    call this function again.

    Parameters:
        owner: the owner of the repository.
        repo: the repository name.
        state: the state of the issues to fetch from GitHub.
        sort: by what to sort results.
        sort_direction: the direction of the sort.
        page: the page of results to return. Each page has at most 30 results.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/issues",
                params={
                    "state": state,
                    "page": page,
                    "sort": sort,
                    "direction": sort_direction,
                },
            )
            .raise_for_status()
            .json()
        )
    issues = [
        GitHubIssue(
            id=obj["id"],
            html_url=obj["html_url"],
            issue_number=obj["number"],
            issue_type="pull_request" if "pull_request" in obj else "issue",
            created_at=datetime.fromisoformat(obj["created_at"]),
            updated_at=datetime.fromisoformat(obj["updated_at"]),
            closed_at=(
                datetime.fromisoformat(obj["closed_at"])
                if obj.get("closed_at")
                else None
            ),
            title=obj["title"],
            state=obj["state"],
            body=obj.get("body") or "",
            user_id=obj["user"]["id"],
            user_login=obj["user"]["login"],
            assignee_id=obj["assignee"]["id"] if obj.get("assignee") is not None else None,
            assignee_login=obj["assignee"]["login"] if obj.get("assignee") is not None else None,
            labels=[
                label["name"] for label in obj.get("labels", [])
            ],  # Extracting label names
        )
        for obj in response_json
    ]
    return issues


@dataclass
class GitHubComment:
    id: int
    body: str
    user_id: int
    user_login: str
    created_at: datetime
    updated_at: datetime


def github_comments(
    owner: str,
    repo: str,
    issue_number: int,
    sort: Literal["created", "updated", "comments"] = "created",
    sort_direction: Literal["asc", "desc"] = "desc",
    page: int = 1,
) -> list[GitHubComment]:
    """
    Fetches comments for a specific issue by its number.

    Returns a paginated listing of comments in the given `repo` owned by `owner`
    for the issue with the given `issue_number`. Each page has at most 30 results.
    To get more results, increment the `page` and call this function again.

    Parameters:
        owner: the owner of the repository.
        repo: the repository name.
        sort: by what to sort results.
        sort_direction: the direction of the sort.
        page: the page of results to return. Each page has at most 30 results.
    """
    with httpx.Client(
        transport=AugmentedTransport(actions_v0.authenticated_request_github)
    ) as client:
        response_json = (
            client.get(
                f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments",
                params={
                    "page": page,
                    "sort": sort,
                    "direction": sort_direction,
                },
            )
            .raise_for_status()
            .json()
        )

    comments = [
        GitHubComment(
            id=comment["id"],
            body=comment["body"],
            user_id=comment["user"]["id"],
            user_login=comment["user"]["login"],
            created_at=datetime.fromisoformat(comment["created_at"]),
            updated_at=datetime.fromisoformat(comment["updated_at"]),
        )
        for comment in response_json
    ]

    return comments
