from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional


from lutraai.decorator import purpose
from lutraai.dependencies import AuthenticatedAsyncClient
from lutraai.dependencies.authentication import (
    InternalAllowedURL,
    InternalOAuthSpec,
    InternalRefreshTokenConfig,
    InternalAuthenticatedClientConfig,
)

github_client = AuthenticatedAsyncClient(
    InternalAuthenticatedClientConfig(
        action_name="authenticated_request_github",
        allowed_urls=(
            InternalAllowedURL(
                scheme=b"https",
                domain_suffix=b"api.github.com",
                add_auth=True,
            ),
            InternalAllowedURL(
                scheme=b"https",
                domain_suffix=b"githubusercontent.com",
                add_auth=False,
            ),
        ),
        base_url=None,
        auth_spec=InternalOAuthSpec(
            auth_name="GitHub",
            auth_group="GitHub",
            auth_type="oauth2",
            access_token_url="https://github.com/login/oauth/access_token",
            authorize_url="https://github.com/login/oauth/authorize",
            api_base_url="https://api.github.com/",
            userinfo_endpoint="https://api.github.com/user",
            userinfo_force_token_type="Bearer",
            scopes_spec={
                "repo": "Full control of repositories.",
            },
            scope_separator=",",
            jwks_uri="",  # None available
            prompt="consent",
            server_metadata_url="",  # None available
            access_type="offline",
            profile_id_field="login",
            logo="https://storage.googleapis.com/lutra-2407-public/7a0dd11e373830a51a565de9fed4a985707c67ccd390f9ae4946a152303ea676.svg",
            header_auth={
                "Authorization": "Bearer {api_key}",
            },
            refresh_token_config=InternalRefreshTokenConfig(
                auth_refresh_type="form",
                body_fields={
                    "client_id": "{client_id}",
                    "client_secret": "{client_secret}",
                    "refresh_token": "{refresh_token}",
                    "grant_type": "refresh_token",
                },
            ),
        ),
    ),
    provider_id="7eb9377f-b770-4cdb-ba7b-31a459de57d3",
)


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


@purpose("Get pull requests.")
async def github_pulls(
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
    response = await github_client.get(
        f"https://api.github.com/repos/{owner}/{repo}/pulls",
        params={
            "state": state,
            "page": page,
            "sort": sort,
            "direction": sort_direction,
        },
    )
    response.raise_for_status()
    await response.aread()
    response_json = response.json()
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


@purpose("Get issues.")
async def github_issues(
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
    response = await github_client.get(
        f"https://api.github.com/repos/{owner}/{repo}/issues",
        params={
            "state": state,
            "page": page,
            "sort": sort,
            "direction": sort_direction,
        },
    )
    response.raise_for_status()
    await response.aread()
    response_json = response.json()
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
            assignee_id=(
                obj["assignee"]["id"] if obj.get("assignee") is not None else None
            ),
            assignee_login=(
                obj["assignee"]["login"] if obj.get("assignee") is not None else None
            ),
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


@purpose("Get comments.")
async def github_comments(
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
    response = await github_client.get(
        f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments",
        params={
            "page": page,
            "sort": sort,
            "direction": sort_direction,
        },
    )
    response.raise_for_status()
    await response.aread()
    response_json = response.json()
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
