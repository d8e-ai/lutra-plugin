from datetime import datetime, timezone
from dataclasses import dataclass
import httpx
from typing import  List, Literal

from lutraai.augmented_request_client import AsyncAugmentedTransport
from lutraai.decorator import purpose
from lutraai.requests import raise_error_text

@dataclass
class RedditId:
    type: Literal["Comment", "Account", "Link", "Message", "Subreddit", "Award"]
    id: str


@dataclass
class RedditPost:
    post_id: RedditId
    title: str
    author_id: RedditId
    text: str
    created: datetime


def _decode_post(child_data):
    if child_data.get("kind") != "t3":
        return None
    post_data = child_data.get("data", {})
    return RedditPost(
        post_id=RedditId(type="Link", id=post_data.get("name")),
        title=post_data.get("title"),
        author_id=RedditId(type="Account", id=post_data.get("author_fullname")),
        text=post_data.get("selftext"),
        created=datetime.fromtimestamp(post_data.get("created_utc"), tz=timezone.utc),
    )

@dataclass
class RedditComment:
    comment_id: RedditId
    comment: str
    author_id: RedditId
    created: datetime
    reply_ids: List[RedditId]

def _decode_comment(child_data):
    if child_data.get("kind") != "t1":
        return None
    comment_data = child_data.get("data", {})
    
    # Handling reply ids 
    replies = comment_data.get("replies") or {}
    
    replies_children = replies.get("data", {}).get("children", [])
    replies_children_id = []
    for replies_child in replies_children:
        replies_child_data = replies_child.get("data")
        replies_children_id.append(replies_child_data.get("name"))

    return RedditComment(
        comment_id=RedditId(type="Comment", id=comment_data.get("name")),
        comment=comment_data.get("body"),
        author_id=RedditId(type="Account", id=comment_data.get("author_fullname")),
        created=datetime.fromtimestamp(comment_data.get("created_utc"), tz=timezone.utc),
        reply_ids=[RedditId(type="Comment", id=reply_child_id) for reply_child_id in replies_children_id],
    )
    
def _get_id36(id: RedditId):
    return id.id.split("_")[1]

@purpose("Fetch subreddit posts")
async def reddit_fetch_subreddit_posts(
    subreddit: str,
    max_results: int = 10,
) -> List[RedditPost]:
    '''Fetches a list of posts from a subreddit
    '''
    url = f"https://oauth.reddit.com/r/{subreddit}.json"
    headers = {
        'User-Agent': 'Lutra (dev)/0.1 by Primary-Review69'
    }
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_reddit),
    ) as client:
        params = {}
        reddit_posts = []
        while True:
            response = await client.get(url, headers=headers, params=params)
            await raise_error_text(response)
            await response.aread()
            data = response.json().get("data", {})
            if children := data.get("children"):
                for child in children:
                    reddit_post = _decode_post(child)
                    
                    if reddit_post:
                        reddit_posts.append(reddit_post)

            if len(reddit_posts) >= max_results:
                break

            if after := data.get("after"):
                params["after"] = after
            else:
                break
    return reddit_posts[:max_results]


@purpose("Get comments from a reddit post")
async def reddit_get_post_comments(
    subreddit: str,
    post_id: RedditId,
    max_comments: int = 10,
) -> List[RedditComment]:
    '''Get comments from a reddit post
    '''
    url = f"https://oauth.reddit.com/r/{subreddit}/comments/article"
    headers = {
        'User-Agent': 'Lutra (dev)/0.1 by Primary-Review69'
    }
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_reddit),
    ) as client:
        params = {
            "article": _get_id36(post_id),
            "limit": max_comments,
        }
        comments = []
        
        response = await client.get(url, headers=headers, params=params)
        await raise_error_text(response)
        await response.aread()
        comments_data = response.json()
        for comment_data in comments_data:
            data = comment_data.get("data", {})
            if children := data.get("children"):
                for child in children:
                    comment = _decode_comment(child)
                    if comment:
                        comments.append(comment)
                
    return comments


@purpose("Add a comment to reply to a post or another comment")
async def reddit_add_comment(
    target_id: RedditId,
    text: str,
):
    '''Add a comment to reply to a post or another comment
    '''
    if target_id.type not in ["Comment", "Link"]:
        raise ValueError("Invalid target_id type")
    
    url = f"https://oauth.reddit.com/api/comment"
    headers = {
        'User-Agent': 'Lutra (dev)/0.1 by Primary-Review69',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    async with httpx.AsyncClient(
        transport=AsyncAugmentedTransport(actions_v0.authenticated_request_reddit),
    ) as client:
        data = {
            "api_type": "json",
            "text": text,
            "thing_id": target_id.id,
            "return_rtjson": "true"
        }
        response = await client.post(url, headers=headers, data=data)
        await raise_error_text(response)
