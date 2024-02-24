"""Tests for the github plugin.

These tests hit a real GitHub repo, so they require an internet connection
and should not be run excessively to avoid rate limit issues.
"""

import types
import unittest
from unittest.mock import patch

import httpx
import plugin

# Create a module that has a symbol called authenticated_request_github
actions_v0 = types.ModuleType("actions_v0")
setattr(actions_v0, "authenticated_request_github", lambda: None)
plugin.__dict__["actions_v0"] = actions_v0


# Create a custom httpx.Client class that ignores the 'transport' argument.
class MyTestHttpxClient(httpx.Client):
    def __init__(self, *args, **kwargs):
        kwargs.pop("transport", None)
        super().__init__(*args, **kwargs)


class TestPlugin(unittest.TestCase):
    @patch("plugin.httpx.Client", new=MyTestHttpxClient)
    def _test_github_pulls(self):
        # Pull this repo's Pulls (including closed ones), sorted by creation date
        # to get the first Pull Request ever created for this repo.
        result = plugin.github_pulls(
            owner="d8e-ai",
            repo="lutra-plugin",
            state="all",
            sort="created",
            sort_direction="asc",
            page=1,
        )
        self.assertGreater(len(result), 0)
        self.assertEqual(
            result[0].html_url, "https://github.com/d8e-ai/lutra-plugin/pull/1"
        )
        self.assertEqual(result[0].title, "Rename to Lutra")

    @patch("plugin.httpx.Client", new=MyTestHttpxClient)
    def _test_github_issues(self):
        page = 1
        while True:
            result = plugin.github_issues(
                owner="d8e-ai",
                repo="lutra-plugin",
                state="all",
                sort="created",
                sort_direction="asc",
                page=page,
            )
            result = [x for x in result if x.issue_type == "issue"]
            if not result:
                page += 1
                continue

            self.assertGreater(len(result), 0)
            self.assertEqual(
                result[0].html_url, "https://github.com/d8e-ai/lutra-plugin/issues/45"
            )
            self.assertEqual(result[0].title, "Make plugins more testable")
            break

    @patch("plugin.httpx.Client", new=MyTestHttpxClient)
    def test_github_comment(self):
        result = plugin.github_comments(
            owner="d8e-ai",
            repo="lutra-plugin",
            issue_number=44,
        )
        print(result)
        self.assertGreater(len(result), 0)
        self.assertEqual(
            result[0].body,
            "@jcharum @vrv let me know if these addressed your concerns :)",
        )
