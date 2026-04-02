"""
Wikipedia Plugin.

Plugin for browsing and querying Wikipedia content.
Supports category pages and article history pages — the two page types
that carry evaluatable ground-truth data for our templates.
"""

from typing import Any, Dict, List
from urllib.parse import parse_qs, unquote, urlparse

from liveweb_arena.plugins.base import BasePlugin

from .api_client import fetch_article_history_api_data, fetch_category_api_data

# Namespaces whose history pages carry no evaluation-relevant data.
_SKIP_NS_PREFIXES = (
    "Talk:", "User:", "User talk:", "Wikipedia:", "Wikipedia talk:",
    "Template:", "Template talk:", "Help:", "Help talk:",
    "Category talk:", "Portal:", "Portal talk:",
    "Special:", "File:", "File talk:",
)


class WikipediaPlugin(BasePlugin):
    """
    Wikipedia plugin for category count and article revision-count queries.

    Two URL types produce API data used for ground-truth collection:

    1. Category pages  (/wiki/Category:<Name>)
       → article_count and subcategory_count via categoryinfo prop.
       The page shows "The following X pages are in this category" — same number.

    2. Article history pages (/wiki/<Title>?action=history)
       → revision timestamps for the past 35 days (up to 500 revisions).
       The page lists timestamped edits the agent can count directly.

    All other Wikipedia pages (regular articles, Talk, Special, etc.) are
    visited for navigation but do not trigger API data fetches.
    """

    name = "wikipedia"

    allowed_domains = [
        "en.wikipedia.org",
        "wikipedia.org",
    ]

    def get_blocked_patterns(self) -> List[str]:
        """Block direct API endpoints to force agents to use the web interface."""
        return [
            "*wikipedia.org/w/api.php*",            # MediaWiki Action API
            "*wikimedia.org/api/rest_v1/metrics*",  # Pageviews REST API
        ]

    def _parse_url(self, url: str):
        """
        Parse a Wikipedia URL into (page_name, action).

        Returns (None, None) for non-Wikipedia or non-/wiki/ URLs.
        ``page_name`` has underscores converted to spaces and is URL-decoded.
        """
        parsed = urlparse(url)
        host = parsed.netloc
        if host not in ("en.wikipedia.org", "wikipedia.org"):
            return None, None
        if not parsed.path.startswith("/wiki/"):
            return None, None
        page_name = unquote(parsed.path[len("/wiki/"):]).replace("_", " ")
        action = parse_qs(parsed.query).get("action", ["view"])[0]
        return page_name, action

    def needs_api_data(self, url: str) -> bool:
        """
        Return True only for page types that contribute GT data:
        - Category pages
        - Article history pages (action=history)

        All other pages (articles, Talk, Special, etc.) are navigation-only.
        """
        page_name, action = self._parse_url(url)
        if page_name is None:
            return False

        if action == "history":
            return not page_name.startswith(_SKIP_NS_PREFIXES)

        if page_name.startswith("Category:"):
            return True

        return False

    async def fetch_api_data(self, url: str) -> Dict[str, Any]:
        """
        Dispatch to the appropriate API fetch based on URL type.

        Category URLs  → fetch_category_api_data
        History URLs   → fetch_article_history_api_data
        Anything else  → {} (never called if needs_api_data returns False)
        """
        page_name, action = self._parse_url(url)
        if page_name is None:
            return {}

        if action == "history":
            return await fetch_article_history_api_data(page_name)

        if page_name.startswith("Category:"):
            category_name = page_name[len("Category:"):]
            return await fetch_category_api_data(category_name)

        return {}
