"""Wikipedia MediaWiki Action API client."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from liveweb_arena.plugins.base_client import APIFetchError, BaseAPIClient, RateLimiter

logger = logging.getLogger(__name__)

CACHE_SOURCE = "wikipedia"

WIKI_API = "https://en.wikipedia.org/w/api.php"
_USER_AGENT = (
    "liveweb-arena-eval/1.0 (automated research; "
    "https://github.com/AffineFoundation/liveweb-arena)"
)

# Shared session for connection reuse
_session: Optional[aiohttp.ClientSession] = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(headers={"User-Agent": _USER_AGENT})
    return _session


async def close_session() -> None:
    """Close the shared session. Call during shutdown."""
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None


class WikipediaClient(BaseAPIClient):
    """
    Wikipedia MediaWiki Action API client.

    Uses formatversion=2 for consistent list-based page responses.
    Rate limited to 1 request/second to remain polite.
    """

    _rate_limiter = RateLimiter(min_interval=1.0)
    MAX_RETRIES = 3

    @classmethod
    async def get(cls, params: Dict[str, str], timeout: float = 15.0) -> Any:
        """
        GET request to the MediaWiki Action API.

        Injects format=json and formatversion=2.
        Raises APIFetchError on HTTP errors or connection failures.
        Retries up to MAX_RETRIES times on 5xx responses.
        """
        all_params = {"format": "json", "formatversion": "2", **params}
        session = await _get_session()

        for attempt in range(cls.MAX_RETRIES):
            await cls._rate_limit()
            try:
                async with session.get(
                    WIKI_API,
                    params=all_params,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status == 200:
                        return await response.json(content_type=None)
                    if response.status >= 500 and attempt < cls.MAX_RETRIES - 1:
                        wait = 2 ** attempt
                        logger.info(
                            f"Wikipedia API {response.status}, retry in {wait}s "
                            f"(attempt {attempt + 1}/{cls.MAX_RETRIES})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    raise APIFetchError(
                        f"Wikipedia API returned HTTP {response.status}",
                        source="wikipedia",
                        status_code=response.status,
                    )
            except APIFetchError:
                raise
            except Exception as e:
                if attempt < cls.MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    logger.info(f"Wikipedia API request failed: {e}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise APIFetchError(
                    f"Wikipedia API request failed: {e}", source="wikipedia"
                ) from e

        raise APIFetchError("Wikipedia API: max retries exceeded", source="wikipedia")


async def fetch_category_api_data(category_name: str) -> Dict[str, Any]:
    """
    Fetch article and subcategory counts for a Wikipedia category.

    Uses the ``categoryinfo`` prop which returns the same totals shown on the
    category page header:
        "The following X pages are in this category, out of Y total."

    Args:
        category_name: Category name without the "Category:" prefix.

    Returns:
        {
            "type": "category",
            "category_name": str,
            "article_count": int,      # Direct pages (ns=0) in the category
            "subcategory_count": int,  # Direct subcategories
        }

    Raises:
        APIFetchError: If the category does not exist or the request fails.
    """
    data = await WikipediaClient.get({
        "action": "query",
        "titles": f"Category:{category_name}",
        "prop": "categoryinfo",
    })

    pages = data.get("query", {}).get("pages", [])
    if not pages:
        raise APIFetchError(
            f"No data returned for Category:{category_name}", source="wikipedia"
        )

    page = pages[0]
    if page.get("missing"):
        raise APIFetchError(
            f"Wikipedia category does not exist: '{category_name}'",
            source="wikipedia",
        )

    catinfo = page.get("categoryinfo", {})
    return {
        "type": "category",
        "category_name": category_name,
        "article_count": catinfo.get("pages", 0),
        "subcategory_count": catinfo.get("subcats", 0),
    }


async def fetch_article_history_api_data(title: str) -> Dict[str, Any]:
    """
    Fetch revision timestamps for a Wikipedia article over the past 35 days.

    Fetches up to 500 revisions (rvlimit=500). The 35-day window is chosen to
    provide buffer for any question window ≤ 30 days. Timestamps are stored in
    ISO 8601 UTC so the GT template can count within any sub-window relative to
    the fetch time.

    Args:
        title: Article title with spaces (e.g. "Quantum mechanics").

    Returns:
        {
            "type": "history",
            "article_title": str,
            "revisions": [{"timestamp": str (ISO 8601 UTC)}, ...],
            "fetched_at": str,  # ISO 8601 UTC — used as the reference "now"
        }

    Raises:
        APIFetchError: If the article does not exist or the request fails.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    window_start = now - timedelta(days=35)
    fmt = "%Y-%m-%dT%H:%M:%SZ"

    data = await WikipediaClient.get({
        "action": "query",
        "titles": title,
        "prop": "revisions",
        "rvprop": "timestamp",
        "rvlimit": "500",
        "rvstart": now.strftime(fmt),
        "rvend": window_start.strftime(fmt),
        "rvdir": "older",
    })

    pages = data.get("query", {}).get("pages", [])
    if not pages:
        raise APIFetchError(
            f"No data returned for article '{title}'", source="wikipedia"
        )

    page = pages[0]
    if page.get("missing"):
        raise APIFetchError(
            f"Wikipedia article does not exist: '{title}'", source="wikipedia"
        )

    revisions: List[Dict[str, str]] = [
        {"timestamp": r["timestamp"]} for r in page.get("revisions", [])
    ]

    return {
        "type": "history",
        "article_title": page.get("title", title),
        "revisions": revisions,
        "fetched_at": now.strftime(fmt),
    }
