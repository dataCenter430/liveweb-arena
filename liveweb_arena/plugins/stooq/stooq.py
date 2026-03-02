"""
Stooq Plugin.

Plugin for financial market data from stooq.com.
"""

import re
from typing import Any, Dict, List
from urllib.parse import urlparse, parse_qs

from liveweb_arena.plugins.base import BasePlugin
from .api_client import fetch_single_asset_data, fetch_homepage_api_data


class StooqPlugin(BasePlugin):
    """
    Stooq plugin for financial market data.

    Handles pages like:
    - https://stooq.com/ (homepage - all assets)
    - https://stooq.com/q/?s=aapl.us (stocks)
    - https://stooq.com/q/?s=^spx (indices)
    - https://stooq.com/q/?s=gc.c (commodities)
    - https://stooq.com/q/?s=eurusd (forex)

    API data includes: open, high, low, close, volume, daily_change_pct, etc.
    """

    name = "stooq"

    allowed_domains = [
        "stooq.com",
        "www.stooq.com",
    ]

    def get_blocked_patterns(self) -> List[str]:
        """Block direct CSV download and ads."""
        return [
            "*/q/d/l/*",  # CSV download endpoint
            "*stooq.com/ads/*",  # Ad frames
        ]

    async def fetch_api_data(self, url: str) -> Dict[str, Any]:
        """
        Fetch API data for a Stooq page.

        - Homepage: Returns all assets in {"assets": {...}} format
        - Detail page: Returns single asset data with "symbol" field

        Args:
            url: Page URL

        Returns:
            API data appropriate for the page type
        """
        # Check for detail page first
        symbol = self._extract_symbol(url)
        if symbol:
            data = await fetch_single_asset_data(symbol)
            if not data:
                raise ValueError(f"Stooq API returned no data for symbol={symbol}")
            return data

        # Homepage - return all assets
        if self._is_homepage(url):
            return await fetch_homepage_api_data()

        return {}

    def _is_homepage(self, url: str) -> bool:
        """Check if URL is the Stooq homepage."""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        # Homepage has no path or just "/"
        return path == '' and not parsed.query

    def needs_api_data(self, url: str) -> bool:
        """
        Determine if this URL needs API data for ground truth.

        Only homepage and asset detail pages can provide API data.
        Other pages (q/d/, q/a/, etc.) are navigation-only.

        Args:
            url: Page URL

        Returns:
            True if API data is needed and available, False otherwise
        """
        # Asset detail page needs API data
        if self._extract_symbol(url):
            return True
        # Homepage needs API data
        if self._is_homepage(url):
            return True
        # Other pages don't need API data
        return False

    def _extract_symbol(self, url: str) -> str:
        """
        Extract symbol from Stooq URL.

        Examples:
            https://stooq.com/q/?s=aapl.us -> aapl.us
            https://stooq.com/q/?s=^spx -> ^spx
            https://stooq.com/q/d/?s=gc.c -> gc.c
            http://stooq.com/q/s/?e=abbv&t= -> abbv (redirected URL format)
        """
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        # Check for 's' parameter (original format)
        if "s" in query:
            return query["s"][0].lower()

        # Check for 'e' parameter (redirected URL format: /q/s/?e=symbol&t=)
        if "e" in query:
            return query["e"][0].lower()

        return ""
