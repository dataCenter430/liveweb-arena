"""CoinGecko API client with rate limiting and API key support"""

import os
import asyncio
from typing import Any, Dict, Optional

import aiohttp


class CoinGeckoClient:
    """
    Centralized CoinGecko API client.

    Supports both free and Pro API:
    - Free: https://api.coingecko.com/api/v3 (rate limited)
    - Pro: https://pro-api.coingecko.com/api/v3 (requires API key)

    Set COINGECKO_API_KEY environment variable to use Pro API.
    """

    FREE_API_BASE = "https://api.coingecko.com/api/v3"
    PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    # Rate limiting for free tier
    _last_request_time: float = 0
    _min_request_interval: float = 2.0  # seconds between requests for free tier
    _lock = asyncio.Lock()

    @classmethod
    def get_api_key(cls) -> Optional[str]:
        """Get API key from environment."""
        return os.getenv("COINGECKO_API_KEY")

    @classmethod
    def get_base_url(cls) -> str:
        """Get appropriate base URL based on API key availability."""
        if cls.get_api_key():
            return cls.PRO_API_BASE
        return cls.FREE_API_BASE

    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Get request headers, including API key if available."""
        headers = {
            "Accept": "application/json",
        }
        api_key = cls.get_api_key()
        if api_key:
            headers["x-cg-pro-api-key"] = api_key
        return headers

    @classmethod
    async def _rate_limit(cls):
        """Apply rate limiting for free tier."""
        if cls.get_api_key():
            # Pro tier has higher limits, minimal delay
            await asyncio.sleep(0.1)
            return

        async with cls._lock:
            import time
            now = time.time()
            elapsed = now - cls._last_request_time
            if elapsed < cls._min_request_interval:
                await asyncio.sleep(cls._min_request_interval - elapsed)
            cls._last_request_time = time.time()

    @classmethod
    async def get(
        cls,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 15.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Make GET request to CoinGecko API.

        Args:
            endpoint: API endpoint (e.g., "/coins/markets")
            params: Query parameters
            timeout: Request timeout in seconds

        Returns:
            JSON response or None on error
        """
        await cls._rate_limit()

        url = f"{cls.get_base_url()}{endpoint}"
        headers = cls.get_headers()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status == 429:
                        # Rate limited - wait and retry once
                        await asyncio.sleep(5)
                        async with session.get(
                            url,
                            params=params,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=timeout),
                        ) as retry_response:
                            if retry_response.status != 200:
                                return None
                            return await retry_response.json()

                    if response.status != 200:
                        return None
                    return await response.json()
        except Exception:
            return None

    @classmethod
    async def get_coin_market_data(
        cls,
        coin_ids: str,
        vs_currency: str = "usd",
    ) -> Optional[list]:
        """
        Get market data for specified coins.

        Args:
            coin_ids: Comma-separated coin IDs (e.g., "bitcoin,ethereum")
            vs_currency: Target currency

        Returns:
            List of coin market data or None
        """
        params = {
            "vs_currency": vs_currency,
            "ids": coin_ids,
            "order": "market_cap_desc",
            "sparkline": "false",
        }
        return await cls.get("/coins/markets", params)

    @classmethod
    async def get_simple_price(
        cls,
        coin_ids: str,
        vs_currencies: str = "usd",
    ) -> Optional[dict]:
        """
        Get simple price data for specified coins.

        Args:
            coin_ids: Comma-separated coin IDs (e.g., "bitcoin,ethereum")
            vs_currencies: Comma-separated currencies (e.g., "usd,eur")

        Returns:
            Dict of prices or None
        """
        params = {
            "ids": coin_ids,
            "vs_currencies": vs_currencies,
        }
        return await cls.get("/simple/price", params)
