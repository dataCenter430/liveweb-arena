"""TMDB API client with rate limiting and Bearer token auth"""

import os
import asyncio
from typing import Any, Dict, Optional

import aiohttp


class TMDBClient:
    """
    Centralized TMDB API client.

    Uses Bearer token authentication via TMDB_API_KEY environment variable.
    TMDB rate limit: 40 requests per 10 seconds.
    """

    API_BASE = "https://api.themoviedb.org/3"

    # Rate limiting
    _last_request_time: float = 0
    _min_request_interval: float = 0.25  # 4 requests per second to stay under limit
    _lock = asyncio.Lock()

    @classmethod
    def get_api_key(cls) -> Optional[str]:
        """Get API key from environment."""
        return os.getenv("TMDB_API_KEY")

    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Get request headers with Bearer token."""
        api_key = cls.get_api_key()
        headers = {
            "Accept": "application/json",
        }
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    @classmethod
    async def _rate_limit(cls):
        """Apply rate limiting."""
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
        Make GET request to TMDB API.

        Args:
            endpoint: API endpoint (e.g., "/movie/550")
            params: Query parameters
            timeout: Request timeout in seconds

        Returns:
            JSON response or None on error
        """
        await cls._rate_limit()

        url = f"{cls.API_BASE}{endpoint}"
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
    async def get_movie(cls, movie_id: str) -> Optional[Dict[str, Any]]:
        """
        Get movie details by ID.

        Args:
            movie_id: TMDB movie ID

        Returns:
            Movie data dict or None
        """
        return await cls.get(f"/movie/{movie_id}")

    @classmethod
    async def get_movie_credits(cls, movie_id: str) -> Optional[Dict[str, Any]]:
        """
        Get movie credits (cast and crew) by ID.

        Args:
            movie_id: TMDB movie ID

        Returns:
            Credits data dict or None
        """
        return await cls.get(f"/movie/{movie_id}/credits")

    @classmethod
    async def get_movie_with_credits(cls, movie_id: str) -> Optional[Dict[str, Any]]:
        """
        Get movie details with appended credits.

        Args:
            movie_id: TMDB movie ID

        Returns:
            Movie data with credits or None
        """
        return await cls.get(f"/movie/{movie_id}", params={"append_to_response": "credits"})
