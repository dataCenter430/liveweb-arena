"""
Cache Module - On-demand page caching with file locking.

Design:
- Each URL gets its own directory based on URL structure
- HTML and API data are fetched together and stored atomically
- File locks ensure multi-process safety
- TTL-based expiration with automatic refresh

Directory structure:
    cache/
    └── www.coingecko.com/
        └── en/
            └── coins/
                └── bitcoin/
                    ├── page.json   # {url, html, api_data, fetched_at}
                    └── .lock
"""

import asyncio
import fcntl
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from urllib.parse import unquote, urlparse

from liveweb_arena.core.block_patterns import is_captcha_page

if TYPE_CHECKING:
    from liveweb_arena.plugins.base import BasePlugin

logger = logging.getLogger(__name__)

# Default TTL: 48 hours
DEFAULT_TTL = 72 * 3600  # 3 days


class CacheFatalError(Exception):
    """
    Raised when page caching fails due to network issues.

    This indicates the browser cannot load the page, making evaluation invalid.
    Evaluation should be terminated immediately.
    """

    def __init__(self, message: str, url: str = None):
        super().__init__(message)
        self.url = url


def log(tag: str, message: str):
    """Simple logging helper."""
    print(f"[{tag}] {message}")


@dataclass
class CachedPage:
    """Cached page data."""
    url: str
    html: str
    api_data: Optional[Dict[str, Any]]
    fetched_at: float
    accessibility_tree: Optional[str] = None  # Cached for deterministic evaluation
    need_api: bool = True  # Whether this page requires API data (default True for safety)

    def is_expired(self, ttl: int) -> bool:
        return time.time() > self.fetched_at + ttl

    def is_complete(self) -> bool:
        """Check if cache is complete (has API data if needed)."""
        if self.need_api:
            return self.api_data is not None and len(self.api_data) > 0
        return True

    def to_dict(self) -> dict:
        result = {
            "url": self.url,
            "html": self.html,
            "api_data": self.api_data,
            "fetched_at": self.fetched_at,
            "need_api": self.need_api,
        }
        if self.accessibility_tree:
            result["accessibility_tree"] = self.accessibility_tree
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "CachedPage":
        return cls(
            url=data["url"],
            html=data["html"],
            api_data=data.get("api_data"),
            fetched_at=data["fetched_at"],
            accessibility_tree=data.get("accessibility_tree"),
            need_api=data.get("need_api", True),  # Default True for old caches
        )


@dataclass
class PageRequirement:
    """Page caching requirement."""
    url: str
    need_api: bool = False

    @staticmethod
    def nav(url: str) -> "PageRequirement":
        """Create navigation page requirement (HTML only)."""
        return PageRequirement(url, need_api=False)

    @staticmethod
    def data(url: str) -> "PageRequirement":
        """Create data page requirement (HTML + API)."""
        return PageRequirement(url, need_api=True)


async def async_file_lock_acquire(lock_path: Path, timeout: float = 60.0) -> int:
    """
    Acquire file lock asynchronously (non-blocking with retry).

    Returns file descriptor that must be released with async_file_lock_release().

    This avoids blocking the event loop while waiting for the lock.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()

    while True:
        fd = open(lock_path, 'w')
        try:
            # Try non-blocking lock
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd  # Lock acquired, return file object
        except BlockingIOError:
            fd.close()
            # Lock held by another process, wait and retry
            if time.time() - start > timeout:
                raise TimeoutError(f"Could not acquire lock {lock_path} within {timeout}s")
            await asyncio.sleep(0.1)  # Yield to event loop
        except Exception:
            fd.close()
            raise


def async_file_lock_release(fd):
    """Release file lock acquired by async_file_lock_acquire()."""
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    finally:
        fd.close()


def safe_path_component(s: str) -> str:
    """Convert string to safe path component."""
    # Replace dangerous characters
    s = re.sub(r'[<>:"/\\|?*]', '_', s)
    s = s.replace(' ', '_')
    s = s.replace(',', '_')
    s = s.replace('&', '_')
    # Limit length
    if len(s) > 200:
        s = s[:200]
    return s


def normalize_url(url: str) -> str:
    """
    Normalize URL for cache lookup.

    Rules:
    1. Lowercase domain
    2. Remove default ports
    3. Remove tracking parameters
    4. Decode percent-encoding in path and query values
    5. Lowercase query keys and values
    6. Sort remaining query parameters
    """
    parsed = urlparse(url)

    # Lowercase domain
    domain = parsed.netloc.lower()

    # Remove default ports
    if domain.endswith(':80') or domain.endswith(':443'):
        domain = domain.rsplit(':', 1)[0]

    # Path: decode percent-encoding, normalize spaces to +
    path = unquote(parsed.path or '/').replace(' ', '+')

    # Filter, sort, and normalize query parameters
    if parsed.query:
        params = []
        tracking = {'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'ref', 'source'}
        for part in parsed.query.split('&'):
            if '=' in part:
                key = unquote(part.split('=')[0]).lower()
                if key not in tracking:
                    # Decode percent-encoding and lowercase for consistent matching
                    value = unquote(part.split('=', 1)[1]).lower()
                    params.append(f"{key}={value}")
            else:
                params.append(unquote(part).lower())
        query = '&'.join(sorted(params))
    else:
        query = ''

    result = f"{parsed.scheme}://{domain}{path}"
    if query:
        result += f"?{query}"
    return result


def url_to_cache_dir(cache_dir: Path, url: str) -> Path:
    """
    Convert URL to cache directory path.

    Examples:
    https://www.coingecko.com/en/coins/bitcoin
    → cache/www.coingecko.com/en/coins/bitcoin/

    https://stooq.com/q/?s=aapl.us
    → cache/stooq.com/q/__s=aapl.us/
    """
    parsed = urlparse(url)

    # Domain (lowercase)
    domain = parsed.netloc.lower()
    if domain.endswith(':80') or domain.endswith(':443'):
        domain = domain.rsplit(':', 1)[0]

    # Path parts - decode percent-encoding, normalize spaces to +
    path = unquote(parsed.path).replace(' ', '+').strip('/')
    if path:
        path_parts = [safe_path_component(p) for p in path.split('/')]
    else:
        path_parts = ['_root_']

    # Query parameters (decode percent-encoding + lowercase for consistent matching)
    if parsed.query:
        query_part = '__' + safe_path_component(unquote(parsed.query).lower())
        path_parts[-1] = path_parts[-1] + query_part

    return cache_dir / domain / '/'.join(path_parts)


def url_display(url: str) -> str:
    """Get short display string for URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path
    query = f"?{parsed.query}" if parsed.query else ""
    display = f"{domain}{path}{query}"
    if len(display) > 80:
        display = display[:77] + '...'
    return display


class CacheManager:
    """
    Unified cache manager.

    Features:
    - On-demand caching
    - File lock protection for multi-process safety
    - TTL-based expiration
    - API data caching for ground truth validation
    """

    # Minimum interval between consecutive cache-miss fetches (seconds)
    _PREFETCH_INTERVAL = 1.0
    # Retry transient _fetch_page failures (timeout, 5xx, connection reset)
    _MAX_PAGE_RETRIES = 2
    _PAGE_RETRY_DELAY = 3.0

    def __init__(self, cache_dir: Path, ttl: int = None):
        self.cache_dir = Path(cache_dir)
        if ttl is None:
            ttl = int(os.environ.get("LIVEWEB_CACHE_TTL", str(DEFAULT_TTL)))
        self.ttl = ttl
        self._playwright = None
        self._browser = None
        self._browser_lock = asyncio.Lock()
        self._last_fetch_time: float = 0

    async def _ensure_browser(self):
        """Ensure shared Playwright browser is running (lazy singleton)."""
        if self._browser is not None:
            return
        async with self._browser_lock:
            if self._browser is not None:
                return
            from playwright.async_api import async_playwright
            from liveweb_arena.core.block_patterns import STEALTH_BROWSER_ARGS
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True, args=STEALTH_BROWSER_ARGS,
            )

    async def shutdown(self):
        """Shutdown shared browser and Playwright."""
        async with self._browser_lock:
            if self._browser:
                try:
                    await asyncio.wait_for(self._browser.close(), timeout=3)
                except Exception:
                    pass
                self._browser = None
            if self._playwright:
                try:
                    await asyncio.wait_for(self._playwright.stop(), timeout=3)
                except Exception:
                    pass
                self._playwright = None

    async def ensure_cached(
        self,
        pages: List[PageRequirement],
        plugin: "BasePlugin",
    ) -> Dict[str, CachedPage]:
        """
        Ensure specified pages are cached.

        Args:
            pages: List of page requirements
            plugin: Plugin for fetching API data

        Returns:
            {normalized_url: CachedPage} mapping
        """
        result = {}

        for page_req in pages:
            normalized = normalize_url(page_req.url)
            cached = await self._ensure_single(page_req.url, plugin, page_req.need_api)
            result[normalized] = cached

        return result

    async def _ensure_single(
        self,
        url: str,
        plugin: "BasePlugin",
        need_api: bool,
    ) -> CachedPage:
        """Ensure single URL is cached."""
        # Use plugin-specific normalization (e.g. Stooq: aapl → aapl.us)
        normalized = plugin.normalize_url(url)
        cache_dir = url_to_cache_dir(self.cache_dir, normalized)
        cache_file = cache_dir / "page.json"
        lock_file = cache_dir / ".lock"

        page_type = "data" if need_api else "nav"

        # 1. Quick check (no lock)
        cached = self._load_if_valid(cache_file, need_api)
        if cached:
            log("Cache", f"HIT {page_type} - {url_display(normalized)}")
            return cached

        # 2. Need update, acquire async lock (non-blocking to avoid deadlock)
        lock_fd = await async_file_lock_acquire(lock_file)
        try:
            # 3. Double check (another process may have updated)
            cached = self._load_if_valid(cache_file, need_api)
            if cached:
                log("Cache", f"HIT {page_type} (after lock) - {url_display(normalized)}")
                return cached

            # 4. Actually fetch - page and API in parallel when possible
            log("Cache", f"MISS {page_type} - fetching {url_display(normalized)}")

            # Rate limit consecutive fetches to avoid triggering anti-bot
            now = time.time()
            wait = self._PREFETCH_INTERVAL - (now - self._last_fetch_time)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_fetch_time = time.time()

            start = time.time()

            try:
                # Check if plugin can derive GT from the page HTML itself.
                # If so, skip the concurrent API fetch — avoids a redundant
                # request to the same URL (e.g. ArXiv listing pages).
                from liveweb_arena.plugins.base import BasePlugin
                can_extract = (
                    need_api
                    and type(plugin).extract_api_data_from_html
                    is not BasePlugin.extract_api_data_from_html
                )

                if can_extract:
                    # Sequential: fetch page, then try to extract GT from HTML.
                    # If the plugin returns None (URL not extractable), fall
                    # back to the separate fetch_api_data call.
                    html, accessibility_tree = await self._fetch_page_with_retry(
                        url, plugin,
                    )
                    try:
                        api_data = plugin.extract_api_data_from_html(url, html)
                    except Exception as e:
                        raise CacheFatalError(
                            f"API data extraction from HTML failed: {e}",
                            url=url,
                        )
                    if api_data is None:
                        try:
                            api_data = await plugin.fetch_api_data(url)
                        except Exception as e:
                            raise CacheFatalError(
                                f"API data fetch failed (GT will be invalid): {e}",
                                url=url,
                            )
                elif need_api:
                    # Concurrent: page + API fetch in parallel (with retry).
                    # Use asyncio.wait with FIRST_EXCEPTION so that a fast API
                    # failure (e.g. daily rate limit) cancels the slow page
                    # fetch instead of wasting a Playwright browser launch.
                    page_task = asyncio.ensure_future(
                        self._fetch_page_with_retry(url, plugin),
                    )
                    api_task = asyncio.ensure_future(plugin.fetch_api_data(url))

                    done, pending = await asyncio.wait(
                        [page_task, api_task],
                        return_when=asyncio.FIRST_EXCEPTION,
                    )

                    # Cancel pending tasks to free Playwright browser
                    # contexts immediately.  Playwright may emit a harmless
                    # "Future exception was never retrieved" warning for
                    # its internal navigation Future — this is cosmetic.
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass

                    # Collect results / errors
                    page_error = None
                    api_error = None

                    if page_task.done() and not page_task.cancelled():
                        page_error = page_task.exception()
                        if page_error is None:
                            html, accessibility_tree = page_task.result()
                    else:
                        page_error = asyncio.CancelledError("cancelled: API failed first")

                    if api_task.done() and not api_task.cancelled():
                        api_error = api_task.exception()
                        if api_error is None:
                            api_data = api_task.result()
                    else:
                        api_error = asyncio.CancelledError("cancelled: page failed first")

                    # Prioritise the REAL error: if one task was cancelled
                    # because the other failed, report the causal failure so
                    # that downstream backoff classification sees the right
                    # keywords (e.g. "rate limit", "CAPTCHA").
                    if api_error is not None and not isinstance(api_error, asyncio.CancelledError):
                        raise CacheFatalError(
                            f"API data fetch failed (GT will be invalid): {api_error}",
                            url=url,
                        )
                    if page_error is not None:
                        raise CacheFatalError(
                            f"Page fetch failed (browser cannot load): {page_error}",
                            url=url,
                        )
                    if api_error is not None:
                        raise CacheFatalError(
                            f"API data fetch failed (GT will be invalid): {api_error}",
                            url=url,
                        )
                else:
                    html, accessibility_tree = await self._fetch_page_with_retry(
                        url, plugin,
                    )
                    api_data = None

                if need_api and not api_data:
                    raise CacheFatalError(
                        f"API data is empty (GT will be invalid)",
                        url=url,
                    )

                cached = CachedPage(
                    url=url,
                    html=html,
                    api_data=api_data,
                    fetched_at=time.time(),
                    accessibility_tree=accessibility_tree,
                    need_api=need_api,
                )

                self._save(cache_file, cached)
                elapsed = time.time() - start
                log("Cache", f"SAVED {page_type} - {url_display(normalized)} ({elapsed:.1f}s)")
                return cached

            except CacheFatalError as e:
                # Refresh failed — try stale cache as fallback
                stale = self._load_stale(cache_file, need_api)
                if stale:
                    # Backoff: update fetched_at so we don't re-fetch on every
                    # request.  Duration depends on failure type — persistent
                    # failures (CAPTCHA, rate limit) get full TTL; transient
                    # failures (network, timeout) get 1/4 TTL.
                    error_msg = str(e).lower()
                    if "captcha" in error_msg or "challenge" in error_msg:
                        stale.fetched_at = time.time()
                        backoff_label = f"{self.ttl // 3600}h (CAPTCHA)"
                    elif "rate limit" in error_msg or "daily limit" in error_msg:
                        stale.fetched_at = time.time()
                        backoff_label = f"{self.ttl // 3600}h (rate limit)"
                    else:
                        stale.fetched_at = time.time() - self.ttl * 0.75
                        backoff_label = f"{self.ttl // 4 // 3600}h (transient)"
                    self._save(cache_file, stale)
                    log("Cache", f"STALE {page_type} (refresh failed, backoff {backoff_label}) - {url_display(normalized)}")
                    return stale
                raise  # No stale data available, propagate the error
        finally:
            async_file_lock_release(lock_fd)

    def _load_if_valid(self, cache_file: Path, need_api: bool) -> Optional[CachedPage]:
        """Load cache if valid (not expired and complete)."""
        return self._load_cache(cache_file, need_api, allow_stale=False)

    def _load_stale(self, cache_file: Path, need_api: bool) -> Optional[CachedPage]:
        """Load cache even if expired (fallback when refresh fails)."""
        return self._load_cache(cache_file, need_api, allow_stale=True)

    def _load_cache(self, cache_file: Path, need_api: bool, allow_stale: bool) -> Optional[CachedPage]:
        """Load cache with optional staleness tolerance."""
        if not cache_file.exists():
            return None

        try:
            cached = self._load(cache_file)
        except Exception as e:
            logger.warning(f"Failed to load cache {cache_file}: {e}")
            self._delete_cache(cache_file)
            return None

        # Content quality: reject CAPTCHA/challenge pages persisted by older code.
        # Unconditional — a CAPTCHA page is never valid, fresh or stale.
        if cached.html and is_captcha_page(cached.html, ""):
            logger.warning(f"CAPTCHA content in cache, deleting: {cache_file}")
            self._delete_cache(cache_file)
            return None

        # Reject trivially short HTML when there's no API data to compensate.
        # API-only pages (e.g. api.taostats.io) legitimately have minimal HTML.
        if not cached.api_data and cached.html and len(cached.html) < 200:
            logger.warning(f"Short HTML ({len(cached.html)}B) without api_data, deleting: {cache_file}")
            self._delete_cache(cache_file)
            return None

        if not allow_stale and cached.is_expired(self.ttl):
            return None

        # Check if cache is complete
        if not cached.is_complete() or (need_api and not cached.api_data):
            if not allow_stale:
                return None
            # Stale mode: still require api_data when need_api (GT depends on it)
            if need_api and not cached.api_data:
                return None
            if not cached.html:
                return None

        return cached

    def _delete_cache(self, cache_file: Path):
        """Delete cache file."""
        try:
            if cache_file.exists():
                cache_file.unlink()
        except Exception as e:
            logger.warning(f"Failed to delete cache {cache_file}: {e}")

    def _load(self, cache_file: Path) -> CachedPage:
        """Load cache from file."""
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return CachedPage.from_dict(data)

    def _save(self, cache_file: Path, cached: CachedPage):
        """Save cache to file."""
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cached.to_dict(), f, ensure_ascii=False)

    # HTTP status codes that _fetch_page may raise but should be retried.
    _RETRYABLE_HTTP = {429, 500, 502, 503, 504}

    async def _fetch_page_with_retry(self, url: str, plugin=None) -> tuple:
        """Fetch page HTML with retry for transient failures.

        Retries on timeouts, HTTP 5xx, 429, and connection errors.
        Permanent failures (other 4xx, CAPTCHA) are raised immediately.
        """
        last_error: Exception = None
        for attempt in range(self._MAX_PAGE_RETRIES):
            try:
                return await self._fetch_page(url, plugin)
            except CacheFatalError as e:
                if not self._is_retryable(e):
                    raise
                last_error = e
            except Exception as e:
                last_error = e

            if attempt < self._MAX_PAGE_RETRIES - 1:
                log("Cache", f"Page fetch retry {attempt + 1} for "
                    f"{url_display(url)}: {last_error}")
                await asyncio.sleep(self._PAGE_RETRY_DELAY)

        raise CacheFatalError(
            f"Page fetch failed (browser cannot load): {last_error}",
            url=url,
        )

    _HTTP_STATUS_RE = re.compile(r"^HTTP (\d{3}) ")

    def _is_retryable(self, error: CacheFatalError) -> bool:
        """Determine whether a _fetch_page error is worth retrying.

        Retryable: timeouts, HTTP 429/5xx, connection errors, short pages.
        Permanent: HTTP 4xx (except 429), CAPTCHA/challenge pages.
        """
        msg = str(error)
        if "CAPTCHA" in msg:
            return False
        m = self._HTTP_STATUS_RE.match(msg)
        if m:
            return int(m.group(1)) in self._RETRYABLE_HTTP
        # Non-HTTP errors (timeouts, connection resets, short pages) — retry
        return True

    async def _fetch_page(self, url: str, plugin=None) -> tuple:
        """
        Fetch page HTML and accessibility tree using shared Playwright browser.

        Args:
            url: Page URL to fetch
            plugin: Optional plugin for page setup (e.g., click "Show All")

        Returns:
            (html, accessibility_tree) tuple
        """
        from liveweb_arena.core.block_patterns import (
            STEALTH_INIT_SCRIPT, STEALTH_USER_AGENT,
            is_captcha_page, should_block_url,
        )

        await self._ensure_browser()
        try:
            context = await self._browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=STEALTH_USER_AGENT,
            )
        except Exception:
            # Browser process may have crashed — clean up and retry once
            await self.shutdown()
            await self._ensure_browser()
            context = await self._browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=STEALTH_USER_AGENT,
            )
        try:
            page = await context.new_page()
            await page.add_init_script(STEALTH_INIT_SCRIPT)

            # Block tracking/ads to avoid networkidle delays
            # Merge global + plugin-specific block patterns
            plugin_block_res = []
            if plugin and hasattr(plugin, 'get_blocked_patterns'):
                for pat in plugin.get_blocked_patterns():
                    regex_pat = re.escape(pat).replace(r"\*", ".*")
                    plugin_block_res.append(re.compile(regex_pat, re.IGNORECASE))

            async def _block_tracking(route):
                req_url = route.request.url
                if should_block_url(req_url):
                    await route.abort("blockedbyclient")
                    return
                for pat_re in plugin_block_res:
                    if pat_re.search(req_url):
                        await route.abort("blockedbyclient")
                        return
                await route.continue_()

            await page.route("**/*", _block_tracking)

            response = await page.goto(url, timeout=60000, wait_until="domcontentloaded")

            # Layer 1: HTTP status check
            if response and response.status >= 400:
                raise CacheFatalError(
                    f"HTTP {response.status} for {url}",
                    url=url,
                )

            # Wait for network idle (short timeout: ads are blocked, so
            # legitimate content loads in ~3-4s; streaming endpoints like
            # aq*.stooq.com keep connections open indefinitely)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

            # Plugin-specific page setup (e.g., click "ALL" to show all rows)
            if plugin and hasattr(plugin, 'setup_page_for_cache'):
                try:
                    await plugin.setup_page_for_cache(page, url)
                except Exception as e:
                    log("Cache", f"Page setup failed (continuing): {e}")

            # Scroll to trigger lazy loading
            for pos in [0, 500, 1000, 2000]:
                await page.evaluate(f"window.scrollTo(0, {pos})")
                await page.wait_for_timeout(300)

            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(500)

            html = await page.content()

            # Layer 2: CAPTCHA/challenge detection
            page_title = await page.title()
            if is_captcha_page(html, page_title):
                raise CacheFatalError(
                    f"CAPTCHA/challenge page detected (title: {page_title!r})",
                    url=url,
                )

            # Layer 3: Minimum content length (real pages are >5KB)
            if len(html) < 1000:
                raise CacheFatalError(
                    f"Page too short ({len(html)} bytes, title: {page_title!r})",
                    url=url,
                )

            # Extract accessibility tree for deterministic caching
            a11y_tree = ""
            try:
                a11y_snapshot = await page.accessibility.snapshot()
                if a11y_snapshot:
                    a11y_tree = self._format_accessibility_tree(a11y_snapshot)
            except Exception:
                pass

            # If accessibility tree is empty, get page text content
            if len(a11y_tree.strip()) < 100:
                try:
                    page_text = await page.evaluate("""
                        () => {
                            const preElements = document.querySelectorAll('pre');
                            if (preElements.length > 0) {
                                return Array.from(preElements).map(el => el.innerText).join('\\n');
                            }
                            return document.body.innerText || '';
                        }
                    """)
                    if page_text.strip():
                        if a11y_tree.strip():
                            a11y_tree += "\n\n--- Page Text Content ---\n" + page_text
                        else:
                            a11y_tree = page_text
                except Exception:
                    pass

            return html, a11y_tree
        finally:
            await context.close()

    def _format_accessibility_tree(self, node: dict, indent: int = 0) -> str:
        """Format accessibility tree node recursively."""
        if not node:
            return ""

        lines = []
        prefix = "\t" * indent

        role = node.get("role", "")
        name = node.get("name", "")
        value = node.get("value", "")

        parts = [role]
        if name:
            parts.append(f'"{name}"')
        if value:
            parts.append(f'value="{value}"')

        lines.append(f"{prefix}{' '.join(parts)}")

        children = node.get("children", [])
        for child in children:
            lines.append(self._format_accessibility_tree(child, indent + 1))

        return "\n".join(lines)

    def get_cached(self, url: str) -> Optional[CachedPage]:
        """Get cached page without triggering update."""
        normalized = normalize_url(url)
        cache_dir = url_to_cache_dir(self.cache_dir, normalized)
        cache_file = cache_dir / "page.json"

        if not cache_file.exists():
            return None

        try:
            return self._load(cache_file)
        except Exception:
            return None

