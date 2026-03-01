"""Shared URL blocking and CAPTCHA detection patterns.

URL blocking: Used by CacheInterceptor (main browser) and CacheManager._fetch_page
(prefetch browser) to block tracking/ad requests that delay networkidle.

CAPTCHA detection: Used by _fetch_page to detect Cloudflare challenge pages
and trigger retry logic instead of caching invalid content.
"""

import re
from typing import List

TRACKING_BLOCK_PATTERNS: List[str] = [
    # Google
    r"google-analytics\.com",
    r"googletagmanager\.com",
    r"googlesyndication\.com",
    r"googleadservices\.com",
    r"google\.com/recaptcha",
    r"doubleclick\.net",
    # Social widgets
    r"facebook\.com/tr",
    r"platform\.twitter\.com",
    r"syndication\.twitter\.com",
    # Analytics
    r"hotjar\.com",
    r"sentry\.io",
    r"analytics",
    r"tracking",
    r"pixel",
    r"beacon",
    # Ad networks & sync
    r"rubiconproject\.com",
    r"criteo\.com",
    r"3lift\.com",
    r"pubmatic\.com",
    r"media\.net",
    r"adnxs\.com",
    r"presage\.io",
    r"onetag-sys\.com",
    r"seedtag\.com",
    r"openx\.net",
    r"btloader\.com",
    r"tappx\.com",
    r"cloudflare\.com/cdn-cgi/challenge",
    # Generic patterns
    r"usync",
    r"syncframe",
    r"user_sync",
    r"checksync",
    # Site-specific ads
    r"stooq\.com/ads/",
]

_BLOCK_RE = re.compile("|".join(TRACKING_BLOCK_PATTERNS), re.IGNORECASE)


def should_block_url(url: str) -> bool:
    """Check if URL matches any tracking/ads pattern."""
    return bool(_BLOCK_RE.search(url))


# ---------------------------------------------------------------------------
# CAPTCHA / challenge page detection
# ---------------------------------------------------------------------------
# Strong signals only — these NEVER appear on normal pages.
# Passive scripts (Turnstile API, reCAPTCHA, hCaptcha) are excluded
# because sites like CoinGecko embed them without blocking content.

CAPTCHA_SIGNALS = [
    # Cloudflare
    ("Just a moment", "title"),
    ("Attention Required", "title"),
    ("Checking your browser", "html"),
    ("cf-browser-verification", "html"),
    ("cf_chl_opt", "html"),
    ("/cdn-cgi/challenge-platform/", "html"),
    # Generic
    ("Access denied", "title"),
    ("Please verify you are a human", "html"),
]


def is_captcha_page(html: str, title: str = "") -> bool:
    """Detect if page is a CAPTCHA/challenge instead of real content."""
    title_lower = title.lower()
    for signal, location in CAPTCHA_SIGNALS:
        if location == "title" and signal.lower() in title_lower:
            return True
        elif location == "html" and signal in html:
            return True
    return False
