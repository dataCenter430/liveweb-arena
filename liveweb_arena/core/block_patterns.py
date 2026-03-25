"""Shared browser anti-detection, URL blocking, and CAPTCHA detection.

Used by both CacheManager._fetch_page (prefetch browser) and CacheInterceptor
(agent browser) to block tracking requests, detect challenge pages, and
present a realistic browser fingerprint.
"""

import re
from typing import List


# ---------------------------------------------------------------------------
# Browser stealth configuration
# ---------------------------------------------------------------------------
# Shared between prefetch browser (cache.py) and agent browser (browser.py).

STEALTH_BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-blink-features=AutomationControlled",
]

STEALTH_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Patches navigator properties that headless Chrome exposes.
# Must be injected via page.add_init_script() BEFORE page.goto().
STEALTH_INIT_SCRIPT = """
    // Hide webdriver flag — Cloudflare specifically checks this
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
    });
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5],
    });
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
    });
    window.chrome = { runtime: {}, };
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) =>
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters);
    // Hide automation indicators
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
"""

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
    # DataDome
    ("captcha-delivery.com", "html"),
    ("geo.captcha-delivery.com", "html"),
    # PerimeterX / HUMAN Security
    ("perimeterx.net/", "html"),
    ("human.com/bot", "html"),
    # Akamai Bot Manager
    ("ak-challenge", "html"),
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
