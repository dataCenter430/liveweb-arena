"""
Step-wise Reward System for LiveWeb Arena

Provides intermediate reward signals to guide RL training:
- Exploration rewards (new domains, new assets, target assets, detail page visits)
- Efficiency rewards (early completion)
- Penalties (repeated URLs, blocked URLs, action failures)
- Terminal rewards (task success/partial/timeout)
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse, urlunparse


class RewardSignal(Enum):
    """Reward signal types for step-wise feedback."""
    # Exploration rewards (positive)
    NEW_DOMAIN = "new_domain"
    NEW_ASSET = "new_asset"
    TARGET_ASSET = "target_asset"
    ALL_TARGETS = "all_targets"
    DETAIL_PAGE_VISIT = "detail_page_visit"  # New: reward for visiting detail pages

    # Efficiency rewards (positive)
    EARLY_COMPLETION = "early_completion"

    # Penalties (negative)
    REPEATED_URL = "repeated_url"
    BLOCKED_URL = "blocked_url"
    ACTION_FAILED = "action_failed"
    PARSE_FAILED = "parse_failed"
    NO_PROGRESS = "no_progress"

    # Terminal rewards
    TASK_SUCCESS = "task_success"
    TASK_PARTIAL = "task_partial"
    MAX_STEPS = "max_steps"


# Detail page URL patterns for different sites
DETAIL_PAGE_PATTERNS = [
    # CoinGecko: /en/coins/bitcoin, /coins/ethereum
    r"coingecko\.com/(?:en/)?coins/[a-z0-9-]+$",
    # Stooq: /q/?s=aapl.us or /q/d/?s=aapl.us
    r"stooq\.com/q/(?:d/)?\?s=[a-z0-9.]+",
    # Taostats: /subnet/1 or /subnets/1
    r"taostats\.io/subnets?/\d+",
    # Weather: /City or /City?format=...
    r"wttr\.in/[A-Za-z+]+(?:\?|$)",
]


def is_detail_page(url: str) -> bool:
    """Check if URL is a detail page (not a list/homepage)."""
    url_lower = url.lower()
    for pattern in DETAIL_PAGE_PATTERNS:
        if re.search(pattern, url_lower):
            return True
    return False


@dataclass
class RewardConfig:
    """Configurable reward parameters."""
    # Exploration rewards (v3: further reduced for better step/terminal ratio)
    new_domain_reward: float = 0.05       # Was 0.08
    new_asset_reward: float = 0.06        # Was 0.10
    target_asset_reward: float = 0.10     # Was 0.15
    all_targets_bonus: float = 0.15       # Was 0.20
    detail_page_reward: float = 0.03      # Was 0.05

    # Efficiency rewards
    early_completion_multiplier: float = 0.08  # Was 0.10

    # Penalties (kept reasonable)
    repeated_url_penalty: float = -0.04   # Was -0.05
    blocked_url_penalty: float = -0.06    # Was -0.08
    action_failed_penalty: float = -0.02
    parse_failed_penalty: float = -0.08   # Was -0.10
    no_progress_penalty: float = -0.02    # Was -0.03

    # Terminal rewards (v3: increased to dominate)
    success_reward: float = 2.00          # Was 1.50
    partial_multiplier: float = 0.70      # Was 0.60
    max_steps_penalty: float = -0.25      # Was -0.20

    # Normalization bounds
    max_step_reward: float = 0.4  # Was 0.5
    min_step_reward: float = -0.15  # Was -0.2


@dataclass
class RewardBreakdown:
    """Breakdown of reward for a single step."""
    total: float = 0.0
    signals: List[tuple] = field(default_factory=list)

    def add(self, signal: RewardSignal, value: float, reason: str = ""):
        """Add a reward signal."""
        self.signals.append((signal.value, value, reason))
        self.total += value

    def clamp(self, min_val: float, max_val: float):
        """Clamp total reward to bounds."""
        self.total = max(min_val, min(max_val, self.total))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total": self.total,
            "signals": [
                {"signal": s, "value": v, "reason": r}
                for s, v, r in self.signals
            ],
        }


class StepwiseRewardCalculator:
    """
    Step-wise reward calculator for browser agent training.

    Tracks state and computes rewards for:
    - New domain visits
    - New asset collection
    - Target asset progress
    - Detail page visits (encourages visiting detail pages for accurate data)
    - Efficiency metrics
    - Penalties for ineffective actions
    """

    def __init__(
        self,
        config: Optional[RewardConfig] = None,
        target_assets: Optional[Set[str]] = None,
        required_domains: Optional[Set[str]] = None,
    ):
        """
        Initialize reward calculator.

        Args:
            config: Reward configuration (uses defaults if None)
            target_assets: Set of asset IDs the agent should collect
            required_domains: Set of domains the agent should visit
        """
        self.config = config or RewardConfig()
        self.target_assets = target_assets or set()
        self.required_domains = required_domains or set()

        # State tracking
        self._visited_urls: Set[str] = set()
        self._visited_domains: Set[str] = set()
        self._collected_assets: Set[str] = set()
        self._collected_targets: Set[str] = set()
        self._confirmed_assets: Set[str] = set()  # Assets confirmed via detail page
        self._all_targets_collected: bool = False

    def calculate_step_reward(
        self,
        url: str,
        action_result: str,
        collected_asset_ids: Set[str],
        is_blocked: bool = False,
        parse_failed: bool = False,
    ) -> RewardBreakdown:
        """
        Calculate reward for a single step.

        Args:
            url: The URL visited in this step
            action_result: Result of the action (e.g., "Success", "Failed: ...")
            collected_asset_ids: Set of all asset IDs collected so far
            is_blocked: Whether the URL was blocked
            parse_failed: Whether action parsing failed

        Returns:
            RewardBreakdown with total reward and signal breakdown
        """
        breakdown = RewardBreakdown()

        # 1. Parse failed penalty (return early)
        if parse_failed:
            breakdown.add(
                RewardSignal.PARSE_FAILED,
                self.config.parse_failed_penalty,
                "Action parse failed"
            )
            breakdown.clamp(self.config.min_step_reward, self.config.max_step_reward)
            return breakdown

        # 2. Blocked URL penalty (return early)
        if is_blocked:
            breakdown.add(
                RewardSignal.BLOCKED_URL,
                self.config.blocked_url_penalty,
                f"Blocked: {url[:50]}"
            )
            breakdown.clamp(self.config.min_step_reward, self.config.max_step_reward)
            return breakdown

        # 3. Action failed penalty
        if "Failed" in action_result:
            breakdown.add(
                RewardSignal.ACTION_FAILED,
                self.config.action_failed_penalty,
                action_result[:80]
            )

        # 4. Repeated URL penalty
        normalized_url = self._normalize_url(url)
        if normalized_url in self._visited_urls and url != "about:blank":
            breakdown.add(
                RewardSignal.REPEATED_URL,
                self.config.repeated_url_penalty,
                f"Repeated: {url[:50]}"
            )
        else:
            self._visited_urls.add(normalized_url)

        # 5. New domain reward
        domain = self._extract_domain(url)
        if domain and domain not in self._visited_domains:
            self._visited_domains.add(domain)
            breakdown.add(
                RewardSignal.NEW_DOMAIN,
                self.config.new_domain_reward,
                f"New domain: {domain}"
            )

        # 6. New asset reward
        new_assets = collected_asset_ids - self._collected_assets
        if new_assets:
            self._collected_assets.update(new_assets)
            breakdown.add(
                RewardSignal.NEW_ASSET,
                self.config.new_asset_reward * len(new_assets),
                f"+{len(new_assets)} assets"
            )

            # 7. Target asset reward
            if self.target_assets:
                new_targets = new_assets & self.target_assets
                if new_targets:
                    self._collected_targets.update(new_targets)
                    breakdown.add(
                        RewardSignal.TARGET_ASSET,
                        self.config.target_asset_reward * len(new_targets),
                        f"+{len(new_targets)} targets"
                    )

                    # 8. All targets collected bonus
                    if (self._collected_targets >= self.target_assets and
                        not self._all_targets_collected):
                        self._all_targets_collected = True
                        breakdown.add(
                            RewardSignal.ALL_TARGETS,
                            self.config.all_targets_bonus,
                            "All targets collected!"
                        )

        # 9. Detail page visit reward (even if asset already collected from homepage)
        # This encourages visiting detail pages for more accurate GT data
        if is_detail_page(url) and url != "about:blank":
            # Extract asset ID from URL and check if it's a target
            asset_id = self._extract_asset_from_url(url)
            if asset_id and asset_id not in self._confirmed_assets:
                self._confirmed_assets.add(asset_id)
                # Give reward if this is a target asset being confirmed
                if asset_id in self.target_assets or not self.target_assets:
                    breakdown.add(
                        RewardSignal.DETAIL_PAGE_VISIT,
                        self.config.detail_page_reward,
                        f"Detail: {asset_id}"
                    )

        # 10. No progress penalty (only if no positive signals)
        positive_signals = [s for s in breakdown.signals if s[1] > 0]
        if not positive_signals and url != "about:blank":
            breakdown.add(
                RewardSignal.NO_PROGRESS,
                self.config.no_progress_penalty,
                "No progress"
            )

        breakdown.clamp(self.config.min_step_reward, self.config.max_step_reward)
        return breakdown

    def calculate_terminal_reward(
        self,
        validation_score: float,
        steps_used: int,
        max_steps: int,
        truncated: bool,
    ) -> RewardBreakdown:
        """
        Calculate terminal reward at episode end.

        Args:
            validation_score: Final validation score (0.0 - 1.0)
            steps_used: Number of steps taken
            max_steps: Maximum allowed steps
            truncated: Whether episode was truncated due to max_steps

        Returns:
            RewardBreakdown with terminal rewards
        """
        breakdown = RewardBreakdown()

        # Truncation penalty
        if truncated:
            breakdown.add(
                RewardSignal.MAX_STEPS,
                self.config.max_steps_penalty,
                f"Truncated at {max_steps}"
            )

        # Success/partial reward based on validation score
        if validation_score >= 0.8:
            breakdown.add(
                RewardSignal.TASK_SUCCESS,
                self.config.success_reward,
                f"Success: {validation_score:.2f}"
            )
            # Early completion bonus
            if steps_used < max_steps * 0.6:
                efficiency = (max_steps - steps_used) / max_steps
                breakdown.add(
                    RewardSignal.EARLY_COMPLETION,
                    self.config.early_completion_multiplier * efficiency,
                    f"Early: {steps_used}/{max_steps}"
                )
        elif validation_score >= 0.3:
            breakdown.add(
                RewardSignal.TASK_PARTIAL,
                validation_score * self.config.partial_multiplier,
                f"Partial: {validation_score:.2f}"
            )

        return breakdown

    def reset(self):
        """Reset state for a new episode."""
        self._visited_urls.clear()
        self._visited_domains.clear()
        self._collected_assets.clear()
        self._collected_targets.clear()
        self._confirmed_assets.clear()
        self._all_targets_collected = False

    def get_state(self) -> Dict[str, Any]:
        """Get current state for debugging/logging."""
        return {
            "visited_urls": len(self._visited_urls),
            "visited_domains": list(self._visited_domains),
            "collected_assets": len(self._collected_assets),
            "collected_targets": len(self._collected_targets),
            "confirmed_assets": len(self._confirmed_assets),
            "all_targets_collected": self._all_targets_collected,
        }

    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for duplicate detection.

        For most sites: remove query/fragment (path-based routing)
        For query-based sites (stooq, wttr.in): keep essential query params
        """
        try:
            p = urlparse(url)
            domain = p.netloc.lower()

            # Stooq uses query params for asset identification: /q/?s=aapl.us
            if "stooq.com" in domain and p.query:
                # Keep the 's' parameter which identifies the asset
                from urllib.parse import parse_qs, urlencode
                params = parse_qs(p.query)
                if 's' in params:
                    kept_query = urlencode({'s': params['s'][0]})
                    return urlunparse((p.scheme, p.netloc, p.path, '', kept_query, ''))

            # wttr.in uses path for location, query for format
            # /Tokyo and /Tokyo?format=j1 should be same location
            if "wttr.in" in domain:
                return urlunparse((p.scheme, p.netloc, p.path, '', '', ''))

            # Default: remove query/fragment (path-based sites like CoinGecko)
            return urlunparse((p.scheme, p.netloc, p.path, '', '', ''))
        except Exception:
            return url

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return None

    def _extract_asset_from_url(self, url: str) -> Optional[str]:
        """Extract asset ID from a detail page URL."""
        url_lower = url.lower()

        # CoinGecko: /en/coins/bitcoin -> bitcoin
        match = re.search(r"coingecko\.com/(?:en/)?coins/([a-z0-9-]+)", url_lower)
        if match:
            return match.group(1)

        # Stooq: /q/?s=aapl.us -> aapl.us
        match = re.search(r"stooq\.com/q/(?:d/)?\?s=([a-z0-9.]+)", url_lower)
        if match:
            return match.group(1)

        # Taostats: /subnet/1 -> 1
        match = re.search(r"taostats\.io/subnets?/(\d+)", url_lower)
        if match:
            return match.group(1)

        # Weather: /Tokyo -> Tokyo
        match = re.search(r"wttr\.in/([A-Za-z+]+?)(?:\?|$)", url)
        if match:
            return match.group(1).replace("+", " ")

        return None
