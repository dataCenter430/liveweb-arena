"""Arbitrage Finder - Cross-site price comparison with percentage difference"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, CompositeTrigger, FetchStrategy, TriggerConfig, GroundTruthResult,
)
from liveweb_arena.core.gt_collector import GTSourceType
from ..utils import get_crypto_24h_change, retry_with_backoff


@dataclass
class AssetPair:
    """Specification for a cross-site price comparison"""
    name: str
    primary_source: str  # "coingecko" or "taostats"
    primary_id: str
    secondary_source: str
    secondary_id: str
    description: str


# Cross-site comparison pairs
# These are assets that appear on multiple data sources with different pricing
COMPARISON_PAIRS = [
    AssetPair(
        name="TAO",
        primary_source="coingecko",
        primary_id="bittensor",
        secondary_source="taostats",
        secondary_id="root",  # TAO itself on Taostats
        description="Compare TAO price on CoinGecko vs implied TAO price from Taostats subnet data",
    ),
    AssetPair(
        name="Bitcoin",
        primary_source="coingecko",
        primary_id="bitcoin",
        secondary_source="stooq",
        secondary_id="btc.v",  # Bitcoin on Stooq
        description="Compare Bitcoin price on CoinGecko vs Stooq",
    ),
    AssetPair(
        name="Ethereum",
        primary_source="coingecko",
        primary_id="ethereum",
        secondary_source="stooq",
        secondary_id="eth.v",  # Ethereum on Stooq
        description="Compare Ethereum price on CoinGecko vs Stooq",
    ),
]


@register_template("hybrid_arbitrage_finder")
class HybridArbitrageFinderTemplate(QuestionTemplate):
    """
    Cross-site arbitrage detection task.

    The agent must:
    1. Check price on primary source (e.g., CoinGecko)
    2. Check price on secondary source (e.g., Stooq)
    3. Identify which source shows higher price
    4. Calculate percentage difference

    RL-friendly features:
    - Exploration order affects efficiency
    - Partial credit: correct winner = 0.5, accurate diff = additional 0.5
    - Real-time data = not memorizable
    - Strategy learning: which source to check first?

    Scoring:
    - 0.5: Correctly identify higher/lower source
    - 1.0: Correct identification + percentage within 5% tolerance
    """

    GT_SOURCE = GTSourceType.API_ONLY

    PATTERNS = [
        (
            "Compare the {asset} price on {source1} with the price on {source2}. "
            "Which source shows a higher price, and what is the percentage difference?"
        ),
        (
            "Check {asset}'s current price on both {source1} and {source2}. "
            "Report which platform has the higher price and the % difference between them."
        ),
        (
            "Find any price discrepancy for {asset} between {source1} and {source2}. "
            "Which source quotes higher? What's the percentage spread?"
        ),
    ]

    def __init__(self):
        super().__init__("hybrid_arbitrage_finder")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate an arbitrage detection task."""
        rng = random.Random(seed)

        # Select a comparison pair
        pair = rng.choice(COMPARISON_PAIRS)

        # Randomly swap which source is mentioned first (doesn't affect GT)
        if rng.random() < 0.5:
            source1 = pair.primary_source.title()
            source2 = pair.secondary_source.title()
        else:
            source1 = pair.secondary_source.title()
            source2 = pair.primary_source.title()

        pattern = rng.choice(self.PATTERNS)
        question_text = pattern.format(
            asset=pair.name,
            source1=source1,
            source2=source2,
        )

        # Start at CoinGecko (most common primary source)
        start_url = "https://www.coingecko.com/"

        validation_info = {
            "asset_name": pair.name,
            "primary_source": pair.primary_source,
            "primary_id": pair.primary_id,
            "secondary_source": pair.secondary_source,
            "secondary_id": pair.secondary_id,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"pair": pair},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=8,  # Visit 2 sites + navigation
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        asset = validation_info.get("asset_name", "")
        primary = validation_info.get("primary_source", "").title()
        secondary = validation_info.get("secondary_source", "").title()
        return f"""Task-Specific Rules (Hybrid - Arbitrage Finder):
- Compare {asset} price on {primary} vs {secondary}
- Identify which source shows higher price
- Calculate percentage difference: (higher - lower) / lower * 100
- Score breakdown:
  - 0.5: Correctly identify which source is higher
  - 1.0: Correct identification + percentage within 5pp
- Output format: "{primary}/{secondary} is higher by X.X%"
"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        """Fetch prices from both sources and compare."""
        from liveweb_arena.core.gt_collector import get_current_gt_collector

        primary_source = validation_info.get("primary_source", "")
        primary_id = validation_info.get("primary_id", "")
        secondary_source = validation_info.get("secondary_source", "")
        secondary_id = validation_info.get("secondary_id", "")
        asset_name = validation_info.get("asset_name", "")

        errors = []

        # Get primary price
        try:
            primary_price = await self._get_price(primary_source, primary_id)
        except Exception as e:
            errors.append(f"{primary_source}: {e}")
            primary_price = None

        # Get secondary price
        try:
            secondary_price = await self._get_price(secondary_source, secondary_id)
        except Exception as e:
            errors.append(f"{secondary_source}: {e}")
            secondary_price = None

        if errors:
            return GroundTruthResult.retry(f"Could not fetch prices: {'; '.join(errors)}")

        if primary_price is None or secondary_price is None:
            return GroundTruthResult.fail("Missing price data")

        # Calculate difference
        if primary_price > secondary_price:
            higher_source = primary_source
            lower_source = secondary_source
            higher_price = primary_price
            lower_price = secondary_price
        else:
            higher_source = secondary_source
            lower_source = primary_source
            higher_price = secondary_price
            lower_price = primary_price

        diff_pct = ((higher_price - lower_price) / lower_price) * 100 if lower_price > 0 else 0

        gt_str = (
            f"Higher: {higher_source.title()} (${higher_price:,.2f}) | "
            f"Lower: {lower_source.title()} (${lower_price:,.2f}) | "
            f"Difference: {diff_pct:.2f}%"
        )

        return GroundTruthResult.ok(gt_str)

    async def _get_price(self, source: str, asset_id: str) -> Optional[float]:
        """Get price from specified source."""
        from liveweb_arena.core.gt_collector import get_current_gt_collector
        from liveweb_arena.plugins.coingecko.api_client import CoinGeckoClient
        from liveweb_arena.plugins.stooq.api_client import StooqClient

        gt_collector = get_current_gt_collector()

        if source == "coingecko":
            # Try collected data first
            if gt_collector is not None:
                api_data = gt_collector.get_collected_api_data()
                if asset_id in api_data:
                    price = api_data[asset_id].get("current_price")
                    if price is not None:
                        return float(price)
                    raise ValueError(f"Price missing for {asset_id}")
                if api_data:  # Some data collected but not this asset
                    raise RuntimeError(f"Agent did not visit CoinGecko page for '{asset_id}'")

            # Live mode fallback
            data = await CoinGeckoClient.get_coin_market_data(asset_id)
            if data and len(data) > 0:
                return float(data[0].get("current_price", 0))
            return None

        elif source == "stooq":
            # Try collected data first
            if gt_collector is not None:
                api_data = gt_collector.get_collected_api_data()
                if asset_id in api_data:
                    price = api_data[asset_id].get("close")
                    if price is not None:
                        return float(price)
                    raise ValueError(f"Price missing for {asset_id}")
                if api_data:
                    raise RuntimeError(f"Agent did not visit Stooq page for '{asset_id}'")

            # Live mode fallback
            data = await StooqClient.get_price_data(asset_id)
            if data:
                return float(data.get("close", 0))
            return None

        elif source == "taostats":
            # Taostats subnet data - price is in TAO, need to convert to USD
            from liveweb_arena.plugins.taostats.api_client import fetch_single_subnet_data

            if gt_collector is not None:
                api_data = gt_collector.get_collected_api_data()
                # Taostats stores data by subnet ID
                if "taostats" in api_data and "subnets" in api_data.get("taostats", {}):
                    subnets = api_data["taostats"]["subnets"]
                    # For root network, the "price" is TAO price in USD
                    if asset_id in subnets:
                        price = subnets[asset_id].get("price", 0)
                        return float(price) if price else None

            # Live mode fallback
            data = await fetch_single_subnet_data(asset_id)
            if data:
                return float(data.get("price", 0))
            return None

        return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate arbitrage detection with partial credit."""
        import re

        result = await self.get_ground_truth(validation_info)

        if not result.success:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details=f"Ground truth unavailable: {result.error}",
            )

        ground_truth = result.value
        answer_lower = answer.lower()

        # Parse GT for expected values
        higher_match = re.search(r"Higher:\s*(\w+)", ground_truth)
        diff_match = re.search(r"Difference:\s*([\d.]+)%", ground_truth)

        if not higher_match or not diff_match:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not parse ground truth",
            )

        expected_higher = higher_match.group(1).lower()
        expected_diff = float(diff_match.group(1))

        # Check if agent identified correct higher source
        source_variations = {
            "coingecko": ["coingecko", "coin gecko", "cg"],
            "stooq": ["stooq"],
            "taostats": ["taostats", "tao stats"],
        }

        # Find which source agent claims is higher
        agent_higher = None
        for source, variations in source_variations.items():
            for var in variations:
                if var in answer_lower:
                    # Check if this source is mentioned as "higher"
                    idx = answer_lower.find(var)
                    context = answer_lower[max(0, idx-30):idx+30+len(var)]
                    if any(word in context for word in ["higher", "more", "greater", "above"]):
                        agent_higher = source
                        break
            if agent_higher:
                break

        # Also check for pattern like "Source X shows higher price"
        if not agent_higher:
            for source, variations in source_variations.items():
                for var in variations:
                    pattern = rf"{var}.*(?:higher|more|greater)"
                    if re.search(pattern, answer_lower):
                        agent_higher = source
                        break

        # Check higher source correctness
        higher_correct = agent_higher == expected_higher if agent_higher else False

        # Extract percentage from answer
        actual_diff = None
        diff_patterns = [
            r"([\d.]+)\s*%?\s*(?:difference|diff|spread|gap)",
            r"(?:difference|diff|spread|gap)\s*(?:of|is|:)?\s*([\d.]+)\s*%?",
            r"by\s*([\d.]+)\s*%",
            r"([\d.]+)\s*%\s*(?:higher|more|greater)",
        ]
        for pattern in diff_patterns:
            match = re.search(pattern, answer_lower)
            if match:
                actual_diff = float(match.group(1))
                break

        # Calculate score
        if higher_correct:
            if actual_diff is not None and abs(actual_diff - expected_diff) <= 5.0:
                score = 1.0
                details = f"Correct source and difference ({actual_diff:.1f}% vs {expected_diff:.1f}%)"
            else:
                score = 0.5
                if actual_diff is not None:
                    details = f"Correct source, but difference off ({actual_diff:.1f}% vs {expected_diff:.1f}%)"
                else:
                    details = "Correct source, but could not extract percentage"
        else:
            score = 0.0
            details = f"Wrong higher source (expected {expected_higher})"

        return ValidationResult(
            score=score,
            is_correct=score >= 0.5,
            expected=ground_truth,
            actual=answer,
            details=details,
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger on either source visit."""
        secondary = validation_info.get("secondary_source", "stooq")
        if secondary == "stooq":
            trigger = UrlPatternTrigger(domains=["stooq.com"])
        elif secondary == "taostats":
            trigger = UrlPatternTrigger(domains=["taostats.io"])
        else:
            trigger = UrlPatternTrigger(domains=["stooq.com", "taostats.io"])
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)

    @classmethod
    def get_cache_source(cls) -> str:
        return "hybrid"

    def get_api_fields(self):
        return ["price", "source_comparison", "percentage_diff"]

    # === Step-wise Reward Interface ===

    def get_target_assets(self, validation_info: Dict[str, Any]) -> set:
        """Return both primary and secondary asset IDs."""
        primary_id = validation_info.get("primary_id", "")
        secondary_id = validation_info.get("secondary_id", "")
        return {primary_id, secondary_id} - {""}  # Remove empty strings

    def get_required_domains(self, validation_info: Dict[str, Any]) -> set:
        """Domains depend on sources used."""
        domains = {"coingecko.com"}  # Always start here
        secondary = validation_info.get("secondary_source", "")
        if secondary == "stooq":
            domains.add("stooq.com")
        elif secondary == "taostats":
            domains.add("taostats.io")
        return domains

    def get_reward_overrides(self) -> Optional[Dict[str, float]]:
        """Two assets to compare - standard rewards."""
        return {
            "target_asset_reward": 0.30,
            "all_targets_bonus": 0.40,
        }
