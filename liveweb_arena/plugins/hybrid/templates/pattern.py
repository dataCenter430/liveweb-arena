"""Time Series Pattern - Multi-timeframe pattern matching across assets"""

import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig, GroundTruthResult,
)
from liveweb_arena.core.gt_collector import GTSourceType
from ..utils import get_crypto_24h_change, get_stooq_24h_change, retry_with_backoff


class PatternType(Enum):
    """Types of patterns to detect"""
    RECOVERY = "recovery"  # Down in short term, up in long term
    MOMENTUM = "momentum"  # Consistent direction across timeframes
    REVERSAL = "reversal"  # Recent change opposite to earlier trend
    DIVERGENCE = "divergence"  # Asset moving opposite to its peers


@dataclass
class AssetSpec:
    """Specification for a tradeable asset"""
    asset_id: str
    name: str
    source: str
    symbol: str


# Crypto assets (CoinGecko - has 24h and 7d data)
CRYPTO_ASSETS = [
    AssetSpec("bitcoin", "Bitcoin", "coingecko", ""),
    AssetSpec("ethereum", "Ethereum", "coingecko", ""),
    AssetSpec("solana", "Solana", "coingecko", ""),
    AssetSpec("binancecoin", "BNB", "coingecko", ""),
    AssetSpec("ripple", "XRP", "coingecko", ""),
    AssetSpec("cardano", "Cardano", "coingecko", ""),
    AssetSpec("dogecoin", "Dogecoin", "coingecko", ""),
    AssetSpec("avalanche-2", "Avalanche", "coingecko", ""),
    AssetSpec("polkadot", "Polkadot", "coingecko", ""),
    AssetSpec("chainlink", "Chainlink", "coingecko", ""),
]

# Stock assets (Stooq - 24h only, but we can compare patterns across assets)
STOCK_ASSETS = [
    AssetSpec("aapl.us", "Apple", "stooq", "aapl.us"),
    AssetSpec("msft.us", "Microsoft", "stooq", "msft.us"),
    AssetSpec("googl.us", "Google", "stooq", "googl.us"),
    AssetSpec("nvda.us", "NVIDIA", "stooq", "nvda.us"),
    AssetSpec("tsla.us", "Tesla", "stooq", "tsla.us"),
    AssetSpec("amzn.us", "Amazon", "stooq", "amzn.us"),
]

# Pattern definitions with criteria
PATTERNS = {
    PatternType.DIVERGENCE: {
        "name": "divergence",
        "description": "moving opposite to the majority of its peer group",
        "question": (
            "Among {assets}, find any asset showing a DIVERGENCE pattern:\n"
            "- Asset's 24h change is OPPOSITE direction from most others\n"
            "- Asset stands out from its peer group behavior\n\n"
            "Which asset(s) are diverging? Report their change and the group trend."
        ),
    },
    PatternType.MOMENTUM: {
        "name": "momentum",
        "description": "showing strongest directional movement in its group",
        "question": (
            "Check {assets} for MOMENTUM patterns:\n"
            "- Identify the asset with strongest absolute 24h change\n"
            "- Must be > 1.5x the group average movement\n\n"
            "Which asset shows momentum? Report the value and comparison to average."
        ),
    },
    PatternType.REVERSAL: {
        "name": "reversal",
        "description": "showing signs of trend reversal compared to peers",
        "question": (
            "Analyze {assets} for potential REVERSAL signals:\n"
            "- Find assets that recently changed direction vs group trend\n"
            "- Asset positive while group average is negative, or vice versa\n\n"
            "Report any reversal candidates with supporting data."
        ),
    },
}


@register_template("hybrid_time_series_pattern")
class HybridTimeSeriesPatternTemplate(QuestionTemplate):
    """
    Pattern detection task across multiple assets.

    Since multi-timeframe data is limited, this template focuses on:
    - Cross-asset pattern detection (divergence, momentum, reversal)
    - Comparing behavior within a peer group
    - Statistical reasoning about group behavior

    RL-friendly features:
    - Must collect ALL asset data to detect patterns
    - Pattern recognition is a learnable skill
    - Partial credit for each correct pattern found
    - No fixed answer - depends on real-time data

    Scoring:
    - Score per pattern correctly identified
    - Penalized for false pattern claims
    - Requires correct reasoning about WHY it's a pattern
    """

    GT_SOURCE = GTSourceType.API_ONLY

    def __init__(self):
        super().__init__("hybrid_time_series_pattern")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a pattern detection task."""
        rng = random.Random(seed)

        # Select pattern type
        pattern_type = rng.choice(list(PATTERNS.keys()))
        pattern_info = PATTERNS[pattern_type]

        # Select assets (4-5 from same category for clearer patterns)
        # Use crypto for more volatility
        num_assets = rng.randint(4, 5)
        selected_assets = rng.sample(CRYPTO_ASSETS, num_assets)
        rng.shuffle(selected_assets)

        asset_names = [a.name for a in selected_assets]
        assets_str = ", ".join(asset_names)

        question_text = pattern_info["question"].format(assets=assets_str)

        start_url = "https://www.coingecko.com/"

        validation_info = {
            "pattern_type": pattern_type.value,
            "pattern_description": pattern_info["description"],
            "assets": [
                {
                    "asset_id": a.asset_id,
                    "name": a.name,
                    "source": a.source,
                    "symbol": a.symbol,
                }
                for a in selected_assets
            ],
            "asset_names": asset_names,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"assets": selected_assets, "pattern_type": pattern_type},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=num_assets * 2 + 3,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        asset_names = validation_info.get("asset_names", [])
        pattern_type = validation_info.get("pattern_type", "")
        pattern_desc = validation_info.get("pattern_description", "")
        return f"""Task-Specific Rules (Hybrid - Time Series Pattern):
- Assets to analyze: {', '.join(asset_names)}
- Pattern type: {pattern_type} - {pattern_desc}
- Must check ALL assets before determining pattern
- Score based on correct pattern identification + reasoning
- Say "No pattern" if criteria not met
- Provide supporting data (actual values) for claims"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        """Detect patterns in the asset data."""
        assets = validation_info.get("assets", [])
        pattern_type = validation_info.get("pattern_type", "")
        if not assets:
            return GroundTruthResult.fail("No assets provided")

        results = []
        errors = []

        for asset in assets:
            source = asset["source"]
            asset_id = asset["asset_id"]
            name = asset["name"]
            symbol = asset.get("symbol", "")

            try:
                if source == "coingecko":
                    change = await get_crypto_24h_change(asset_id)
                else:
                    change = await get_stooq_24h_change(symbol)

                results.append({"name": name, "change": change})
            except RuntimeError as e:
                errors.append(f"{name}: {str(e)}")
            except Exception as e:
                errors.append(f"{name}: {str(e)}")

        if errors:
            return GroundTruthResult.retry(f"Could not fetch all data: {'; '.join(errors)}")

        if len(results) < 3:
            return GroundTruthResult.fail("Insufficient data for pattern detection")

        # Detect pattern based on type
        if pattern_type == "divergence":
            pattern_assets = self._detect_divergence(results)
        elif pattern_type == "momentum":
            pattern_assets = self._detect_momentum(results)
        elif pattern_type == "reversal":
            pattern_assets = self._detect_reversal(results)
        else:
            pattern_assets = []

        # Calculate group statistics
        changes = [r["change"] for r in results]
        avg_change = sum(changes) / len(changes)
        avg_abs = sum(abs(c) for c in changes) / len(changes)
        num_positive = sum(1 for c in changes if c > 0)
        num_negative = sum(1 for c in changes if c < 0)

        if pattern_assets:
            pattern_str = "; ".join([
                f"{p['name']}({p['change']:+.2f}%: {p['reason']})"
                for p in pattern_assets
            ])
        else:
            pattern_str = "None detected"

        all_str = ", ".join([f"{r['name']}={r['change']:+.2f}%" for r in results])
        group_str = f"avg={avg_change:+.2f}%, pos={num_positive}, neg={num_negative}"

        gt_str = (
            f"Pattern ({pattern_type}): [{pattern_str}] | "
            f"Group: {group_str} | "
            f"All: {all_str}"
        )

        return GroundTruthResult.ok(gt_str)

    def _detect_divergence(self, results: List[Dict]) -> List[Dict]:
        """Find assets moving opposite to majority."""
        changes = [r["change"] for r in results]
        num_positive = sum(1 for c in changes if c > 0)
        num_negative = sum(1 for c in changes if c < 0)

        # Need clear majority (at least 60%)
        total = len(changes)
        majority_threshold = total * 0.6

        divergent = []
        if num_positive >= majority_threshold:
            # Majority positive - find negatives
            for r in results:
                if r["change"] < -0.5:  # Meaningfully negative
                    divergent.append({
                        "name": r["name"],
                        "change": r["change"],
                        "reason": f"down while {num_positive}/{total} are up",
                    })
        elif num_negative >= majority_threshold:
            # Majority negative - find positives
            for r in results:
                if r["change"] > 0.5:  # Meaningfully positive
                    divergent.append({
                        "name": r["name"],
                        "change": r["change"],
                        "reason": f"up while {num_negative}/{total} are down",
                    })

        return divergent

    def _detect_momentum(self, results: List[Dict]) -> List[Dict]:
        """Find assets with strongest directional movement."""
        changes = [abs(r["change"]) for r in results]
        avg_abs = sum(changes) / len(changes) if changes else 0

        # Find assets with > 1.5x average movement
        threshold = avg_abs * 1.5
        momentum_assets = []

        for r in results:
            if abs(r["change"]) > threshold and threshold > 0.5:
                momentum_assets.append({
                    "name": r["name"],
                    "change": r["change"],
                    "reason": f"|{abs(r['change']):.1f}%| > 1.5x avg |{avg_abs:.1f}%|",
                })

        return momentum_assets

    def _detect_reversal(self, results: List[Dict]) -> List[Dict]:
        """Find assets potentially reversing vs group trend."""
        changes = [r["change"] for r in results]
        avg_change = sum(changes) / len(changes) if changes else 0

        # Find assets opposite to group average direction
        reversal_assets = []

        if abs(avg_change) > 0.5:  # Group has meaningful trend
            for r in results:
                # Asset opposite to group and meaningfully so
                if avg_change > 0.5 and r["change"] < -0.5:
                    reversal_assets.append({
                        "name": r["name"],
                        "change": r["change"],
                        "reason": f"negative while group avg is +{avg_change:.1f}%",
                    })
                elif avg_change < -0.5 and r["change"] > 0.5:
                    reversal_assets.append({
                        "name": r["name"],
                        "change": r["change"],
                        "reason": f"positive while group avg is {avg_change:.1f}%",
                    })

        return reversal_assets

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate pattern detection."""
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
        pattern_type = validation_info.get("pattern_type", "")

        # Parse GT patterns
        gt_patterns = self._parse_gt_patterns(ground_truth)
        gt_pattern_names = set(p.lower() for p in gt_patterns)

        # Check if GT indicates no pattern
        no_pattern_gt = "none detected" in ground_truth.lower()

        # Check if agent claims no pattern
        no_pattern_answer = any(phrase in answer_lower for phrase in [
            "no pattern", "none found", "no divergence", "no momentum",
            "no reversal", "does not show", "doesn't show",
        ])

        # Case 1: No pattern exists
        if no_pattern_gt:
            if no_pattern_answer:
                return ValidationResult(
                    score=1.0,
                    is_correct=True,
                    expected=ground_truth,
                    actual=answer,
                    details="Correctly identified no pattern",
                )
            else:
                # Agent claimed pattern when none exists
                return ValidationResult(
                    score=0.0,
                    is_correct=False,
                    expected=ground_truth,
                    actual=answer,
                    details="Claimed pattern when none exists",
                )

        # Case 2: Pattern exists
        asset_names = [a.lower() for a in validation_info.get("asset_names", [])]
        reported_patterns = self._parse_reported_patterns(answer_lower, asset_names)

        if no_pattern_answer and gt_pattern_names:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Missed patterns: {gt_pattern_names}",
            )

        # Calculate match score
        if not gt_pattern_names:
            score = 0.0 if reported_patterns else 1.0
        elif not reported_patterns:
            score = 0.0
        else:
            true_positives = gt_pattern_names & reported_patterns
            precision = len(true_positives) / len(reported_patterns)
            recall = len(true_positives) / len(gt_pattern_names)

            if precision + recall > 0:
                score = 2 * precision * recall / (precision + recall)
            else:
                score = 0.0

        details = f"GT patterns: {gt_pattern_names or 'none'}, Reported: {reported_patterns or 'none'}"

        return ValidationResult(
            score=score,
            is_correct=score >= 0.5,
            expected=ground_truth,
            actual=answer,
            details=details,
        )

    def _parse_gt_patterns(self, ground_truth: str) -> List[str]:
        """Parse pattern asset names from GT."""
        import re
        match = re.search(r"Pattern \(\w+\):\s*\[([^\]]+)\]", ground_truth)
        if not match:
            return []

        patterns_str = match.group(1)
        if "none detected" in patterns_str.lower():
            return []

        # Extract names before parentheses
        names = re.findall(r"(\w+)\s*\(", patterns_str)
        return names

    def _parse_reported_patterns(self, answer: str, valid_names: List[str]) -> set:
        """Extract pattern asset names from agent's answer."""
        import re
        reported = set()

        # Build variations
        variations = {
            "bitcoin": ["btc"], "ethereum": ["eth"], "solana": ["sol"],
            "bnb": ["binance"], "xrp": ["ripple"], "cardano": ["ada"],
            "dogecoin": ["doge"], "avalanche": ["avax"], "polkadot": ["dot"],
            "chainlink": ["link"],
        }

        for name in valid_names:
            name_lower = name.lower()
            # Check if appears in pattern context
            if self._appears_as_pattern(answer, name_lower):
                reported.add(name_lower)
                continue

            if name_lower in variations:
                for var in variations[name_lower]:
                    if self._appears_as_pattern(answer, var):
                        reported.add(name_lower)
                        break

        return reported

    def _appears_as_pattern(self, answer: str, name: str) -> bool:
        """Check if name appears in pattern context."""
        import re
        patterns = [
            rf"{name}.*(?:diverge|diverging|divergent|momentum|reversal|reversing)",
            rf"(?:diverge|diverging|momentum|reversal|reversing|pattern|shows?).*{name}",
            rf"{name}.*(?:opposite|against|contrary|different)",
            rf"{name}.*(?:strong|strongest|significant)",
        ]
        for pattern in patterns:
            if re.search(pattern, answer.lower()):
                return True
        return False

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger on CoinGecko coin page visit."""
        trigger = UrlPatternTrigger(domains=["coingecko.com"])
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)

    @classmethod
    def get_cache_source(cls) -> str:
        return "hybrid"

    def get_api_fields(self):
        return ["24h_change", "pattern_detection", "group_statistics"]

    # === Step-wise Reward Interface ===

    def get_target_assets(self, validation_info: Dict[str, Any]) -> set:
        """Return all assets - pattern detection needs all data."""
        assets = validation_info.get("assets", [])
        return {a["asset_id"] for a in assets}

    def get_required_domains(self, validation_info: Dict[str, Any]) -> set:
        """Primarily CoinGecko for crypto patterns."""
        return {"coingecko.com"}

    def get_reward_overrides(self) -> Optional[Dict[str, float]]:
        """All assets important for pattern detection."""
        return {
            "target_asset_reward": 0.20,
            "all_targets_bonus": 0.35,
        }
