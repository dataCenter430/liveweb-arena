"""Anomaly Detection - Statistical outlier finding with F1-score validation"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig, GroundTruthResult,
)
from liveweb_arena.core.gt_collector import GTSourceType
from ..utils import get_crypto_24h_change, get_stooq_24h_change


@dataclass
class AssetSpec:
    """Specification for a tradeable asset"""
    asset_id: str
    name: str
    source: str
    symbol: str


# Stable crypto assets
CRYPTO_ASSETS = [
    AssetSpec("bitcoin", "Bitcoin", "coingecko", ""),
    AssetSpec("ethereum", "Ethereum", "coingecko", ""),
    AssetSpec("solana", "Solana", "coingecko", ""),
    AssetSpec("binancecoin", "BNB", "coingecko", ""),
    AssetSpec("ripple", "XRP", "coingecko", ""),
    AssetSpec("cardano", "Cardano", "coingecko", ""),
    AssetSpec("dogecoin", "Dogecoin", "coingecko", ""),
    AssetSpec("avalanche-2", "Avalanche", "coingecko", ""),
]

# Stable stock assets
STOCK_ASSETS = [
    AssetSpec("aapl.us", "Apple", "stooq", "aapl.us"),
    AssetSpec("msft.us", "Microsoft", "stooq", "msft.us"),
    AssetSpec("googl.us", "Google", "stooq", "googl.us"),
    AssetSpec("nvda.us", "NVIDIA", "stooq", "nvda.us"),
    AssetSpec("tsla.us", "Tesla", "stooq", "tsla.us"),
    AssetSpec("amzn.us", "Amazon", "stooq", "amzn.us"),
    AssetSpec("meta.us", "Meta", "stooq", "meta.us"),
]


@register_template("hybrid_anomaly_detection")
class HybridAnomalyDetectionTemplate(QuestionTemplate):
    """
    Anomaly detection task requiring statistical reasoning.

    The agent must:
    1. Collect 24h change data for all listed assets
    2. Calculate the average change
    3. Identify anomalies based on criteria:
       - Change more than 2x the average magnitude
       - OR change in opposite direction from majority

    RL-friendly features:
    - Must explore ALL assets to compute average (no shortcuts)
    - Open-ended result (0 to N anomalies)
    - F1-score validation (precision + recall)
    - Penalizes both false positives and false negatives

    Scoring (F1-score):
    - Precision: correct_anomalies / reported_anomalies
    - Recall: correct_anomalies / gt_anomalies
    - Score = 2 * P * R / (P + R)
    """

    GT_SOURCE = GTSourceType.API_ONLY

    # Multiplier threshold for magnitude anomaly
    MAGNITUDE_MULTIPLIER = 2.0

    PATTERNS = [
        (
            "Among these assets ({assets}), find any that show anomalous 24h performance:\n"
            "- Change magnitude MORE than 2x the group average\n"
            "- OR change direction OPPOSITE from the majority\n\n"
            "Report all anomalies with their values and why they're anomalous."
        ),
        (
            "Analyze these assets for outliers: {assets}.\n"
            "An anomaly is defined as:\n"
            "1. 24h change > 2x the average absolute change, OR\n"
            "2. Moving opposite to most other assets\n\n"
            "List any anomalies found with explanation."
        ),
        (
            "Check {assets} for unusual behavior today.\n"
            "Flag as anomaly if:\n"
            "- Performance magnitude exceeds 2x group average\n"
            "- Direction differs from majority trend\n\n"
            "Report anomalies and reasoning. Say 'None' if no anomalies."
        ),
    ]

    def __init__(self):
        super().__init__("hybrid_anomaly_detection")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate an anomaly detection task with 5 assets."""
        rng = random.Random(seed)

        # Select 2-3 crypto + 2-3 stocks (total 5)
        num_crypto = rng.randint(2, 3)
        num_stocks = 5 - num_crypto

        selected_crypto = rng.sample(CRYPTO_ASSETS, num_crypto)
        selected_stocks = rng.sample(STOCK_ASSETS, num_stocks)

        all_assets = selected_crypto + selected_stocks
        rng.shuffle(all_assets)

        asset_names = [a.name for a in all_assets]
        assets_str = ", ".join(asset_names)

        pattern = rng.choice(self.PATTERNS)
        question_text = pattern.format(assets=assets_str)

        start_url = "https://www.coingecko.com/"

        validation_info = {
            "assets": [
                {
                    "asset_id": a.asset_id,
                    "name": a.name,
                    "source": a.source,
                    "symbol": a.symbol,
                }
                for a in all_assets
            ],
            "asset_names": asset_names,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"assets": all_assets},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=len(all_assets) * 2 + 3,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        asset_names = validation_info.get("asset_names", [])
        return f"""Task-Specific Rules (Hybrid - Anomaly Detection):
- Check all assets: {', '.join(asset_names)}
- Anomaly criteria:
  1. |change| > 2x average |change| of all assets
  2. OR change direction opposite from majority
- Score uses F1-score (precision * recall harmonic mean)
- Penalized for both missing anomalies and false alarms
- If no anomalies exist, report "None" or "No anomalies"
- Must check ALL assets before concluding"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        """Fetch all changes and compute anomalies."""
        assets = validation_info.get("assets", [])
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
            return GroundTruthResult.fail("Insufficient data for anomaly detection")

        # Calculate anomalies
        anomalies = self._find_anomalies(results)

        if anomalies:
            anomaly_str = "; ".join([
                f"{a['name']}({a['change']:+.2f}%, reason: {a['reason']})"
                for a in anomalies
            ])
        else:
            anomaly_str = "None"

        all_str = ", ".join([f"{r['name']}={r['change']:+.2f}%" for r in results])
        avg_abs = sum(abs(r["change"]) for r in results) / len(results)

        gt_str = (
            f"Anomalies: [{anomaly_str}] | "
            f"Count: {len(anomalies)} | "
            f"Avg|change|: {avg_abs:.2f}% | "
            f"All: {all_str}"
        )

        return GroundTruthResult.ok(gt_str)

    def _find_anomalies(self, results: List[Dict]) -> List[Dict]:
        """Find anomalies in the results."""
        anomalies = []

        # Calculate statistics
        changes = [r["change"] for r in results]
        avg_abs = sum(abs(c) for c in changes) / len(changes)
        threshold = avg_abs * self.MAGNITUDE_MULTIPLIER

        # Count positive vs negative
        num_positive = sum(1 for c in changes if c > 0)
        num_negative = sum(1 for c in changes if c < 0)
        majority_positive = num_positive > num_negative

        for r in results:
            reasons = []
            change = r["change"]

            # Check magnitude anomaly
            if abs(change) > threshold and threshold > 0.5:  # Avoid false positives when all small
                reasons.append(f"magnitude {abs(change):.1f}% > 2x avg {avg_abs:.1f}%")

            # Check direction anomaly
            is_positive = change > 0
            if majority_positive and not is_positive and num_positive >= 3:
                reasons.append(f"down while {num_positive}/{len(results)} are up")
            elif not majority_positive and is_positive and num_negative >= 3:
                reasons.append(f"up while {num_negative}/{len(results)} are down")

            if reasons:
                anomalies.append({
                    "name": r["name"],
                    "change": change,
                    "reason": " & ".join(reasons),
                })

        return anomalies

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate using F1-score."""
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

        # Parse GT anomalies
        gt_anomalies = self._parse_gt_anomalies(ground_truth)
        gt_anomaly_names = set(a.lower() for a in gt_anomalies)

        # Parse agent's claimed anomalies
        asset_names = [a.lower() for a in validation_info.get("asset_names", [])]
        reported_anomalies = self._parse_reported_anomalies(answer_lower, asset_names)

        # Calculate F1 score
        if not gt_anomaly_names and not reported_anomalies:
            # No anomalies exist and agent correctly said none
            if any(word in answer_lower for word in ["none", "no anomal", "no outlier"]):
                return ValidationResult(
                    score=1.0,
                    is_correct=True,
                    expected=ground_truth,
                    actual=answer,
                    details="Correctly identified no anomalies",
                )
            else:
                return ValidationResult(
                    score=0.5,
                    is_correct=False,
                    expected=ground_truth,
                    actual=answer,
                    details="No anomalies exist, but answer unclear",
                )

        if not gt_anomaly_names:
            # No GT anomalies but agent reported some = false positives
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"False positives: reported {reported_anomalies} but no anomalies exist",
            )

        if not reported_anomalies:
            # GT has anomalies but agent found none
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Missed all anomalies: {gt_anomaly_names}",
            )

        # Calculate precision and recall
        true_positives = gt_anomaly_names & reported_anomalies
        precision = len(true_positives) / len(reported_anomalies) if reported_anomalies else 0
        recall = len(true_positives) / len(gt_anomaly_names) if gt_anomaly_names else 0

        if precision + recall > 0:
            f1 = 2 * precision * recall / (precision + recall)
        else:
            f1 = 0.0

        details = (
            f"TP={len(true_positives)}, P={precision:.2f}, R={recall:.2f}, F1={f1:.2f} | "
            f"GT: {gt_anomaly_names} | Reported: {reported_anomalies}"
        )

        return ValidationResult(
            score=f1,
            is_correct=f1 >= 0.5,
            expected=ground_truth,
            actual=answer,
            details=details,
        )

    def _parse_gt_anomalies(self, ground_truth: str) -> List[str]:
        """Parse anomaly names from GT string."""
        import re
        # Format: "Anomalies: [Name1(...); Name2(...)] | ..."
        match = re.search(r"Anomalies:\s*\[([^\]]+)\]", ground_truth)
        if not match:
            return []

        anomalies_str = match.group(1)
        if anomalies_str.lower() == "none":
            return []

        # Extract names before the parentheses
        names = re.findall(r"(\w+)\s*\(", anomalies_str)
        return names

    def _parse_reported_anomalies(self, answer: str, valid_names: List[str]) -> set:
        """Extract anomaly names reported by agent."""
        reported = set()

        # Build name variations
        variations = {
            "bitcoin": ["btc"], "ethereum": ["eth"], "solana": ["sol"],
            "bnb": ["binance"], "xrp": ["ripple"], "cardano": ["ada"],
            "dogecoin": ["doge"], "avalanche": ["avax"], "polkadot": ["dot"],
            "apple": ["aapl"], "microsoft": ["msft"], "google": ["googl"],
            "nvidia": ["nvda"], "tesla": ["tsla"], "amazon": ["amzn"],
            "meta": ["facebook"],
        }

        for name in valid_names:
            name_lower = name.lower()
            # Check if this name appears in anomaly context
            if self._appears_as_anomaly(answer, name_lower):
                reported.add(name_lower)
                continue

            # Check variations
            if name_lower in variations:
                for var in variations[name_lower]:
                    if self._appears_as_anomaly(answer, var):
                        reported.add(name_lower)
                        break

        return reported

    def _appears_as_anomaly(self, answer: str, name: str) -> bool:
        """Check if name appears in anomaly context."""
        import re
        # Look for patterns like "Bitcoin is an anomaly" or "anomaly: Bitcoin"
        patterns = [
            rf"{name}.*(?:anomal|outlier|unusual|abnormal)",
            rf"(?:anomal|outlier).*{name}",
            rf"{name}.*(?:2x|twice|double|opposite|against)",
        ]
        for pattern in patterns:
            if re.search(pattern, answer.lower()):
                return True

        # Also check if name is in a list context after "anomalies:" or "outliers:"
        list_match = re.search(r"(?:anomal|outlier)[^:]*:\s*([^\.]+)", answer.lower())
        if list_match and name in list_match.group(1):
            return True

        return False

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger on Stooq visit (cross-site task)."""
        trigger = UrlPatternTrigger(domains=["stooq.com"])
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)

    @classmethod
    def get_cache_source(cls) -> str:
        return "hybrid"

    def get_api_fields(self):
        return ["24h_change", "anomaly_detection", "statistics"]

    # === Step-wise Reward Interface ===

    def get_target_assets(self, validation_info: Dict[str, Any]) -> set:
        """Return all assets - must check all to find anomalies."""
        assets = validation_info.get("assets", [])
        return {a["asset_id"] for a in assets}

    def get_required_domains(self, validation_info: Dict[str, Any]) -> set:
        """Requires both CoinGecko and Stooq."""
        return {"coingecko.com", "stooq.com"}

    def get_reward_overrides(self) -> Optional[Dict[str, float]]:
        """All assets needed for statistical analysis."""
        return {
            "target_asset_reward": 0.20,
            "all_targets_bonus": 0.35,
        }
