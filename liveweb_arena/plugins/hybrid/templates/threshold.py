"""Threshold Alert - Multi-condition monitoring task with per-condition scoring"""

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
from ..utils import get_crypto_24h_change, get_stooq_24h_change, get_stooq_price


class ConditionType(Enum):
    """Types of threshold conditions"""
    PRICE_ABOVE = "price_above"
    PRICE_BELOW = "price_below"
    CHANGE_ABOVE = "change_above"
    CHANGE_BELOW = "change_below"


@dataclass
class AssetSpec:
    """Specification for a tradeable asset"""
    asset_id: str
    name: str
    source: str
    symbol: str


@dataclass
class ConditionSpec:
    """Specification for a threshold condition"""
    asset: AssetSpec
    condition_type: ConditionType
    threshold: float
    description: str


# Stable crypto assets
CRYPTO_ASSETS = [
    AssetSpec("bitcoin", "Bitcoin", "coingecko", ""),
    AssetSpec("ethereum", "Ethereum", "coingecko", ""),
    AssetSpec("solana", "Solana", "coingecko", ""),
    AssetSpec("binancecoin", "BNB", "coingecko", ""),
    AssetSpec("ripple", "XRP", "coingecko", ""),
    AssetSpec("cardano", "Cardano", "coingecko", ""),
    AssetSpec("dogecoin", "Dogecoin", "coingecko", ""),
]

# Stable stock assets
STOCK_ASSETS = [
    AssetSpec("aapl.us", "Apple", "stooq", "aapl.us"),
    AssetSpec("msft.us", "Microsoft", "stooq", "msft.us"),
    AssetSpec("googl.us", "Google", "stooq", "googl.us"),
    AssetSpec("nvda.us", "NVIDIA", "stooq", "nvda.us"),
    AssetSpec("tsla.us", "Tesla", "stooq", "tsla.us"),
    AssetSpec("amzn.us", "Amazon", "stooq", "amzn.us"),
]

# Threshold ranges for different condition types
CRYPTO_CHANGE_THRESHOLDS = [3.0, 4.0, 5.0, 6.0, 7.0]  # percentage
STOCK_CHANGE_THRESHOLDS = [2.0, 2.5, 3.0, 3.5, 4.0]  # percentage
STOCK_PRICE_THRESHOLDS = {
    "aapl.us": [150, 175, 200, 225, 250],
    "msft.us": [350, 400, 425, 450, 475],
    "googl.us": [150, 175, 200, 225],
    "nvda.us": [100, 125, 150, 175, 200],
    "tsla.us": [200, 250, 300, 350, 400],
    "amzn.us": [175, 200, 225, 250],
}


@register_template("hybrid_threshold_alert")
class HybridThresholdAlertTemplate(QuestionTemplate):
    """
    Multi-condition threshold monitoring task.

    The agent must check multiple conditions and report which are met:
    - Crypto/stock 24h change above/below threshold
    - Stock price above/below threshold

    RL-friendly features:
    - Can potentially terminate early if conditions found
    - Per-condition scoring (each correct = 1/N points)
    - Strategy learning: check most likely conditions first
    - Cross-site navigation required

    Scoring:
    - Each condition evaluated independently
    - Score = correct_evaluations / total_conditions
    - Partial credit for partial correctness
    """

    GT_SOURCE = GTSourceType.API_ONLY

    def __init__(self):
        super().__init__("hybrid_threshold_alert")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a threshold alert task with 3-4 conditions."""
        rng = random.Random(seed)

        num_conditions = rng.randint(3, 4)
        conditions = self._generate_conditions(rng, num_conditions)

        # Build question text
        conditions_text = "\n".join([
            f"{i+1}. {c.description}"
            for i, c in enumerate(conditions)
        ])

        patterns = [
            (
                "Check if any of these conditions are currently met:\n"
                f"{conditions_text}\n\n"
                "Report which conditions are TRUE and their actual values."
            ),
            (
                "Monitor these alerts:\n"
                f"{conditions_text}\n\n"
                "Which alerts are triggered? Include the actual values."
            ),
            (
                "Evaluate these threshold conditions:\n"
                f"{conditions_text}\n\n"
                "For each, state TRUE or FALSE with the current value."
            ),
        ]

        question_text = rng.choice(patterns)
        start_url = "https://www.coingecko.com/"

        validation_info = {
            "conditions": [
                {
                    "asset_id": c.asset.asset_id,
                    "asset_name": c.asset.name,
                    "source": c.asset.source,
                    "symbol": c.asset.symbol,
                    "condition_type": c.condition_type.value,
                    "threshold": c.threshold,
                    "description": c.description,
                }
                for c in conditions
            ],
            "num_conditions": len(conditions),
        }

        # Estimate steps: each condition might need 2 visits
        expected_steps = len(conditions) * 2 + 2

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"conditions": conditions},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=expected_steps,
        )

    def _generate_conditions(self, rng: random.Random, num: int) -> List[ConditionSpec]:
        """Generate a mix of conditions."""
        conditions = []
        used_assets = set()

        # Ensure mix of crypto and stock conditions
        min_crypto = max(1, num // 2)
        min_stock = max(1, num - min_crypto)

        # Generate crypto conditions (always 24h change)
        crypto_count = 0
        while crypto_count < min_crypto and len(conditions) < num:
            asset = rng.choice(CRYPTO_ASSETS)
            if asset.asset_id in used_assets:
                continue
            used_assets.add(asset.asset_id)

            threshold = rng.choice(CRYPTO_CHANGE_THRESHOLDS)
            is_above = rng.choice([True, False])

            if is_above:
                cond_type = ConditionType.CHANGE_ABOVE
                desc = f"{asset.name} 24h change > +{threshold}%"
            else:
                cond_type = ConditionType.CHANGE_BELOW
                desc = f"{asset.name} 24h change < -{threshold}%"

            conditions.append(ConditionSpec(asset, cond_type, threshold, desc))
            crypto_count += 1

        # Generate stock conditions (mix of price and change)
        stock_count = 0
        while stock_count < min_stock and len(conditions) < num:
            asset = rng.choice(STOCK_ASSETS)
            if asset.asset_id in used_assets:
                continue
            used_assets.add(asset.asset_id)

            # 50% price conditions, 50% change conditions
            if rng.random() < 0.5 and asset.symbol in STOCK_PRICE_THRESHOLDS:
                threshold = rng.choice(STOCK_PRICE_THRESHOLDS[asset.symbol])
                is_above = rng.choice([True, False])
                if is_above:
                    cond_type = ConditionType.PRICE_ABOVE
                    desc = f"{asset.name} price > ${threshold}"
                else:
                    cond_type = ConditionType.PRICE_BELOW
                    desc = f"{asset.name} price < ${threshold}"
            else:
                threshold = rng.choice(STOCK_CHANGE_THRESHOLDS)
                is_above = rng.choice([True, False])
                if is_above:
                    cond_type = ConditionType.CHANGE_ABOVE
                    desc = f"{asset.name} 24h change > +{threshold}%"
                else:
                    cond_type = ConditionType.CHANGE_BELOW
                    desc = f"{asset.name} 24h change < -{threshold}%"

            conditions.append(ConditionSpec(asset, cond_type, threshold, desc))
            stock_count += 1

        rng.shuffle(conditions)
        return conditions

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        conditions = validation_info.get("conditions", [])
        cond_list = "\n".join([f"  - {c['description']}" for c in conditions])
        return f"""Task-Specific Rules (Hybrid - Threshold Alert):
- Evaluate each condition independently:
{cond_list}
- Report TRUE/FALSE for each with actual value
- Score = correct_evaluations / total_conditions
- Partial credit for partially correct answers
- Must check actual data, not guess based on trends"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        """Evaluate all conditions and return ground truth."""
        conditions = validation_info.get("conditions", [])
        if not conditions:
            return GroundTruthResult.fail("No conditions provided")

        results = []
        errors = []

        for cond in conditions:
            source = cond["source"]
            asset_id = cond["asset_id"]
            asset_name = cond["asset_name"]
            symbol = cond.get("symbol", "")
            cond_type = cond["condition_type"]
            threshold = cond["threshold"]

            try:
                # Fetch the relevant value
                if cond_type in ["change_above", "change_below"]:
                    if source == "coingecko":
                        value = await get_crypto_24h_change(asset_id)
                    else:
                        value = await get_stooq_24h_change(symbol)
                    value_str = f"{value:+.2f}%"
                else:  # price conditions
                    value = await get_stooq_price(symbol)
                    value_str = f"${value:,.2f}"

                # Evaluate condition
                if cond_type == "change_above":
                    is_true = value > threshold
                elif cond_type == "change_below":
                    is_true = value < -threshold
                elif cond_type == "price_above":
                    is_true = value > threshold
                else:  # price_below
                    is_true = value < threshold

                results.append({
                    "description": cond["description"],
                    "asset_name": asset_name,
                    "is_true": is_true,
                    "actual_value": value,
                    "value_str": value_str,
                    "threshold": threshold,
                })

            except RuntimeError as e:
                errors.append(f"{asset_name}: {str(e)}")
            except Exception as e:
                errors.append(f"{asset_name}: {str(e)}")

        if errors:
            error_msg = "; ".join(errors)
            return GroundTruthResult.retry(f"Could not evaluate all conditions: {error_msg}")

        # Build ground truth string
        true_conditions = [r for r in results if r["is_true"]]
        false_conditions = [r for r in results if not r["is_true"]]

        true_str = ", ".join([
            f"{r['asset_name']}={r['value_str']}"
            for r in true_conditions
        ]) if true_conditions else "None"

        false_str = ", ".join([
            f"{r['asset_name']}={r['value_str']}"
            for r in false_conditions
        ]) if false_conditions else "None"

        gt_str = (
            f"TRUE: [{true_str}] | "
            f"FALSE: [{false_str}] | "
            f"Count: {len(true_conditions)}/{len(results)} met"
        )

        return GroundTruthResult.ok(gt_str)

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate per-condition accuracy."""
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
        conditions = validation_info.get("conditions", [])

        # Re-evaluate conditions to get detailed results
        # (In production, cache this from get_ground_truth)
        correct_count = 0
        total_count = len(conditions)
        evaluation_details = []

        for cond in conditions:
            asset_name = cond["asset_name"].lower()
            cond_type = cond["condition_type"]
            threshold = cond["threshold"]

            # Parse GT to find this condition's result
            is_gt_true = self._parse_condition_result(ground_truth, asset_name)

            # Parse answer to see if agent got it right
            agent_says_true = self._check_agent_answer(answer, asset_name, cond_type, threshold)

            if agent_says_true == is_gt_true:
                correct_count += 1
                evaluation_details.append(f"{asset_name}: correct")
            else:
                evaluation_details.append(
                    f"{asset_name}: wrong (GT={is_gt_true}, agent={agent_says_true})"
                )

        score = correct_count / total_count if total_count > 0 else 0.0
        is_correct = score >= 0.75  # At least 75% correct

        return ValidationResult(
            score=score,
            is_correct=is_correct,
            expected=ground_truth,
            actual=answer,
            details=f"Correct: {correct_count}/{total_count}. {'; '.join(evaluation_details)}",
        )

    def _parse_condition_result(self, ground_truth: str, asset_name: str) -> bool:
        """Check if asset is in TRUE or FALSE section of GT."""
        gt_lower = ground_truth.lower()
        asset_lower = asset_name.lower()

        # Find TRUE section
        true_start = gt_lower.find("true:")
        false_start = gt_lower.find("false:")

        if true_start == -1 or false_start == -1:
            return False

        true_section = gt_lower[true_start:false_start]
        return asset_lower in true_section

    def _check_agent_answer(
        self,
        answer: str,
        asset_name: str,
        cond_type: str,
        threshold: float
    ) -> Optional[bool]:
        """Determine what the agent claims about this condition."""
        import re
        answer_lower = answer.lower()
        asset_lower = asset_name.lower()

        # Look for explicit TRUE/FALSE statements
        # Pattern: "Asset: TRUE" or "Asset is true" or "Asset: met"
        patterns_true = [
            rf"{asset_lower}[:\s]+true",
            rf"{asset_lower}[:\s]+met",
            rf"{asset_lower}[:\s]+yes",
            rf"(?:condition|alert).*{asset_lower}.*(?:true|met|triggered)",
        ]
        patterns_false = [
            rf"{asset_lower}[:\s]+false",
            rf"{asset_lower}[:\s]+not met",
            rf"{asset_lower}[:\s]+no",
            rf"(?:condition|alert).*{asset_lower}.*(?:false|not met|not triggered)",
        ]

        for pattern in patterns_true:
            if re.search(pattern, answer_lower):
                return True
        for pattern in patterns_false:
            if re.search(pattern, answer_lower):
                return False

        # If no explicit statement, check if asset appears in "triggered" or "met" context
        if asset_lower in answer_lower:
            # Check surrounding context
            idx = answer_lower.find(asset_lower)
            context = answer_lower[max(0, idx-50):idx+50+len(asset_lower)]
            if any(word in context for word in ["true", "met", "triggered", "satisfied"]):
                return True
            if any(word in context for word in ["false", "not met", "not triggered", "not satisfied"]):
                return False

        # Default: assume not mentioned = False
        return False if asset_lower in answer_lower else None

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
        return ["24h_change", "price", "condition_evaluation"]

    # === Step-wise Reward Interface ===

    def get_target_assets(self, validation_info: Dict[str, Any]) -> set:
        """Return all condition assets - each needs to be checked."""
        conditions = validation_info.get("conditions", [])
        return {c["asset_id"] for c in conditions}

    def get_required_domains(self, validation_info: Dict[str, Any]) -> set:
        """Requires both CoinGecko (crypto) and Stooq (stocks)."""
        return {"coingecko.com", "stooq.com"}

    def get_reward_overrides(self) -> Optional[Dict[str, float]]:
        """Each condition is important - moderate target reward."""
        return {
            "target_asset_reward": 0.20,
            "all_targets_bonus": 0.30,
        }
