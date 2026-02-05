"""Portfolio Rebalance - Multi-asset ranking with actionable recommendations"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
    source: str  # "coingecko" or "stooq"
    symbol: str


# Stable crypto assets (top market cap)
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

# Stable stock assets (mega-cap $100B+)
STOCK_ASSETS = [
    AssetSpec("aapl.us", "Apple", "stooq", "aapl.us"),
    AssetSpec("msft.us", "Microsoft", "stooq", "msft.us"),
    AssetSpec("googl.us", "Google", "stooq", "googl.us"),
    AssetSpec("amzn.us", "Amazon", "stooq", "amzn.us"),
    AssetSpec("nvda.us", "NVIDIA", "stooq", "nvda.us"),
    AssetSpec("meta.us", "Meta", "stooq", "meta.us"),
    AssetSpec("tsla.us", "Tesla", "stooq", "tsla.us"),
    AssetSpec("jpm.us", "JPMorgan", "stooq", "jpm.us"),
    AssetSpec("v.us", "Visa", "stooq", "v.us"),
    AssetSpec("wmt.us", "Walmart", "stooq", "wmt.us"),
]


@register_template("hybrid_portfolio_rebalance")
class HybridPortfolioRebalanceTemplate(QuestionTemplate):
    """
    Portfolio rebalance task with actionable recommendations.

    The agent must:
    1. Check 24h performance for all assets in the portfolio
    2. Identify the best and worst performers
    3. Recommend INCREASE for best, DECREASE for worst, HOLD for middle
    4. Calculate the performance spread (best% - worst%)

    RL-friendly features:
    - Must explore ALL assets (no shortcuts)
    - Partial credit via Kendall tau for ranking
    - Spread calculation adds numerical accuracy component
    - Cross-site navigation (CoinGecko + Stooq)

    Scoring breakdown:
    - 70%: Ranking correctness (Kendall tau correlation)
    - 30%: Spread accuracy (within 2 percentage points)
    """

    GT_SOURCE = GTSourceType.API_ONLY

    PATTERNS = [
        (
            "You have a portfolio with these assets: {assets}. "
            "Based on current 24h performance, recommend:\n"
            "- INCREASE: the best performer\n"
            "- DECREASE: the worst performer\n"
            "- HOLD: the middle performers\n"
            "Also report the performance spread (best% - worst%)."
        ),
        (
            "Analyze this portfolio: {assets}. "
            "Rank them by today's performance and tell me:\n"
            "1. Which to INCREASE (top performer)\n"
            "2. Which to DECREASE (bottom performer)\n"
            "3. Which to HOLD (middle ones)\n"
            "Include the spread between best and worst performance."
        ),
        (
            "Portfolio review: {assets}. "
            "Based on 24-hour returns, identify:\n"
            "- Best performer (recommend INCREASE)\n"
            "- Worst performer (recommend DECREASE)\n"
            "- Others (recommend HOLD)\n"
            "What's the performance gap between best and worst?"
        ),
    ]

    def __init__(self):
        super().__init__("hybrid_portfolio_rebalance")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a portfolio rebalance task with 4 assets."""
        rng = random.Random(seed)

        # Select 2 crypto + 2 stocks for balanced portfolio
        selected_crypto = rng.sample(CRYPTO_ASSETS, 2)
        selected_stocks = rng.sample(STOCK_ASSETS, 2)

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
            expected_steps=len(all_assets) * 2 + 4,  # 2 steps per asset + overhead
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        asset_names = validation_info.get("asset_names", [])
        return f"""Task-Specific Rules (Hybrid - Portfolio Rebalance):
- Rank all assets by 24h performance: {', '.join(asset_names)}
- Identify: INCREASE (best), DECREASE (worst), HOLD (middle)
- Calculate spread: (best% - worst%)
- Score breakdown:
  - 70%: Ranking correctness (Kendall tau >= 0.6 for partial, >= 0.99 for full)
  - 30%: Spread accuracy (within 2 percentage points for full credit)
- Example output: "INCREASE: Bitcoin (+5.2%), DECREASE: Apple (-1.3%), HOLD: Ethereum, NVIDIA. Spread: 6.5%"
"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        """Fetch all asset performances and compute recommendations."""
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
            error_msg = "; ".join(errors)
            return GroundTruthResult.retry(f"Could not fetch all asset data: {error_msg}")

        if len(results) < 2:
            return GroundTruthResult.fail("Insufficient assets for ranking")

        # Sort by change descending
        sorted_results = sorted(results, key=lambda x: x["change"], reverse=True)

        best = sorted_results[0]
        worst = sorted_results[-1]
        middle = sorted_results[1:-1]

        spread = best["change"] - worst["change"]

        # Build ground truth string
        middle_names = [r["name"] for r in middle]
        ranking_str = " > ".join([f"{r['name']}({r['change']:+.2f}%)" for r in sorted_results])

        gt_str = (
            f"INCREASE: {best['name']} ({best['change']:+.2f}%) | "
            f"DECREASE: {worst['name']} ({worst['change']:+.2f}%) | "
            f"HOLD: {', '.join(middle_names)} | "
            f"Spread: {spread:.2f}pp | "
            f"Ranking: {ranking_str}"
        )

        return GroundTruthResult.ok(gt_str)

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate recommendations using combined ranking + spread scoring."""
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
        # Format: "INCREASE: X (%) | DECREASE: Y (%) | HOLD: ... | Spread: N.NNpp | Ranking: ..."
        increase_match = re.search(r"INCREASE:\s*([^(|]+)", ground_truth)
        decrease_match = re.search(r"DECREASE:\s*([^(|]+)", ground_truth)
        spread_match = re.search(r"Spread:\s*([\d.]+)pp", ground_truth)
        ranking_match = re.search(r"Ranking:\s*(.+)$", ground_truth)

        if not all([increase_match, decrease_match, spread_match]):
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not parse ground truth",
            )

        expected_increase = increase_match.group(1).strip().lower()
        expected_decrease = decrease_match.group(1).strip().lower()
        expected_spread = float(spread_match.group(1))

        # Extract expected order from ranking
        expected_order = self._parse_ranking_order(ranking_match.group(1) if ranking_match else "")

        # 1. Check ranking via Kendall tau (70% of score)
        actual_order = self._parse_answer_order(answer, expected_order)
        if len(actual_order) >= 2:
            tau = self._kendall_tau(expected_order, actual_order)
        else:
            tau = 0.0

        if tau >= 0.99:
            ranking_score = 1.0
        elif tau >= 0.6:
            ranking_score = 0.5
        else:
            ranking_score = 0.0

        # 2. Check spread accuracy (30% of score)
        spread_score = 0.0
        actual_spread = self._extract_spread(answer)
        if actual_spread is not None:
            spread_diff = abs(actual_spread - expected_spread)
            if spread_diff <= 2.0:  # Within 2 percentage points
                spread_score = 1.0
            elif spread_diff <= 5.0:  # Within 5 percentage points
                spread_score = 0.5

        # Combined score
        final_score = 0.7 * ranking_score + 0.3 * spread_score

        # Determine correctness (require at least partial credit on both)
        is_correct = final_score >= 0.7

        details = (
            f"Ranking tau={tau:.2f} (score={ranking_score:.1f}), "
            f"Spread diff={abs(actual_spread - expected_spread):.1f}pp (score={spread_score:.1f})"
            if actual_spread is not None else
            f"Ranking tau={tau:.2f} (score={ranking_score:.1f}), Spread not found"
        )

        return ValidationResult(
            score=final_score,
            is_correct=is_correct,
            expected=ground_truth,
            actual=answer,
            details=details,
        )

    def _parse_ranking_order(self, ranking_str: str) -> List[str]:
        """Extract ordered list from ranking string like 'Bitcoin(+5%) > ETH(+3%)'."""
        import re
        # Match name followed by percentage
        matches = re.findall(r"([A-Za-z]+)\s*\([^)]+\)", ranking_str)
        return [m.lower() for m in matches]

    def _parse_answer_order(self, answer: str, expected_names: List[str]) -> List[str]:
        """Extract ordering from answer based on known asset names."""
        answer_lower = answer.lower()

        # Build name variations for matching
        variations = {
            "bitcoin": ["btc"], "ethereum": ["eth"], "solana": ["sol"],
            "xrp": ["ripple"], "cardano": ["ada"], "dogecoin": ["doge"],
            "avalanche": ["avax"], "polkadot": ["dot"], "chainlink": ["link"],
            "bnb": ["binance"], "apple": ["aapl"], "microsoft": ["msft"],
            "nvidia": ["nvda"], "tesla": ["tsla"], "google": ["googl", "alphabet"],
            "amazon": ["amzn"], "meta": ["facebook"], "jpmorgan": ["jpm"],
            "visa": [], "walmart": ["wmt"],
        }
        name_map = {}
        for name in expected_names:
            name_lower = name.lower()
            name_map[name_lower] = name_lower
            if name_lower in variations:
                for var in variations[name_lower]:
                    name_map[var] = name_lower

        # Find positions of each asset in the answer
        positions = []
        for variant, canonical in name_map.items():
            pos = answer_lower.find(variant)
            if pos >= 0:
                if not any(p[1] == canonical for p in positions):
                    positions.append((pos, canonical))

        positions.sort(key=lambda x: x[0])
        return [p[1] for p in positions]

    def _kendall_tau(self, expected: List[str], actual: List[str]) -> float:
        """Calculate Kendall tau correlation between two rankings."""
        exp_pos = {name.lower(): i for i, name in enumerate(expected)}
        actual_filtered = [a.lower() for a in actual if a.lower() in exp_pos]

        if len(actual_filtered) < 2:
            return 0.0

        n = len(actual_filtered)
        concordant = 0
        discordant = 0

        for i in range(n):
            for j in range(i + 1, n):
                a_i, a_j = actual_filtered[i], actual_filtered[j]
                exp_i, exp_j = exp_pos.get(a_i, 0), exp_pos.get(a_j, 0)

                if exp_i < exp_j:
                    concordant += 1
                elif exp_i > exp_j:
                    discordant += 1

        total = concordant + discordant
        if total == 0:
            return 1.0

        return (concordant - discordant) / total

    def _extract_spread(self, answer: str) -> Optional[float]:
        """Extract spread value from answer."""
        import re
        # Look for patterns like "spread: 6.5%", "spread is 6.5", "6.5pp spread"
        patterns = [
            r"spread[:\s]+([+-]?\d+\.?\d*)\s*(?:%|pp|percentage)?",
            r"([+-]?\d+\.?\d*)\s*(?:%|pp)?\s*spread",
            r"difference[:\s]+([+-]?\d+\.?\d*)\s*(?:%|pp)?",
            r"gap[:\s]+([+-]?\d+\.?\d*)\s*(?:%|pp)?",
        ]
        for pattern in patterns:
            match = re.search(pattern, answer.lower())
            if match:
                return abs(float(match.group(1)))
        return None

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger on Stooq visit (after CoinGecko exploration)."""
        trigger = UrlPatternTrigger(domains=["stooq.com"])
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)

    @classmethod
    def get_cache_source(cls) -> str:
        return "hybrid"

    def get_api_fields(self):
        return ["24h_change", "ranking", "spread"]

    # === Step-wise Reward Interface ===

    def get_target_assets(self, validation_info: Dict[str, Any]) -> set:
        """Return all 4 assets as targets - agent must collect all for ranking."""
        assets = validation_info.get("assets", [])
        return {a["asset_id"] for a in assets}

    def get_required_domains(self, validation_info: Dict[str, Any]) -> set:
        """Requires both CoinGecko (crypto) and Stooq (stocks)."""
        return {"coingecko.com", "stooq.com"}

    def get_reward_overrides(self) -> Dict[str, float]:
        """All assets are equally important for ranking."""
        return {
            "target_asset_reward": 0.30,
            "all_targets_bonus": 0.50,
        }
