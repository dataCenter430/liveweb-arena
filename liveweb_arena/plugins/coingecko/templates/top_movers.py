"""Top Gainers/Losers query template for CoinGecko - MULTI-STEP INTERACTION"""

import random
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from ..api_client import CoinGeckoClient


@register_template("coingecko_top_movers")
class CoinGeckoTopMoversTemplate(QuestionTemplate):
    """
    Template for top gainers/losers queries - REQUIRES MULTI-STEP INTERACTION.

    This template requires the agent to:
    1. Navigate to CoinGecko homepage or categories
    2. Find and click on "Top Gainers" or "Top Losers" section
    3. Identify the #1 coin and its percentage change

    Examples:
    - What cryptocurrency gained the most in the last 24 hours?
    - Which coin is the biggest loser today on CoinGecko?
    - What is the #1 top gainer on CoinGecko right now?
    """

    GAINER_PATTERNS = [
        "Among the top 100 cryptocurrencies by market cap, which one gained the most in the last 24 hours?",
        "What is the biggest 24h gainer among top 100 coins on CoinGecko?",
        "Find the top performing coin in the last 24 hours from CoinGecko's top 100 by market cap.",
        "Which top-100 market cap cryptocurrency has the highest 24h gain on CoinGecko?",
    ]

    LOSER_PATTERNS = [
        "Among the top 100 cryptocurrencies by market cap, which one lost the most in the last 24 hours?",
        "What is the biggest 24h loser among top 100 coins on CoinGecko?",
        "Find the worst performing coin in the last 24 hours from CoinGecko's top 100 by market cap.",
        "Which top-100 market cap cryptocurrency has the biggest 24h loss on CoinGecko?",
    ]

    def __init__(self):
        super().__init__("coingecko_top_movers")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a top movers question."""
        rng = random.Random(seed)

        # Select gainer or loser
        if variant is not None:
            is_gainer = (variant % 2) == 0
        else:
            is_gainer = rng.choice([True, False])

        if is_gainer:
            patterns = self.GAINER_PATTERNS
            query_type = "gainer"
        else:
            patterns = self.LOSER_PATTERNS
            query_type = "loser"

        pattern = rng.choice(patterns)

        validation_info = {
            "query_type": query_type,
        }

        # Start URL is the homepage - agent must navigate to find top movers
        return GeneratedQuestion(
            question_text=pattern,
            start_url="https://www.coingecko.com",
            variables={"query_type": query_type},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=8,  # More steps needed for navigation
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        query_type = validation_info.get("query_type", "gainer")
        if query_type == "gainer":
            return """Task-Specific Rules (CoinGecko - Top Gainer):
- Answer must include the coin name AND the percentage gain
- Score 1.0: Correct coin name with percentage within 5 points
- Score 0.5: Correct coin name but percentage off by more than 5 points
- Score 0.0: Wrong coin or no answer
- Accept formats: "Bitcoin (+15.2%)", "BTC gained 15%", "Bitcoin is up 15.2%"
- Note: Top gainers change frequently, validation uses real-time data"""
        else:
            return """Task-Specific Rules (CoinGecko - Top Loser):
- Answer must include the coin name AND the percentage loss
- Score 1.0: Correct coin name with percentage within 5 points
- Score 0.5: Correct coin name but percentage off by more than 5 points
- Score 0.0: Wrong coin or no answer
- Accept formats: "Bitcoin (-15.2%)", "BTC lost 15%", "Bitcoin is down 15.2%"
- Note: Top losers change frequently, validation uses real-time data"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch top gainer/loser from CoinGecko API."""
        query_type = validation_info.get("query_type", "gainer")

        try:
            # Get top 100 coins by market cap and sort by 24h change
            data = await CoinGeckoClient.get(
                "/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": "100",
                    "page": "1",
                    "sparkline": "false",
                    "price_change_percentage": "24h",
                }
            )

            if not data:
                return None

            # Filter out coins with None change
            valid_coins = [
                c for c in data
                if c.get("price_change_percentage_24h") is not None
            ]

            if not valid_coins:
                return None

            # Sort by 24h change
            if query_type == "gainer":
                sorted_coins = sorted(
                    valid_coins,
                    key=lambda x: x.get("price_change_percentage_24h", 0),
                    reverse=True
                )
            else:
                sorted_coins = sorted(
                    valid_coins,
                    key=lambda x: x.get("price_change_percentage_24h", 0),
                    reverse=False
                )

            top_coin = sorted_coins[0]
            name = top_coin.get("name", "Unknown")
            change = top_coin.get("price_change_percentage_24h", 0)

            if query_type == "gainer":
                return f"{name} (+{change:.2f}%)"
            else:
                return f"{name} ({change:.2f}%)"

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate top mover answer."""
        import re

        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        # Parse expected: "CoinName (+/-XX.XX%)"
        exp_match = re.match(r'(.+?)\s*\(([+-]?\d+\.?\d*)%\)', ground_truth)
        if not exp_match:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not parse ground truth",
            )

        expected_name = exp_match.group(1).lower().strip()
        expected_pct = float(exp_match.group(2))

        # Check if answer contains the coin name
        answer_lower = answer.lower()
        name_found = expected_name in answer_lower

        # Also check for common variations
        if not name_found:
            # Try first word of expected name
            first_word = expected_name.split()[0]
            name_found = first_word in answer_lower

        # Parse percentage from answer
        pct_match = re.search(r'([+-]?\d+\.?\d*)\s*%', answer)
        if pct_match:
            actual_pct = float(pct_match.group(1))
            # Handle negative detection from context
            if any(w in answer_lower for w in ["down", "lost", "fell", "dropped", "-"]):
                if actual_pct > 0:
                    actual_pct = -actual_pct
        else:
            actual_pct = None

        if name_found:
            if actual_pct is not None:
                diff = abs(actual_pct - expected_pct)
                if diff <= 5:
                    return ValidationResult(
                        score=1.0,
                        is_correct=True,
                        expected=ground_truth,
                        actual=answer,
                        details=f"Correct coin and percentage (diff: {diff:.1f}pp)",
                    )
                else:
                    return ValidationResult(
                        score=0.5,
                        is_correct=False,
                        expected=ground_truth,
                        actual=answer,
                        details=f"Correct coin but percentage off (diff: {diff:.1f}pp)",
                    )
            else:
                return ValidationResult(
                    score=0.5,
                    is_correct=False,
                    expected=ground_truth,
                    actual=answer,
                    details="Correct coin but no percentage found",
                )

        return ValidationResult(
            score=0.0,
            is_correct=False,
            expected=ground_truth,
            actual=answer,
            details="Wrong coin or could not identify coin name",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """
        Trigger when AI visits the gainers/losers page.

        URLs to match:
        - /en/crypto-gainers-losers
        - /en/coins/trending
        - Homepage with sorting
        """
        trigger = UrlPatternTrigger(
            domains=["coingecko.com"],
            url_contains="gainer",  # Matches gainers-losers page
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
