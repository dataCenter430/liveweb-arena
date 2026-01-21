"""Market rank query template for CoinGecko - HIGH DIFFICULTY"""

import random
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .price import CoinVariable, CoinSpec
from ..api_client import CoinGeckoClient


@register_template("coingecko_rank")
class CoinGeckoRankTemplate(QuestionTemplate):
    """
    Template for market cap rank queries - HIGH DIFFICULTY.

    Requires finding the rank of a coin, which may need scrolling
    through the market rankings page or finding it on the coin page.

    Examples:
    - What is Bitcoin's market cap rank?
    - What rank is Solana by market capitalization?
    - Where does Cardano rank in the crypto market?
    """

    PATTERNS = [
        "What is {coin}'s market cap rank?",
        "What rank is {coin} by market capitalization?",
        "Where does {coin} rank in the crypto market by market cap?",
        "What position is {coin} in the cryptocurrency rankings?",
    ]

    def __init__(self):
        super().__init__("coingecko_rank")
        self._coin_var = CoinVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a rank question."""
        rng = random.Random(seed)
        coin = self._coin_var.sample(rng)

        pattern = rng.choice(self.PATTERNS)
        question_text = pattern.format(coin=coin.name)

        validation_info = {
            "coin_id": coin.coin_id,
            "coin_name": coin.name,
            "coin_symbol": coin.symbol,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=f"https://www.coingecko.com/en/coins/{coin.coin_id}",
            variables={"coin": coin},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        return """Task-Specific Rules (CoinGecko - Market Rank):
- Ranks change frequently but typically within a few positions
- Score 1.0: Exact rank match or within 1 position
- Score 0.5: Within 3 positions
- Score 0.0: More than 3 positions off
- Accept formats: "#5", "5", "5th", "rank 5", "ranked #5", "position 5"
- Note: Lower rank number = higher market cap (rank 1 is highest)"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch market rank from CoinGecko API."""
        coin_id = validation_info.get("coin_id", "")
        if not coin_id:
            return None

        try:
            data = await CoinGeckoClient.get_coin_market_data(coin_id)
            if not data:
                return None

            rank = data[0].get("market_cap_rank")
            if rank is not None:
                return f"#{rank}"

            return None
        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate rank answer."""
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

        # Parse expected rank
        expected_match = re.search(r'(\d+)', ground_truth)
        if not expected_match:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not parse expected rank",
            )
        expected_rank = int(expected_match.group(1))

        # Parse actual rank from answer
        actual_match = re.search(r'(\d+)', answer)
        if not actual_match:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not find rank number in answer",
            )
        actual_rank = int(actual_match.group(1))

        diff = abs(actual_rank - expected_rank)

        if diff <= 1:
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=ground_truth,
                actual=answer,
                details=f"Rank match (diff: {diff})",
            )
        elif diff <= 3:
            return ValidationResult(
                score=0.5,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Within 3 positions (diff: {diff})",
            )
        else:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Outside tolerance (diff: {diff})",
            )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when AI visits the coin's page."""
        coin_id = validation_info.get("coin_id", "")
        trigger = UrlPatternTrigger(
            domains=["coingecko.com"],
            url_contains=coin_id if coin_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
