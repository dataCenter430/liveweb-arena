"""24h Trading Volume query template for CoinGecko - LOW DIFFICULTY"""

import random
from typing import Any, Dict, List, Optional

import aiohttp

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .price import CoinVariable, CoinSpec


@register_template("coingecko_volume")
class CoinGeckoVolumeTemplate(QuestionTemplate):
    """
    Template for 24h trading volume queries - LOW DIFFICULTY.

    Simple single-value lookup on coin page.

    Examples:
    - What is Bitcoin's 24-hour trading volume?
    - How much Ethereum was traded in the last 24 hours?
    """

    COINGECKO_API = "https://api.coingecko.com/api/v3"

    PATTERNS = [
        "What is {coin}'s 24-hour trading volume?",
        "How much {coin} was traded in the last 24 hours?",
        "What is the 24h trading volume for {coin}?",
        "What is {coin}'s daily trading volume?",
    ]

    def __init__(self):
        super().__init__("coingecko_volume")
        self._coin_var = CoinVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a trading volume question."""
        rng = random.Random(seed)
        coin = self._coin_var.sample(rng)

        pattern = rng.choice(self.PATTERNS)
        question_text = pattern.format(coin=coin.name)
        start_url = f"https://www.coingecko.com/en/coins/{coin.coin_id}"

        validation_info = {
            "coin_id": coin.coin_id,
            "coin_name": coin.name,
            "coin_symbol": coin.symbol,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"coin": coin},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        return """Task-Specific Rules (CoinGecko - 24h Volume):
- Trading volume fluctuates frequently
- Score 1.0: Values match within 10% tolerance
- Score 0.5: Values match within 20% tolerance
- Score 0.0: Values differ by more than 20%
- Accept formats: $1.2B, $1.2 billion, 1200000000, 1.2B USD"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch 24h volume from CoinGecko API."""
        coin_id = validation_info.get("coin_id", "")
        if not coin_id:
            return None

        try:
            url = f"{self.COINGECKO_API}/coins/markets"
            params = {
                "vs_currency": "usd",
                "ids": coin_id,
                "order": "market_cap_desc",
                "sparkline": "false",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status != 200:
                        return None
                    data = await response.json()

            if not data:
                return None

            volume = data[0].get("total_volume")
            if volume is not None:
                if volume >= 1e9:
                    return f"${volume/1e9:.2f} billion"
                elif volume >= 1e6:
                    return f"${volume/1e6:.2f} million"
                else:
                    return f"${volume:,.0f}"

            return None
        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate trading volume answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        expected_val = self._parse_volume(ground_truth)
        actual_val = self._parse_volume(answer)

        if expected_val is None or actual_val is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details="Could not parse volume values",
            )

        diff_pct = abs(actual_val - expected_val) / expected_val * 100

        if diff_pct <= 10:
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=ground_truth,
                actual=answer,
                details=f"Within 10% tolerance (diff: {diff_pct:.1f}%)",
            )
        elif diff_pct <= 20:
            return ValidationResult(
                score=0.5,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Within 20% tolerance (diff: {diff_pct:.1f}%)",
            )
        else:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=ground_truth,
                actual=answer,
                details=f"Outside tolerance (diff: {diff_pct:.1f}%)",
            )

    def _parse_volume(self, text: str) -> Optional[float]:
        """Parse volume from text."""
        import re
        if not text:
            return None

        text = text.replace(",", "").replace("$", "").strip().lower()

        multipliers = {
            "trillion": 1e12, "t": 1e12,
            "billion": 1e9, "b": 1e9,
            "million": 1e6, "m": 1e6,
            "thousand": 1e3, "k": 1e3,
        }

        multiplier = 1
        for word, mult in multipliers.items():
            if word in text:
                text = re.sub(rf'\s*{word}\s*', '', text)
                multiplier = mult
                break

        match = re.search(r'[\d.]+', text)
        if match:
            try:
                return float(match.group()) * multiplier
            except ValueError:
                pass
        return None

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
