"""Stooq plugin for financial market data queries"""

import random
from typing import Dict, List

from liveweb_arena.plugins.base import BasePlugin, SubTask, ValidationResult
from liveweb_arena.core.validators.base import QuestionTemplate, get_registered_templates

# Import templates to trigger registration
from . import templates as _  # noqa: F401


class StooqPlugin(BasePlugin):
    """
    Plugin for querying financial market data from stooq.com.

    Stooq is a financial data portal providing real-time and historical
    prices for stocks, indices, currencies, and commodities.

    Supported templates:
    - stooq_price: Current price queries for individual instruments
    - stooq_comparison: Compare multiple instruments
    - stooq_historical: Historical data queries
    - stooq_market_summary: Market overview and analysis questions

    Ground truth is fetched from Stooq's CSV download endpoint.
    """

    def __init__(self, templates: List[str] = None):
        self._template_instances: Dict[str, QuestionTemplate] = {}

        # Get stooq templates from global registry
        registered = get_registered_templates()
        stooq_templates = {
            k: v for k, v in registered.items()
            if k.startswith("stooq_")
        }

        template_names = templates or list(stooq_templates.keys())
        for name in template_names:
            if name in stooq_templates:
                self._template_instances[name] = stooq_templates[name]()

    @property
    def name(self) -> str:
        return "stooq"

    @property
    def supported_sites(self) -> List[str]:
        return ["stooq.com"]

    @property
    def description(self) -> str:
        return "Query financial market data including stocks, indices, currencies, and commodities from stooq.com"

    @property
    def usage_hint(self) -> str:
        return """## Stooq Financial Data (stooq.com)

**Website**: https://stooq.com

**URL Patterns**:
- Stock quote: https://stooq.com/q/?s=aapl.us (US stocks use .us suffix)
- Index quote: https://stooq.com/q/?s=^dji (indices use ^ prefix)
- Currency pair: https://stooq.com/q/?s=eurusd
- Commodity: https://stooq.com/q/?s=gc.f (futures use .f suffix)
- Historical data: https://stooq.com/q/d/?s=aapl.us

**Key Pages**:
- /q/?s={symbol} - Quote page with current price, change, volume
- /q/d/?s={symbol} - Historical data table
- /t/?i=510 - Main indices overview
- /t/?i=515 - NYSE stocks list

**Available Data**:
- Current price (Last)
- Price change (absolute and percentage)
- Open, High, Low prices
- Trading volume
- Historical daily data (Open, High, Low, Close, Volume)

**Symbol Examples**:
- US Stocks: aapl.us, msft.us, googl.us, amzn.us, nvda.us
- Indices: ^dji (Dow Jones), ^spx (S&P 500), ^ndx (NASDAQ 100)
- Currencies: eurusd, gbpusd, usdjpy
- Commodities: gc.f (Gold), cl.f (Oil), si.f (Silver)

**Tips**:
- Percentage changes are shown with + or - signs
- The page updates automatically with real-time data
- Historical data can be viewed in table format or downloaded
- Market data may be delayed by 15-20 minutes for some instruments
"""

    async def generate_task(
        self,
        seed: int,
        template_name: str = None,
        metric: str = None,
    ) -> SubTask:
        """
        Generate a Stooq query task.

        Args:
            seed: Random seed for task generation
            template_name: Specific template to use (e.g., "stooq_price")
            metric: Not used for Stooq (kept for API compatibility)
        """
        rng = random.Random(seed)

        if not self._template_instances:
            raise ValueError("No templates available")

        # Select template
        if template_name and template_name in self._template_instances:
            selected_template_name = template_name
        else:
            selected_template_name = rng.choice(list(self._template_instances.keys()))

        template = self._template_instances[selected_template_name]

        # Generate question
        question = template.generate(seed)

        return SubTask(
            plugin_name=self.name,
            intent=question.question_text,
            validation_info={
                "template_name": selected_template_name,
                **question.validation_info,
            },
            answer_tag="",
        )

    async def validate_answer(
        self, answer: str, validation_info: dict
    ) -> ValidationResult:
        """Validate answer using the appropriate template"""
        template_name = validation_info.get("template_name")
        template = self._template_instances.get(template_name)

        if template is None:
            template = list(self._template_instances.values())[0]

        result = await template.validate_answer(answer, validation_info)

        return ValidationResult(
            score=result.score,
            is_correct=result.is_correct,
            expected=result.expected,
            actual=result.actual,
            details=result.details,
        )

    async def get_ground_truth(self, validation_info: dict):
        """Get ground truth from the appropriate template"""
        template_name = validation_info.get("template_name")
        template = self._template_instances.get(template_name)

        if template is None:
            return None

        return await template.get_ground_truth(validation_info)

    def get_validation_rules(self, validation_info: dict) -> str:
        """Get validation rules from template"""
        template_name = validation_info.get("template_name")
        template = self._template_instances.get(template_name)

        if template is None:
            return ""

        return template.get_validation_rules(validation_info)
