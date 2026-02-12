"""Hacker News question templates.

RL-friendly template design:
- All templates require multi-step reasoning
- All templates require computation or comparison
- All templates have large exploration space
- Low memorization risk due to dynamic data and combinatorial question space
"""

from .external_page_title import HackerNewsExternalPageTitleTemplate
from .multi_condition_filter import HackerNewsMultiConditionFilterTemplate
from .extrema_comparison import HackerNewsExtremaComparisonTemplate
from .category_comparison import HackerNewsCategoryComparisonTemplate
from .news_summary import HackerNewsNewsSummaryTemplate

__all__ = [
    "HackerNewsExternalPageTitleTemplate",
    "HackerNewsMultiConditionFilterTemplate",
    "HackerNewsExtremaComparisonTemplate",
    "HackerNewsCategoryComparisonTemplate",
    "HackerNewsNewsSummaryTemplate",
]
