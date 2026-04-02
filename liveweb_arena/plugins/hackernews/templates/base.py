"""Base class for Hacker News templates."""

from typing import Any, Dict

from liveweb_arena.core.ground_truth_trigger import TriggerConfig, UrlPatternTrigger
from liveweb_arena.core.gt_collector import GTSourceType
from liveweb_arena.core.validators.base import QuestionTemplate


class HackerNewsTemplateBase(QuestionTemplate):
    """Shared HN template defaults for trigger/cache/GT source."""

    def get_ground_truth_trigger(self, validation_info: Dict[str, Any]) -> TriggerConfig:
        return TriggerConfig(trigger=UrlPatternTrigger(domains=["news.ycombinator.com"]))

    @classmethod
    def get_cache_source(cls) -> str:
        return "hackernews"

    def get_gt_source(self) -> GTSourceType:
        return GTSourceType.PAGE_ONLY
