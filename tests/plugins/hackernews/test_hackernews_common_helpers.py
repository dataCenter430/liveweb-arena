"""Tests for shared Hacker News helpers and refactored existing templates."""

import asyncio
from typing import Any, Dict

import pytest

from liveweb_arena.core.gt_collector import GTSourceType, set_current_gt_collector
from liveweb_arena.plugins.hackernews.templates.common import (
    extract_first_number,
    get_category_stories,
    get_homepage_stories,
    title_matches,
    title_partial_match,
)
from liveweb_arena.plugins.hackernews.templates.category_comparison import (
    HackerNewsCategoryComparisonTemplate,
)
from liveweb_arena.plugins.hackernews.templates.extrema_comparison import (
    HackerNewsExtremaComparisonTemplate,
)
from liveweb_arena.plugins.hackernews.templates.multi_condition_filter import (
    HackerNewsMultiConditionFilterTemplate,
)
from liveweb_arena.plugins.hackernews.templates.news_summary import (
    HackerNewsNewsSummaryTemplate,
)


class _DummyCollector:
    def __init__(self, data: Dict[str, Dict[str, Any]]):
        self._data = data

    def get_collected_api_data(self) -> Dict[str, Dict[str, Any]]:
        return self._data


def _run_with_collector(data: Dict[str, Dict[str, Any]], coro):
    set_current_gt_collector(_DummyCollector(data))
    try:
        return asyncio.run(coro)
    finally:
        set_current_gt_collector(None)


def _make_story(rank: int, title: str, score: int, comments: int) -> Dict[str, Any]:
    return {
        "id": 1000 + rank,
        "rank": rank,
        "title": title,
        "score": score,
        "descendants": comments,
    }


# ── get_homepage_stories ─────────────────────────────────────────────

def test_get_homepage_stories_filters_non_story_keys():
    """Helper must skip category/user/external collector keys."""
    collected = {
        "101": _make_story(1, "Alpha", 100, 50),
        "102": _make_story(2, "Beta", 90, 45),
        "103": _make_story(3, "Gamma", 80, 30),
        "user:alice": {"user": {"id": "alice"}},
        "hn_category:ask": {"category": "ask", "stories": {}},
        "external:https://x.com": {"is_external": True},
        "hn_external:1": {"is_external": True},
    }
    set_current_gt_collector(_DummyCollector(collected))
    try:
        stories, failure = get_homepage_stories(
            story_count=3,
            required_fields=("rank", "title", "score", "descendants"),
        )
    finally:
        set_current_gt_collector(None)

    assert failure is None
    assert stories is not None
    assert len(stories) == 3
    assert [s["rank"] for s in stories] == [1, 2, 3]


def test_get_homepage_stories_requires_complete_data():
    collected = {
        "101": _make_story(1, "Alpha", 100, 50),
        "102": {"id": 1002, "rank": 2, "title": "Beta", "score": 90},
        "103": _make_story(3, "Gamma", 80, 30),
    }
    set_current_gt_collector(_DummyCollector(collected))
    try:
        stories, failure = get_homepage_stories(
            story_count=3,
            required_fields=("rank", "title", "score", "descendants"),
        )
    finally:
        set_current_gt_collector(None)

    assert stories is None
    assert failure is not None
    assert failure.is_data_not_collected()


def test_get_homepage_stories_rejects_duplicate_ranks():
    collected = {
        "101": _make_story(1, "Alpha", 100, 50),
        "102": _make_story(1, "Beta", 90, 40),
        "103": _make_story(2, "Gamma", 80, 30),
    }
    set_current_gt_collector(_DummyCollector(collected))
    try:
        stories, failure = get_homepage_stories(
            story_count=2,
            required_fields=("rank", "title", "score", "descendants"),
        )
    finally:
        set_current_gt_collector(None)

    assert stories is None
    assert failure is not None
    assert "Duplicate homepage ranks" in (failure.error or "")
    assert failure.is_system_error()


# ── get_category_stories ─────────────────────────────────────────────

def test_get_category_stories_happy_path():
    """Happy path: returns stories ordered by rank with required fields."""
    collected = {
        "hn_category:ask": {
            "stories": {
                "1001": {"id": 1001, "rank": 2, "score": 20, "title": "Beta"},
                "1002": {"id": 1002, "rank": 1, "score": 30, "title": "Alpha"},
                "1003": {"id": 1003, "rank": 3, "score": 10, "title": "Gamma"},
            }
        }
    }
    set_current_gt_collector(_DummyCollector(collected))
    try:
        stories, failure = get_category_stories(
            category_slug="ask",
            upto_rank=3,
            required_fields=("score",),
        )
    finally:
        set_current_gt_collector(None)

    assert failure is None
    assert stories is not None
    assert len(stories) == 3
    assert [s["rank"] for s in stories] == [1, 2, 3]
    assert [s["title"] for s in stories] == ["Alpha", "Beta", "Gamma"]


def test_get_category_stories_rejects_duplicate_ranks():
    collected = {
        "hn_category:ask": {
            "stories": {
                "1001": {"id": 1001, "rank": 1, "score": 10},
                "1002": {"id": 1002, "rank": 1, "score": 20},
            }
        }
    }
    set_current_gt_collector(_DummyCollector(collected))
    try:
        stories, failure = get_category_stories(
            category_slug="ask",
            upto_rank=2,
            required_fields=("score",),
        )
    finally:
        set_current_gt_collector(None)

    assert stories is None
    assert failure is not None
    assert failure.is_system_error()


# ── extract_first_number ─────────────────────────────────────────────

def test_extract_first_number_unsigned_integer():
    assert extract_first_number("answer is 42", signed=False, allow_float=False) == 42.0


def test_extract_first_number_signed_integer():
    assert extract_first_number("delta is -7", signed=True, allow_float=False) == -7.0


def test_extract_first_number_float():
    assert extract_first_number("ratio 3.75x", signed=False, allow_float=True) == 3.75


def test_extract_first_number_no_match():
    assert extract_first_number("no number here", signed=False, allow_float=False) is None


# ── title_matches / title_partial_match ──────────────────────────────

def test_title_matches_exact():
    assert title_matches("Show HN: My Project", "Show HN: My Project")


def test_title_matches_tolerates_punctuation():
    assert title_matches("A.I. beats humans", "AI beats humans")


def test_title_matches_rejects_different_titles():
    assert not title_matches("Deep Learning Survey", "Reinforcement Learning Survey")


def test_title_partial_match_handles_short_titles():
    assert title_partial_match("Go vs Zig", "I think Go vs Zig wins", min_ratio=0.5)
    assert not title_partial_match("Go vs Zig", "Rust memory model", min_ratio=0.5)


def test_title_partial_match_token_ratio():
    assert title_partial_match(
        "Building a faster database engine",
        "About building a faster database engine for production",
        min_ratio=0.6,
    )
    assert not title_partial_match(
        "Building a faster database engine",
        "Cooking recipes for beginners",
        min_ratio=0.6,
    )


# ── Existing template base class migration ───────────────────────────

@pytest.mark.parametrize("cls", [
    HackerNewsMultiConditionFilterTemplate,
    HackerNewsExtremaComparisonTemplate,
    HackerNewsCategoryComparisonTemplate,
    HackerNewsNewsSummaryTemplate,
])
def test_refactored_templates_use_shared_base(cls):
    tmpl = cls()
    assert tmpl.get_gt_source() == GTSourceType.PAGE_ONLY
    assert cls.get_cache_source() == "hackernews"
    trigger = tmpl.get_ground_truth_trigger({})
    assert trigger is not None


@pytest.mark.parametrize("cls", [
    HackerNewsMultiConditionFilterTemplate,
    HackerNewsExtremaComparisonTemplate,
    HackerNewsCategoryComparisonTemplate,
    HackerNewsNewsSummaryTemplate,
])
def test_refactored_templates_generate_valid_questions(cls):
    tmpl = cls()
    q = tmpl.generate(seed=42)
    assert q.question_text
    assert q.start_url
    assert q.template_name
    for key, value in q.validation_info.items():
        assert isinstance(value, (str, int, float, bool, type(None))), (
            f"{cls.__name__}.validation_info['{key}'] is not JSON-serializable"
        )


# ── Existing template error handling (regression) ────────────────────

def test_multi_condition_filter_invalid_numeric_returns_system_error():
    tmpl = HackerNewsMultiConditionFilterTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 50),
        "12": {"id": 1012, "rank": 2, "title": "Beta", "score": "bad", "descendants": 60},
        "13": _make_story(3, "Gamma", 80, 30),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
        "16": _make_story(6, "Zeta", 50, 10),
        "17": _make_story(7, "Eta", 40, 5),
        "18": _make_story(8, "Theta", 30, 5),
        "19": _make_story(9, "Iota", 20, 5),
        "20": _make_story(10, "Kappa", 10, 5),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "condition_type": "both_high",
        "story_count": 10,
        "threshold1": 30,
        "threshold2": 20,
    }))
    assert result.success is False
    assert "Invalid numeric fields" in (result.error or "")
    assert result.is_system_error()


def test_extrema_comparison_invalid_numeric_returns_system_error():
    tmpl = HackerNewsExtremaComparisonTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 50),
        "12": {"id": 1012, "rank": 2, "title": "Beta", "score": 90, "descendants": "oops"},
        "13": _make_story(3, "Gamma", 80, 30),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric_field": "descendants",
        "comparison": "difference",
        "story_count": 5,
    }))
    assert result.success is False
    assert "Invalid numeric value" in (result.error or "")
    assert result.is_system_error()


def test_category_comparison_preserves_system_error_from_helper():
    tmpl = HackerNewsCategoryComparisonTemplate()
    collected = {
        "hn_category:ask": {
            "stories": {
                "1001": {"id": 1001, "rank": 1, "score": 10},
                "1002": {"id": 1002, "rank": 1, "score": 20},
            }
        },
        "hn_category:show": {
            "stories": {
                "2001": {"id": 2001, "rank": 1, "score": 5},
            }
        },
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric_field": "score",
        "comparison_mode": "which_higher",
        "rank": 1,
        "category1_name": "Ask HN",
        "category1_slug": "ask",
        "category2_name": "Show HN",
        "category2_slug": "show",
    }))
    assert result.success is False
    assert result.is_system_error()


def test_category_comparison_invalid_numeric_returns_system_error():
    tmpl = HackerNewsCategoryComparisonTemplate()
    collected = {
        "hn_category:ask": {
            "stories": {
                "1001": {"id": 1001, "rank": 1, "score": "oops"},
            }
        },
        "hn_category:show": {
            "stories": {
                "2001": {"id": 2001, "rank": 1, "score": 5},
            }
        },
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric_field": "score",
        "comparison_mode": "which_higher",
        "rank": 1,
        "category1_name": "Ask HN",
        "category1_slug": "ask",
        "category2_name": "Show HN",
        "category2_slug": "show",
    }))
    assert result.success is False
    assert result.is_system_error()
    assert "Invalid numeric value" in (result.error or "")
