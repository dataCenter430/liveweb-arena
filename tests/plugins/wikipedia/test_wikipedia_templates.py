"""Tests for the Wikipedia plugin templates.

Covers:
1. Template registration
2. Question generation invariants (URL, validation_info fields, determinism)
3. GT logic for category_count (found, not_collected, fuzzy matching)
4. GT logic for edit_count (correct count, cutoff arithmetic, edge cases)
5. WikipediaPlugin URL routing (needs_api_data, fetch_api_data dispatch)
6. titles_match and normalize_title helper behaviour
7. Pool size assertions
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest

from liveweb_arena.core.gt_collector import GTSourceType, set_current_gt_collector
from liveweb_arena.core.validators.base import get_registered_templates
from liveweb_arena.plugins.wikipedia.templates.category_count import (
    CATEGORY_POOL,
    CountMetric,
    WikipediaCategoryCountTemplate,
)
from liveweb_arena.plugins.wikipedia.templates.edit_count import (
    ARTICLE_POOL,
    TIME_WINDOWS,
    WikipediaEditCountTemplate,
)
from liveweb_arena.plugins.wikipedia.templates.common import normalize_title, titles_match
from liveweb_arena.plugins.wikipedia.wikipedia import WikipediaPlugin


# ── Helpers ──────────────────────────────────────────────────────────────────


class _DummyCollector:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def get_collected_api_data(self) -> Dict[str, Any]:
        return self._data


def _run_gt(data: Dict[str, Any], coro):
    set_current_gt_collector(_DummyCollector(data))
    try:
        return asyncio.run(coro)
    finally:
        set_current_gt_collector(None)


def _category_entry(category_name: str, article_count: int, subcategory_count: int = 3) -> Dict:
    return {
        "type": "category",
        "category_name": category_name,
        "article_count": article_count,
        "subcategory_count": subcategory_count,
    }


def _history_entry(
    article_title: str,
    timestamps,  # list of ISO 8601 strings
    fetched_at: str,
) -> Dict:
    return {
        "type": "history",
        "article_title": article_title,
        "revisions": [{"timestamp": ts} for ts in timestamps],
        "fetched_at": fetched_at,
    }


SEEDS = [1, 42, 100, 999, 12345]


# ── 1. Template registration ──────────────────────────────────────────────────


@pytest.mark.parametrize("name", [
    "wikipedia_category_count",
    "wikipedia_edit_count",
])
def test_template_registered(name):
    templates = get_registered_templates()
    assert name in templates, f"Template '{name}' not registered"


# ── 2. Generation invariants ──────────────────────────────────────────────────


@pytest.mark.parametrize("seed", SEEDS)
def test_category_count_generate(seed):
    q = WikipediaCategoryCountTemplate().generate(seed)
    assert q.question_text
    assert q.start_url == "https://en.wikipedia.org/wiki/Main_Page"
    assert q.template_name == "wikipedia_category_count"
    assert "category_name" in q.validation_info
    assert "metric" in q.validation_info
    assert q.validation_info["metric"] in {"article_count", "subcategory_count"}
    assert q.validation_info["category_name"] in CATEGORY_POOL


@pytest.mark.parametrize("seed", SEEDS)
def test_edit_count_generate(seed):
    q = WikipediaEditCountTemplate().generate(seed)
    assert q.question_text
    assert "en.wikipedia.org/wiki/" in q.start_url
    assert q.template_name == "wikipedia_edit_count"
    assert "article_title" in q.validation_info
    assert "days" in q.validation_info
    assert q.validation_info["days"] in TIME_WINDOWS
    assert q.validation_info["article_title"] in ARTICLE_POOL


def test_category_count_deterministic():
    tmpl = WikipediaCategoryCountTemplate()
    q1 = tmpl.generate(42)
    q2 = tmpl.generate(42)
    assert q1.question_text == q2.question_text
    assert q1.validation_info == q2.validation_info


def test_edit_count_deterministic():
    tmpl = WikipediaEditCountTemplate()
    q1 = tmpl.generate(42)
    q2 = tmpl.generate(42)
    assert q1.question_text == q2.question_text
    assert q1.validation_info == q2.validation_info


def test_category_count_variant_selects_metric():
    tmpl = WikipediaCategoryCountTemplate()
    metrics = list(CountMetric)
    for i, metric in enumerate(metrics):
        q = tmpl.generate(seed=1, variant=i)
        assert q.validation_info["metric"] == metric.api_field


@pytest.mark.parametrize("cls", [
    WikipediaCategoryCountTemplate,
    WikipediaEditCountTemplate,
])
def test_validation_info_values_are_serializable(cls):
    q = cls().generate(seed=1)
    for key, val in q.validation_info.items():
        assert isinstance(val, (str, int, float, bool, type(None))), (
            f"{cls.__name__}.validation_info['{key}'] = {type(val).__name__} "
            f"is not JSON-serializable"
        )


# ── 3. GT source and cache source ─────────────────────────────────────────────


@pytest.mark.parametrize("cls", [
    WikipediaCategoryCountTemplate,
    WikipediaEditCountTemplate,
])
def test_gt_source_is_page_only(cls):
    assert cls().get_gt_source() == GTSourceType.PAGE_ONLY


@pytest.mark.parametrize("cls", [
    WikipediaCategoryCountTemplate,
    WikipediaEditCountTemplate,
])
def test_cache_source_is_wikipedia(cls):
    assert cls.get_cache_source() == "wikipedia"


# ── 4. category_count GT logic ────────────────────────────────────────────────


def test_category_count_articles_found():
    tmpl = WikipediaCategoryCountTemplate()
    data = {"wp:cat1": _category_entry("Programming languages", article_count=700)}
    result = _run_gt(data, tmpl.get_ground_truth({
        "category_name": "Programming languages",
        "metric": "article_count",
        "metric_display": "articles",
    }))
    assert result.success is True
    assert result.value == "700"


def test_category_count_subcategories_found():
    tmpl = WikipediaCategoryCountTemplate()
    data = {"wp:cat2": _category_entry("Operating systems", article_count=50, subcategory_count=12)}
    result = _run_gt(data, tmpl.get_ground_truth({
        "category_name": "Operating systems",
        "metric": "subcategory_count",
        "metric_display": "subcategories",
    }))
    assert result.success is True
    assert result.value == "12"


def test_category_count_fuzzy_title_match():
    """Underscore and case variants of a category name should still match."""
    tmpl = WikipediaCategoryCountTemplate()
    # API returns with spaces; template stores with spaces; should match both ways
    data = {"wp:cat3": _category_entry("Nobel Prize in Physics laureates", article_count=220)}
    result = _run_gt(data, tmpl.get_ground_truth({
        "category_name": "Nobel_Prize_in_Physics_laureates",
        "metric": "article_count",
        "metric_display": "articles",
    }))
    assert result.success is True
    assert result.value == "220"


def test_category_count_not_collected_empty_data():
    tmpl = WikipediaCategoryCountTemplate()
    result = _run_gt({}, tmpl.get_ground_truth({
        "category_name": "Programming languages",
        "metric": "article_count",
        "metric_display": "articles",
    }))
    assert result.success is False


def test_category_count_not_collected_wrong_category():
    tmpl = WikipediaCategoryCountTemplate()
    data = {"wp:cat1": _category_entry("Python libraries", article_count=30)}
    result = _run_gt(data, tmpl.get_ground_truth({
        "category_name": "Programming languages",
        "metric": "article_count",
        "metric_display": "articles",
    }))
    assert result.success is False


def test_category_count_ignores_non_category_data():
    """History entries in collected data must not be mistaken for categories."""
    tmpl = WikipediaCategoryCountTemplate()
    data = {
        "wp:hist": _history_entry("Python (programming language)", [], "2024-01-01T00:00:00Z"),
    }
    result = _run_gt(data, tmpl.get_ground_truth({
        "category_name": "Programming languages",
        "metric": "article_count",
        "metric_display": "articles",
    }))
    assert result.success is False


def test_category_count_no_gt_collector():
    tmpl = WikipediaCategoryCountTemplate()
    set_current_gt_collector(None)
    result = asyncio.run(tmpl.get_ground_truth({
        "category_name": "X", "metric": "article_count", "metric_display": "articles",
    }))
    assert result.success is False
    assert "system_error" in (result.failure_type.value if result.failure_type else "")


# ── 5. edit_count GT logic ────────────────────────────────────────────────────


def _make_timestamps(fetched_at: datetime, days_back_list):
    """Return ISO strings for revisions at given days-back offsets."""
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return [(fetched_at - timedelta(days=d)).strftime(fmt) for d in days_back_list]


def test_edit_count_counts_within_window():
    tmpl = WikipediaEditCountTemplate()
    now = datetime(2024, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    # 3 edits within 7 days, 2 older
    timestamps = _make_timestamps(now, [1, 3, 6, 8, 15])
    data = {"wp:hist": _history_entry("Quantum mechanics", timestamps, now.strftime(fmt))}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Quantum mechanics", "days": 7,
    }))
    assert result.success is True
    assert result.value == "3"


def test_edit_count_14_day_window():
    tmpl = WikipediaEditCountTemplate()
    now = datetime(2024, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    # 5 within 14 days, 2 older
    timestamps = _make_timestamps(now, [1, 5, 10, 13, 13, 20, 30])
    data = {"wp:hist": _history_entry("Climate change", timestamps, now.strftime(fmt))}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Climate change", "days": 14,
    }))
    assert result.success is True
    assert result.value == "5"


def test_edit_count_zero_edits_in_window():
    tmpl = WikipediaEditCountTemplate()
    now = datetime(2024, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    # All edits older than 7 days
    timestamps = _make_timestamps(now, [10, 20, 30])
    data = {"wp:hist": _history_entry("Ancient Rome", timestamps, now.strftime(fmt))}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Ancient Rome", "days": 7,
    }))
    assert result.success is True
    assert result.value == "0"


def test_edit_count_empty_revisions():
    tmpl = WikipediaEditCountTemplate()
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    data = {"wp:hist": _history_entry("Some Article", [], "2024-04-02T12:00:00Z")}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Some Article", "days": 7,
    }))
    assert result.success is True
    assert result.value == "0"


def test_edit_count_fuzzy_title_match():
    """Underscore form of title should match space form stored in collected data."""
    tmpl = WikipediaEditCountTemplate()
    now = datetime(2024, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    timestamps = _make_timestamps(now, [1, 2])
    # Stored with spaces
    data = {"wp:hist": _history_entry("Quantum mechanics", timestamps, now.strftime(fmt))}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Quantum_mechanics", "days": 7,
    }))
    assert result.success is True
    assert result.value == "2"


def test_edit_count_not_collected_wrong_article():
    tmpl = WikipediaEditCountTemplate()
    now = datetime(2024, 4, 2, 12, 0, 0, tzinfo=timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    data = {"wp:hist": _history_entry("Wikipedia", _make_timestamps(now, [1]), now.strftime(fmt))}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Python (programming language)", "days": 7,
    }))
    assert result.success is False


def test_edit_count_not_collected_empty_data():
    tmpl = WikipediaEditCountTemplate()
    result = _run_gt({}, tmpl.get_ground_truth({
        "article_title": "Wikipedia", "days": 7,
    }))
    assert result.success is False


def test_edit_count_malformed_fetched_at_is_system_error():
    tmpl = WikipediaEditCountTemplate()
    data = {"wp:hist": {
        "type": "history",
        "article_title": "Wikipedia",
        "revisions": [{"timestamp": "2024-04-01T12:00:00Z"}],
        "fetched_at": "not-a-date",
    }}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Wikipedia", "days": 7,
    }))
    assert result.success is False
    assert result.failure_type is not None
    assert "system_error" in result.failure_type.value


def test_edit_count_ignores_non_history_data():
    """Category entries must not be mistaken for history entries."""
    tmpl = WikipediaEditCountTemplate()
    data = {"wp:cat": _category_entry("Wikipedia", article_count=100)}
    result = _run_gt(data, tmpl.get_ground_truth({
        "article_title": "Wikipedia", "days": 7,
    }))
    assert result.success is False


def test_edit_count_no_gt_collector():
    tmpl = WikipediaEditCountTemplate()
    set_current_gt_collector(None)
    result = asyncio.run(tmpl.get_ground_truth({"article_title": "X", "days": 7}))
    assert result.success is False
    assert result.failure_type is not None
    assert "system_error" in result.failure_type.value


# ── 6. WikipediaPlugin URL routing ────────────────────────────────────────────


@pytest.mark.parametrize("url, expected", [
    ("https://en.wikipedia.org/wiki/Category:Programming_languages", True),
    ("https://en.wikipedia.org/wiki/Category:2024_films", True),
    ("https://en.wikipedia.org/wiki/Quantum_mechanics?action=history", True),
    ("https://en.wikipedia.org/wiki/Python_(programming_language)?action=history", True),
    # Regular articles do NOT need API data
    ("https://en.wikipedia.org/wiki/Quantum_mechanics", False),
    ("https://en.wikipedia.org/wiki/Main_Page", False),
    # Namespace pages should be excluded
    ("https://en.wikipedia.org/wiki/Talk:Quantum_mechanics?action=history", False),
    ("https://en.wikipedia.org/wiki/Special:PageInfo/Quantum_mechanics", False),
    ("https://en.wikipedia.org/wiki/Wikipedia:About?action=history", False),
    # Non-wiki paths
    ("https://en.wikipedia.org/w/index.php?title=Foo", False),
    ("https://www.google.com/", False),
])
def test_plugin_needs_api_data(url, expected):
    plugin = WikipediaPlugin()
    assert plugin.needs_api_data(url) is expected, f"needs_api_data({url!r}) should be {expected}"


@pytest.mark.asyncio
async def test_plugin_fetch_dispatches_category():
    plugin = WikipediaPlugin()
    url = "https://en.wikipedia.org/wiki/Category:Programming_languages"
    fake_data = {
        "type": "category",
        "category_name": "Programming languages",
        "article_count": 700,
        "subcategory_count": 5,
    }
    with patch(
        "liveweb_arena.plugins.wikipedia.wikipedia.fetch_category_api_data",
        new=AsyncMock(return_value=fake_data),
    ) as mock_fetch:
        result = await plugin.fetch_api_data(url)
    mock_fetch.assert_called_once_with("Programming languages")
    assert result == fake_data


@pytest.mark.asyncio
async def test_plugin_fetch_dispatches_history():
    plugin = WikipediaPlugin()
    url = "https://en.wikipedia.org/wiki/Quantum_mechanics?action=history"
    fake_data = {
        "type": "history",
        "article_title": "Quantum mechanics",
        "revisions": [],
        "fetched_at": "2024-04-02T12:00:00Z",
    }
    with patch(
        "liveweb_arena.plugins.wikipedia.wikipedia.fetch_article_history_api_data",
        new=AsyncMock(return_value=fake_data),
    ) as mock_fetch:
        result = await plugin.fetch_api_data(url)
    mock_fetch.assert_called_once_with("Quantum mechanics")
    assert result == fake_data


@pytest.mark.asyncio
async def test_plugin_fetch_unknown_url_returns_empty():
    plugin = WikipediaPlugin()
    result = await plugin.fetch_api_data("https://en.wikipedia.org/wiki/Main_Page")
    assert result == {}


# ── 7. titles_match and normalize_title ───────────────────────────────────────


@pytest.mark.parametrize("a, b, expected", [
    ("Programming languages", "Programming languages", True),
    ("Programming_languages", "Programming languages", True),
    ("PROGRAMMING LANGUAGES", "programming languages", True),
    ("Nobel Prize in Physics laureates", "Nobel_Prize_in_Physics_laureates", True),
    # Length-ratio guard: short string must be ≥85% length of longer
    ("Poland", "Cities in Poland", False),
    ("Programming", "Programming languages", False),
    ("", "Programming languages", False),
    ("Quantum mechanics", "", False),
])
def test_titles_match(a, b, expected):
    assert titles_match(a, b) is expected


@pytest.mark.parametrize("value, expected", [
    ("Quantum mechanics", "quantum mechanics"),
    ("Quantum_mechanics", "quantum mechanics"),
    ("Nobel Prize in Physics laureates", "nobel prize in physics laureates"),
    ("C++ (programming language)", "c programming language"),
    ("  spaces  everywhere  ", "spaces everywhere"),
])
def test_normalize_title(value, expected):
    assert normalize_title(value) == expected


# ── 8. Pool size assertions ────────────────────────────────────────────────────


def test_category_pool_size():
    assert len(CATEGORY_POOL) >= 250, (
        f"CATEGORY_POOL has {len(CATEGORY_POOL)} entries; need ≥250 for 500 variants"
    )


def test_article_pool_size():
    assert len(ARTICLE_POOL) >= 200, (
        f"ARTICLE_POOL has {len(ARTICLE_POOL)} entries; need ≥200 for 600 variants"
    )


def test_no_duplicate_categories():
    assert len(CATEGORY_POOL) == len(set(CATEGORY_POOL)), "CATEGORY_POOL contains duplicates"


def test_no_duplicate_articles():
    assert len(ARTICLE_POOL) == len(set(ARTICLE_POOL)), "ARTICLE_POOL contains duplicates"
