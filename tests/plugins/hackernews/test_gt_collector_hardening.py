"""Tests for GT collector homepage refresh hardening."""

import asyncio

from liveweb_arena.core.gt_collector import GTCollector


def test_homepage_refresh_clears_stale_ranks():
    """Stories that drop off homepage must have their rank cleared."""
    collector = GTCollector(subtasks=[])
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {
            "stories": {
                "101": {"id": 101, "rank": 1, "title": "A", "score": 100, "descendants": 10},
                "102": {"id": 102, "rank": 2, "title": "B", "score": 90, "descendants": 9},
            }
        },
    ))
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {
            "stories": {
                "201": {"id": 201, "rank": 1, "title": "C", "score": 80, "descendants": 8},
                "202": {"id": 202, "rank": 2, "title": "D", "score": 70, "descendants": 7},
            }
        },
    ))

    data = collector.get_collected_api_data()
    assert data["101"].get("rank") is None
    assert data["102"].get("rank") is None
    assert data["201"]["rank"] == 1
    assert data["202"]["rank"] == 2


def test_homepage_refresh_preserves_detail_page_fields():
    """Homepage refresh must update rank but not overwrite detail-page authority."""
    collector = GTCollector(subtasks=[])
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/item?id=101",
        "",
        {"id": 101, "title": "A", "score": 123, "descendants": 45},
    ))
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {"stories": {"101": {"id": 101, "rank": 3, "score": 100, "descendants": 10}}},
    ))

    data = collector.get_collected_api_data()["101"]
    assert data["rank"] == 3
    assert data["score"] == 123
    assert data["descendants"] == 45


def test_homepage_refresh_adds_new_fields_from_homepage():
    """Homepage data should fill in fields not yet present from detail pages."""
    collector = GTCollector(subtasks=[])
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/item?id=101",
        "",
        {"id": 101, "title": "A", "score": 123},
    ))
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {"stories": {"101": {"id": 101, "rank": 1, "score": 100, "descendants": 10}}},
    ))

    data = collector.get_collected_api_data()["101"]
    assert data["rank"] == 1
    assert data["score"] == 123
    assert data["descendants"] == 10


def test_stale_rank_clearing_skips_non_story_entries():
    """Category, user, and external entries must not have rank cleared."""
    collector = GTCollector(subtasks=[])
    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {
            "stories": {
                "101": {"id": 101, "rank": 1, "title": "A", "score": 100, "descendants": 10},
            }
        },
    ))
    collector._collected_api_data["user:alice"] = {"rank": 1, "user": True}
    collector._collected_api_data["hn_category:ask"] = {"rank": 1, "category": True}

    asyncio.run(collector.on_page_visit(
        "https://news.ycombinator.com/",
        "",
        {
            "stories": {
                "201": {"id": 201, "rank": 1, "title": "B", "score": 80, "descendants": 8},
            }
        },
    ))

    data = collector.get_collected_api_data()
    assert data["user:alice"]["rank"] == 1
    assert data["hn_category:ask"]["rank"] == 1
    assert data["101"].get("rank") is None
    assert data["201"]["rank"] == 1
