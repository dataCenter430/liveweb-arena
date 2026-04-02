"""Shared helpers for Hacker News templates."""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from liveweb_arena.core.ground_truth_trigger import GroundTruthResult
from liveweb_arena.core.gt_collector import get_current_gt_collector

_NON_STORY_PREFIXES = ("user:", "hn_category:", "external:", "hn_external:")


def get_collected_data() -> Tuple[Optional[Dict[str, Dict[str, Any]]], Optional[GroundTruthResult]]:
    """Get collected API data for PAGE_ONLY templates."""
    gt_collector = get_current_gt_collector()
    if gt_collector is None:
        return None, GroundTruthResult.system_error("No GT collector")

    collected = gt_collector.get_collected_api_data()
    if not collected:
        return None, GroundTruthResult.not_collected("No HN data collected")
    return collected, None


def _is_story_record(key: str, data: Any) -> bool:
    """Return True when this key/data pair looks like an HN story record."""
    if not isinstance(data, dict):
        return False
    if key.startswith(_NON_STORY_PREFIXES):
        return False
    return True


def get_homepage_stories(
    *,
    story_count: int,
    required_fields: Sequence[str],
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[GroundTruthResult]]:
    """
    Return top-N homepage stories with required fields.

    Story records are identified from collector entries using:
    - key not prefixed with user:/hn_category:/external:/hn_external:
    - dict payload with integer rank in [1, story_count]
    - all required_fields present and not None
    """
    collected, failure = get_collected_data()
    if failure is not None:
        return None, failure

    stories: List[Dict[str, Any]] = []
    for key, data in collected.items():
        if not _is_story_record(key, data):
            continue

        rank_raw = data.get("rank")
        if not isinstance(rank_raw, int) or rank_raw < 1 or rank_raw > story_count:
            continue

        if any(data.get(field) is None for field in required_fields):
            continue

        stories.append(data)

    rank_to_story: Dict[int, Dict[str, Any]] = {}
    duplicate_ranks: List[int] = []
    for story in stories:
        rank = story["rank"]
        if rank in rank_to_story:
            duplicate_ranks.append(rank)
            continue
        rank_to_story[rank] = story

    if duplicate_ranks:
        deduped = sorted(set(duplicate_ranks))
        return None, GroundTruthResult.system_error(
            f"Duplicate homepage ranks in collected data: {deduped}"
        )

    required_ranks = list(range(1, story_count + 1))
    missing_ranks = [rank for rank in required_ranks if rank not in rank_to_story]
    if missing_ranks:
        available_ranks = sorted(rank_to_story.keys())
        return None, GroundTruthResult.not_collected(
            f"Only {len(rank_to_story)} stories have complete data (need {story_count}). "
            f"Available ranks: {available_ranks}. "
            f"Missing ranks: {missing_ranks}. "
            f"Agent may need to visit more story detail pages."
        )

    ordered = [rank_to_story[rank] for rank in required_ranks]
    return ordered, None


def get_category_stories(
    *,
    category_slug: str,
    upto_rank: int,
    required_fields: Sequence[str],
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[GroundTruthResult]]:
    """Return category stories (ask/show/jobs) for ranks <= upto_rank."""
    collected, failure = get_collected_data()
    if failure is not None:
        return None, failure

    category_key = f"hn_category:{category_slug}"
    entry = collected.get(category_key)
    if not isinstance(entry, dict):
        return None, GroundTruthResult.not_collected(
            f"Category data not collected for '{category_slug}'. "
            f"Agent needs to visit {category_slug} page."
        )

    raw_stories = entry.get("stories")
    if not isinstance(raw_stories, dict):
        return None, GroundTruthResult.not_collected(
            f"Collected category '{category_slug}' has no story map"
        )

    stories: List[Dict[str, Any]] = []
    for data in raw_stories.values():
        if not isinstance(data, dict):
            continue

        rank_raw = data.get("rank")
        if not isinstance(rank_raw, int) or rank_raw < 1 or rank_raw > upto_rank:
            continue

        if any(data.get(field) is None for field in required_fields):
            continue

        stories.append(data)

    rank_to_story: Dict[int, Dict[str, Any]] = {}
    duplicate_ranks: List[int] = []
    for story in stories:
        rank = story["rank"]
        if rank in rank_to_story:
            duplicate_ranks.append(rank)
            continue
        rank_to_story[rank] = story

    if duplicate_ranks:
        deduped = sorted(set(duplicate_ranks))
        return None, GroundTruthResult.system_error(
            f"Duplicate category ranks in collected data for '{category_slug}': {deduped}"
        )

    ordered = [rank_to_story[rank] for rank in sorted(rank_to_story.keys())]
    return ordered, None


def extract_first_number(
    answer: str,
    *,
    signed: bool = False,
    allow_float: bool = False,
) -> Optional[float]:
    """Extract first numeric token from answer text."""
    if allow_float:
        pattern = r"-?\d+(?:\.\d+)?" if signed else r"\d+(?:\.\d+)?"
    else:
        pattern = r"-?\d+" if signed else r"\d+"

    matches = re.findall(pattern, answer)
    if not matches:
        return None

    token = matches[0]
    try:
        return float(token) if allow_float else float(int(token))
    except ValueError:
        return None


def normalize_text(value: str) -> str:
    """Normalize text for loose title matching."""
    collapsed = " ".join(value.split())
    return "".join(ch.lower() for ch in collapsed if ch.isalnum() or ch == " ").strip()


def title_matches(expected: str, answer: str) -> bool:
    """Return True when answer appears to contain the expected title."""
    expected_norm = normalize_text(expected)
    answer_norm = normalize_text(answer)
    if not expected_norm or not answer_norm:
        return False
    if expected_norm == answer_norm:
        return True
    if expected_norm in answer_norm:
        shorter, longer = expected_norm, answer_norm
    elif answer_norm in expected_norm:
        shorter, longer = answer_norm, expected_norm
    else:
        return False
    return len(shorter) / len(longer) >= 0.75


def title_partial_match(expected: str, answer: str, *, min_ratio: float = 0.6) -> bool:
    """Return True when enough non-trivial expected tokens appear in the answer."""
    expected_tokens = [tok for tok in expected.lower().split() if len(tok) > 3]
    if not expected_tokens:
        expected_norm = normalize_text(expected)
        answer_norm = normalize_text(answer)
        if not expected_norm or not answer_norm:
            return False
        return expected_norm in answer_norm or answer_norm in expected_norm
    answer_lower = answer.lower()
    token_hits = sum(1 for tok in expected_tokens if tok in answer_lower)
    return (token_hits / len(expected_tokens)) >= min_ratio
