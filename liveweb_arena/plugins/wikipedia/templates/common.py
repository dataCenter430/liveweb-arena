"""Shared helpers for Wikipedia templates."""

from typing import Any, Dict, Optional

from liveweb_arena.core.gt_collector import get_current_gt_collector


def normalize_title(value: str) -> str:
    """
    Normalize a Wikipedia title for robust matching.

    Converts underscores to spaces, collapses whitespace, lower-cases,
    and strips non-alphanumeric characters.  Handles "Category:" prefix
    transparently so callers can pass either form.
    """
    value = value.replace("_", " ")
    value = " ".join(value.split())
    return "".join(ch.lower() for ch in value if ch.isalnum() or ch == " ").strip()


def titles_match(expected: str, actual: str) -> bool:
    """
    Fuzzy title comparison resilient to casing, underscore/space, and
    punctuation differences.

    Uses the same 85% length-ratio substring guard as the Open Library
    common module so that short article names don't false-match long ones
    (e.g. "Poland" should not match "Cities in Poland").
    """
    lhs = normalize_title(expected)
    rhs = normalize_title(actual)
    if not lhs or not rhs:
        return False
    if lhs == rhs:
        return True
    shorter, longer = (lhs, rhs) if len(lhs) <= len(rhs) else (rhs, lhs)
    if shorter not in longer:
        return False
    return len(shorter) / len(longer) >= 0.85


def get_collected_data() -> Optional[Dict[str, Any]]:
    """Return all collected API data from the current GT collector, or None."""
    collector = get_current_gt_collector()
    if collector is None:
        return None
    return collector.get_collected_api_data()
