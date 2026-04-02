"""Article edit count template for Wikipedia — MEDIUM difficulty

RL-friendly design:
- Requires navigating from an article to its revision history page
- Agent must count revisions within a time window from the history page
- Highly dynamic: counts change every time any editor saves a change
- 200 articles × 3 time windows = 600 question variants
- All answers are exact integers; LLM world knowledge cannot supply them

Red-team summary (all checks pass):
  1. API semantic: rvprop=timestamp matches timestamps shown on ?action=history ✓
  2. World knowledge: Edit counts unknown to LLM, change daily → <60% accuracy ✓
  3. Memorization space: 200 articles × 3 windows = 600 variants ✓
  4. Answer stability: Changes every time an edit is saved (hours or less) ✓
  5. Random baseline: Integer with wide range → ~0% random guess accuracy ✓
  6. Cross-param collapse: Different articles have different edit frequencies ✓
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from liveweb_arena.core.ground_truth_trigger import (
    GroundTruthResult, TriggerConfig, UrlPatternTrigger,
)
from liveweb_arena.core.gt_collector import GTSourceType, get_current_gt_collector
from liveweb_arena.core.validators.base import (
    GeneratedQuestion, QuestionTemplate, ValidationResult, register_template,
)

from .common import get_collected_data, titles_match

# 200 articles with consistent edit activity across diverse topics.
# Each is a well-known English Wikipedia article that receives regular edits.
ARTICLE_POOL = (
    # ── Technology and companies ─────────────────────────────────────────
    "Wikipedia", "Artificial intelligence", "Machine learning",
    "Deep learning", "ChatGPT", "OpenAI", "Microsoft",
    "Apple Inc.", "Google", "Amazon (company)", "Meta Platforms",
    "Tesla, Inc.", "SpaceX", "YouTube", "Netflix",
    "Spotify", "TikTok", "Instagram", "Reddit", "Twitter",
    "Elon Musk", "Mark Zuckerberg", "Jeff Bezos", "Bill Gates",
    "Python (programming language)", "JavaScript", "Linux",
    "Microsoft Windows", "Android (operating system)", "iOS",
    "Internet", "World Wide Web", "Bitcoin", "Ethereum",
    "Cryptocurrency", "Blockchain", "Electric vehicle",
    "Renewable energy", "Solar energy", "Wind power",
    # ── Politics and world affairs ───────────────────────────────────────
    "United States", "Russia", "China", "United Kingdom",
    "European Union", "Ukraine", "Israel", "Palestine",
    "Joe Biden", "Donald Trump", "Vladimir Putin", "Xi Jinping",
    "Barack Obama", "Emmanuel Macron",
    "War in Ukraine", "Israeli–Palestinian conflict",
    "NATO", "United Nations", "World Health Organization",
    "Taliban", "Afghanistan", "Syria", "Iran", "North Korea",
    "Democracy", "Authoritarianism", "Fascism",
    "Communism", "Capitalism", "Socialism",
    "Immigration", "Refugee", "Human rights",
    # ── History ──────────────────────────────────────────────────────────
    "World War II", "World War I", "The Holocaust",
    "Cold War", "American Revolution", "French Revolution",
    "Russian Revolution", "Civil rights movement",
    "Slavery", "Colonialism",
    "Roman Empire", "British Empire", "Mongol Empire", "Ottoman Empire",
    "September 11 attacks", "Iraq War", "Vietnam War",
    "Korean War", "Nuclear weapon", "Space Race",
    # ── Science and medicine ─────────────────────────────────────────────
    "COVID-19 pandemic", "SARS-CoV-2",
    "Climate change", "Global warming", "Greenhouse gas",
    "Evolution", "Natural selection", "DNA",
    "Genetics", "CRISPR", "Cancer", "Diabetes",
    "Mental health", "Depression (mood)", "Anxiety disorder",
    "Autism", "Alzheimer's disease", "Antibiotic resistance",
    "HIV/AIDS", "Malaria", "Influenza",
    "Theory of relativity", "Quantum mechanics", "Big Bang",
    "Black hole", "Dark matter",
    # ── Culture and entertainment ─────────────────────────────────────────
    "Taylor Swift", "Beyoncé", "Drake (musician)",
    "The Beatles", "Michael Jackson", "Elvis Presley",
    "Lady Gaga", "Rihanna", "Ed Sheeran",
    "Star Wars", "Marvel Cinematic Universe", "Harry Potter",
    "The Lord of the Rings", "Game of Thrones",
    "Breaking Bad", "The Office (American TV series)",
    "Friends (TV series)", "Stranger Things",
    "Fortnite", "Minecraft", "Grand Theft Auto",
    "Pokémon", "Super Mario",
    # ── Sports ───────────────────────────────────────────────────────────
    "FIFA World Cup", "Summer Olympic Games", "Association football",
    "Basketball", "Tennis", "Cricket", "Rugby union",
    "Formula One", "National Football League",
    "LeBron James", "Cristiano Ronaldo", "Lionel Messi",
    "Novak Djokovic", "Serena Williams", "Roger Federer",
    "Tiger Woods", "Michael Jordan",
    # ── Social and economic topics ────────────────────────────────────────
    "Feminism", "Racism", "Antisemitism",
    "Poverty", "Inequality", "Free speech",
    "Inflation", "Recession", "Stock market",
    "Globalization", "Free trade",
    # ── Additional high-traffic articles ─────────────────────────────────
    "United States Constitution", "French language", "Jesus",
    "Muhammad", "Adolf Hitler", "Joseph Stalin",
    "Ancient Rome", "Ancient Egypt", "Ancient Greece",
    "Philosophy", "Mathematics", "Physics", "Chemistry", "Biology",
    "Astronomy", "Geography", "History",
    "India", "Brazil", "Germany", "France", "Japan", "Pakistan",
    "Indonesia", "Nigeria", "Bangladesh", "Mexico",
    "Climate engineering", "Nuclear power", "Vaccine",
    "COVID-19 vaccine",
)

assert len(ARTICLE_POOL) >= 200, (
    f"ARTICLE_POOL has only {len(ARTICLE_POOL)} entries; need ≥200 for 600 variants"
)

TIME_WINDOWS = (7, 14, 30)  # days

PATTERNS = {
    7: [
        'How many times has the Wikipedia article "{title}" been edited in the past 7 days?',
        'How many edits has the Wikipedia article "{title}" received in the last 7 days?',
        'In the past 7 days, how many revisions were made to the Wikipedia article "{title}"?',
    ],
    14: [
        'How many times has the Wikipedia article "{title}" been edited in the past 14 days?',
        'How many edits has the Wikipedia article "{title}" received in the last 14 days?',
        'In the past 14 days, how many revisions were made to the Wikipedia article "{title}"?',
    ],
    30: [
        'How many times has the Wikipedia article "{title}" been edited in the past 30 days?',
        'How many edits has the Wikipedia article "{title}" received in the last 30 days?',
        'In the past 30 days, how many revisions were made to the Wikipedia article "{title}"?',
    ],
}


@register_template("wikipedia_edit_count")
class WikipediaEditCountTemplate(QuestionTemplate):
    """
    Template for article revision-count queries on Wikipedia.

    MEDIUM difficulty: Find an article, open its history page, and count
    revisions within a rolling time window.

    RL value:
    - Multi-step navigation: article → "View history" tab → count dated entries
    - Dynamic data: edit counts change whenever any editor saves a revision
    - Large variant space: 200 articles × 3 time windows = 600 variants
    - Requires date arithmetic: agent must distinguish edits by timestamp
    """

    GT_SOURCE = GTSourceType.PAGE_ONLY

    def __init__(self):
        super().__init__("wikipedia_edit_count")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)

        windows = list(TIME_WINDOWS)
        days = windows[variant % len(windows)] if variant is not None else rng.choice(windows)
        title = rng.choice(ARTICLE_POOL)

        question_text = rng.choice(PATTERNS[days]).format(title=title)
        article_slug = title.replace(" ", "_")
        # Start at the article page; agent must click "View history" to reach
        # the history page where the revision timestamps are visible.
        start_url = f"https://en.wikipedia.org/wiki/{article_slug}"

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"title": title, "days": days},
            validation_info={
                "article_title": title,
                "days": days,
            },
            template_name=self.name,
            expected_steps=6,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        title = validation_info.get("article_title", "")
        days = validation_info.get("days", "")
        return (
            f'Task-Specific Rules (Wikipedia Edit Count):\n'
            f'- Article: "{title}"\n'
            f'- Time window: past {days} days\n'
            f'- Score 1.0: Within ±3 of the correct edit count\n'
            f'- Score 0.5: Within ±10% of the correct value\n'
            f'- Score 0.0: Wrong value or no answer\n'
            f'- The revision history is on the article\'s "View history" tab.\n'
            f'- Agent must count entries whose timestamp falls within the window.'
        )

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        title = validation_info.get("article_title", "")
        days = int(validation_info.get("days", 7))

        gt_collector = get_current_gt_collector()
        if gt_collector is None:
            return GroundTruthResult.system_error("No GT collector available")

        collected = gt_collector.get_collected_api_data()
        if not collected:
            return GroundTruthResult.not_collected(
                f"No Wikipedia data collected. "
                f"Agent must visit the history page of '{title}'."
            )

        for _url_key, data in collected.items():
            if not isinstance(data, dict):
                continue
            if data.get("type") != "history":
                continue
            if not titles_match(title, data.get("article_title", "")):
                continue

            revisions = data.get("revisions")
            fetched_at_str = data.get("fetched_at")
            if not isinstance(revisions, list) or not fetched_at_str:
                return GroundTruthResult.system_error(
                    f"Malformed history data for '{title}': "
                    f"missing revisions or fetched_at"
                )

            # Parse reference time. fetched_at is stored as "YYYY-MM-DDTHH:MM:SSZ".
            try:
                fetched_at = datetime.strptime(fetched_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError as e:
                return GroundTruthResult.system_error(
                    f"Cannot parse fetched_at '{fetched_at_str}': {e}"
                )

            cutoff = fetched_at - timedelta(days=days)
            # ISO 8601 UTC timestamps sort lexicographically — string comparison is valid.
            cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
            count = sum(1 for r in revisions if r.get("timestamp", "") >= cutoff_str)

            return GroundTruthResult.ok(str(count))

        return GroundTruthResult.not_collected(
            f"History for article '{title}' not found in collected data. "
            f"Agent must navigate to the article's 'View history' tab."
        )

    async def validate_answer(
        self, answer: str, validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Not used — the pipeline uses LLM-based validation via get_validation_rules()."""
        return ValidationResult(
            score=0.0, is_correct=False, expected=None, actual=answer,
            details="Use LLM validation",
        )

    def get_ground_truth_trigger(self, validation_info: dict) -> TriggerConfig:
        trigger = UrlPatternTrigger(domains=["en.wikipedia.org", "wikipedia.org"])
        return TriggerConfig(trigger=trigger)

    @classmethod
    def get_cache_source(cls) -> str:
        return "wikipedia"

    def get_gt_source(self) -> GTSourceType:
        return self.GT_SOURCE
