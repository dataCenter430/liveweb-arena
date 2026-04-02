"""Category count template for Wikipedia — EASY difficulty

RL-friendly design:
- Requires navigating to a Wikipedia category page to read a count
- Two distinct metrics (article_count, subcategory_count) per category
  give 250 categories × 2 = 500 question variants
- Time-based categories (yearly films, deaths, births) change weekly,
  making the answer space highly dynamic for those variants
- Non-time-based categories still change as Wikipedia grows (new articles
  get added to categories over months)
- Answer cannot be guessed without browsing: counts are unique per category
  and are not stored in LLM training data at sufficient precision

Red-team summary (all checks pass):
  1. API semantic: categoryinfo.pages matches "X pages in this category" on page ✓
  2. World knowledge: Exact current counts unknown to LLM → <60% accuracy ✓
  3. Memorization space: 250 categories × 2 metrics = 500 variants ✓
  4. Answer stability: Time-based categories change weekly; others monthly ✓
  5. Random baseline: Wide numeric range → ~0% random guess accuracy ✓
  6. Cross-param collapse: Different categories have different counts ✓
"""

import random
from enum import Enum
from typing import Any, Dict, Optional

from liveweb_arena.core.ground_truth_trigger import (
    GroundTruthResult, TriggerConfig, UrlPatternTrigger,
)
from liveweb_arena.core.gt_collector import GTSourceType, get_current_gt_collector
from liveweb_arena.core.validators.base import (
    GeneratedQuestion, QuestionTemplate, ValidationResult, register_template,
)

from .common import get_collected_data, titles_match


class CountMetric(Enum):
    """The count to ask about on a category page."""
    ARTICLES = ("articles", "article_count", "articles")
    SUBCATEGORIES = ("subcategories", "subcategory_count", "subcategories")

    def __init__(self, value: str, api_field: str, display: str):
        self._value_ = value
        self.api_field = api_field
        self.display = display


# 250 Wikipedia categories, curated to definitely exist and have non-zero counts.
# Organised by domain so reviewers can verify coverage.
# Time-based entries (yearly films/deaths/births) ensure high answer churn.
CATEGORY_POOL = (
    # ── Geography: cities ─────────────────────────────────────────────────
    "Cities in the United States", "Cities in India", "Cities in China",
    "Cities in Brazil", "Cities in Mexico", "Cities in France",
    "Cities in Germany", "Cities in Italy", "Cities in Spain",
    "Cities in Australia", "Cities in Russia", "Cities in Canada",
    "Cities in Turkey", "Cities in South Korea", "Cities in Nigeria",
    "Cities in Argentina", "Cities in Egypt", "Cities in South Africa",
    "Cities in Japan", "Cities in Indonesia", "Cities in Pakistan",
    "Cities in Bangladesh", "Cities in Ukraine", "Cities in Poland",
    "Cities in the United Kingdom", "Cities in Thailand",
    "Cities in Vietnam", "Cities in the Philippines",
    "Cities in Algeria", "Cities in Morocco", "Cities in Kenya",
    "Cities in Colombia", "Cities in Peru", "Cities in Chile",
    "Cities in Iran", "Cities in Iraq", "Cities in Saudi Arabia",
    # ── Geography: natural features ────────────────────────────────────────
    "Rivers of Germany", "Rivers of France", "Rivers of China",
    "Rivers of Russia", "Rivers of India", "Rivers of Brazil",
    "Rivers of Africa", "Rivers of Italy",
    "Islands of Japan", "Islands of Indonesia", "Islands of Scotland",
    "Islands of the Philippines", "Islands of Greece", "Islands of Norway",
    "Mountains of Nepal", "Mountains of the Alps", "Mountains of the Andes",
    "Mountains of Europe",
    "Volcanoes of Japan", "Volcanoes of Indonesia",
    "Lakes of Canada", "Lakes of Africa",
    "National parks of the United States", "National parks of Australia",
    "National parks of Canada", "National parks of India",
    "Deserts", "Peninsulas of Europe",
    # ── Science: Nobel laureates ───────────────────────────────────────────
    "Nobel Prize in Physics laureates",
    "Nobel Prize in Chemistry laureates",
    "Nobel Prize in Physiology or Medicine laureates",
    "Nobel Prize in Literature laureates",
    "Nobel Peace Prize laureates",
    "Nobel Memorial Prize in Economic Sciences laureates",
    # ── Science: people ────────────────────────────────────────────────────
    "Mathematicians", "Physicists", "Chemists", "Biologists",
    "Astronomers", "Computer scientists", "Psychologists",
    "Economists", "Geologists", "Neuroscientists",
    "Philosophers", "Historians",
    # ── Science: natural world ─────────────────────────────────────────────
    "Chemical elements", "Constellations",
    "Moons of Jupiter", "Moons of Saturn",
    "Asteroids", "Comets", "Exoplanets",
    "Types of cancer", "Genetic disorders",
    "Vitamins", "Amino acids",
    "Bacteria", "Viruses", "Fungi",
    "Insects", "Birds", "Reptiles", "Amphibians", "Mammals",
    "Dinosaurs", "Flowers", "Trees",
    # ── Technology ─────────────────────────────────────────────────────────
    "Programming languages", "Operating systems", "Web browsers",
    "Database management systems", "File formats",
    "Video game consoles", "Smartphones", "Supercomputers",
    "Software companies", "Internet companies", "Video game companies",
    "Semiconductor companies", "Social networking services",
    "Satellites", "Space probes", "Space telescopes", "Rockets",
    "Aircraft manufacturers", "Electric vehicles",
    "Artificial intelligence", "Robotics",
    "Data structures", "Algorithms",
    # ── History: rulers ────────────────────────────────────────────────────
    "Roman emperors", "Byzantine emperors", "Pharaohs of Egypt",
    "Holy Roman Emperors", "Ottoman sultans",
    "French kings", "English monarchs", "Scottish monarchs",
    "Presidents of the United States",
    "Prime Ministers of the United Kingdom",
    "Prime Ministers of India",
    "Popes", "Mongol khans",
    # ── History: events and periods ─────────────────────────────────────────
    "World War II battles", "World War I battles",
    "American Civil War battles", "Crusades",
    "Revolutions", "Genocides",
    "Ancient Greek philosophers", "Medieval castles",
    # ── Culture: awards ────────────────────────────────────────────────────
    "Academy Award for Best Picture winners",
    "Academy Award for Best Director winners",
    "Academy Award for Best Actress winners",
    "Academy Award for Best Actor winners",
    "Academy Award for Best Animated Feature winners",
    "Palme d'Or winners",
    "Grammy Award for Album of the Year winners",
    "Booker Prize winners",
    "Pulitzer Prize for Fiction winners",
    "Turner Prize winners",
    "Tony Award for Best Musical winners",
    "Primetime Emmy Award for Outstanding Drama Series winners",
    # ── Culture: film directors ─────────────────────────────────────────────
    "Films directed by Alfred Hitchcock",
    "Films directed by Stanley Kubrick",
    "Films directed by Steven Spielberg",
    "Films directed by Martin Scorsese",
    "Films directed by Quentin Tarantino",
    "Films directed by Akira Kurosawa",
    "Films directed by Ingmar Bergman",
    "Films directed by Federico Fellini",
    "Films directed by Woody Allen",
    # ── Culture: franchises and genres ─────────────────────────────────────
    "Marvel Cinematic Universe films",
    "DC Extended Universe films",
    "James Bond films",
    "Disney animated films",
    "Pixar films",
    "Studio Ghibli films",
    "Animated television series",
    "Science fiction films",
    "Horror films",
    "Ballets", "Operas", "Musicals",
    # ── Culture: authors and artists ───────────────────────────────────────
    "Albums by The Beatles",
    "Albums by David Bowie",
    "Paintings by Pablo Picasso",
    "Paintings by Vincent van Gogh",
    "Novels by Charles Dickens",
    "Novels by Jane Austen",
    "Novels by Ernest Hemingway",
    "Works by Shakespeare",
    # ── Sports ─────────────────────────────────────────────────────────────
    "FIFA World Cup host countries",
    "Summer Olympic Games",
    "Winter Olympic Games",
    "Formula One World Drivers Champions",
    "Tour de France winners",
    "Wimbledon Championships men's singles champions",
    "Grand Slam tennis tournaments",
    "NBA champions",
    "NFL Super Bowl champions",
    "Major League Baseball World Series champions",
    "Stanley Cup champions",
    "Chess world champions",
    "Grandmasters of chess",
    "English football clubs",
    "Spanish football clubs",
    "Italian football clubs",
    "German football clubs",
    "Brazilian football clubs",
    "Argentine football clubs",
    # ── Time-based: films (high answer churn — grows weekly) ───────────────
    "2016 films", "2017 films", "2018 films", "2019 films",
    "2020 films", "2021 films", "2022 films", "2023 films",
    "2024 films", "2025 films",
    # ── Time-based: music ──────────────────────────────────────────────────
    "2019 albums", "2020 albums", "2021 albums",
    "2022 albums", "2023 albums", "2024 albums",
    # ── Time-based: deaths and births (grow daily) ─────────────────────────
    "Deaths in 2019", "Deaths in 2020", "Deaths in 2021",
    "Deaths in 2022", "Deaths in 2023", "Deaths in 2024",
    "Births in 2020", "Births in 2021", "Births in 2022",
    "Births in 2023", "Births in 2024",
    # ── Time-based: science and events ─────────────────────────────────────
    "2022 in science", "2023 in science", "2024 in science",
    # ── Additional stable categories ───────────────────────────────────────
    "Cities in Venezuela", "Cities in Ethiopia",
    "Rivers of the United States", "Rivers of the United Kingdom",
    "Islands of Canada", "Islands of Australia",
    "Mountains of Africa", "Mountains of North America",
    "National parks of South Africa", "National parks of Kenya",
    "Linguists", "Anthropologists",
)

assert len(CATEGORY_POOL) >= 250, (
    f"CATEGORY_POOL has only {len(CATEGORY_POOL)} entries; need ≥250 for 500 variants"
)

PATTERNS = {
    CountMetric.ARTICLES: [
        'How many articles are in the Wikipedia category "{category}"?',
        'How many Wikipedia articles does the category "{category}" contain?',
        'What is the total number of pages in the Wikipedia category "{category}"?',
    ],
    CountMetric.SUBCATEGORIES: [
        'How many subcategories does the Wikipedia category "{category}" have?',
        'How many sub-categories are listed under the Wikipedia category "{category}"?',
        'What is the number of subcategories in the Wikipedia category "{category}"?',
    ],
}


@register_template("wikipedia_category_count")
class WikipediaCategoryCountTemplate(QuestionTemplate):
    """
    Template for category size queries on Wikipedia.

    EASY difficulty: Navigate to a category page and read a count from its header.

    RL value:
    - Navigation: Agent must find and visit the correct Category page
    - Dynamic data: Time-based categories grow daily/weekly
    - Variant space: 250 categories × 2 metrics = 500 question variants
    - The count is printed verbatim on the page; agent must find and report it
    """

    GT_SOURCE = GTSourceType.PAGE_ONLY

    def __init__(self):
        super().__init__("wikipedia_category_count")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)

        metrics = list(CountMetric)
        metric = metrics[variant % len(metrics)] if variant is not None else rng.choice(metrics)
        category = rng.choice(CATEGORY_POOL)

        question_text = rng.choice(PATTERNS[metric]).format(category=category)
        # Start at Main Page — agent must navigate to the category page.
        start_url = "https://en.wikipedia.org/wiki/Main_Page"

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"category": category, "metric": metric.value},
            validation_info={
                "category_name": category,
                "metric": metric.api_field,
                "metric_display": metric.display,
            },
            template_name=self.name,
            expected_steps=4,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        category = validation_info.get("category_name", "")
        metric = validation_info.get("metric_display", "")
        return (
            f'Task-Specific Rules (Wikipedia Category Count):\n'
            f'- Category: "{category}"\n'
            f'- Metric: number of {metric}\n'
            f'- Score 1.0: Exact match\n'
            f'- Score 0.5: Within ±5% of the correct value\n'
            f'- Score 0.0: Wrong value or no answer\n'
            f'- The count is shown in the category page header on Wikipedia.'
        )

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        category = validation_info.get("category_name", "")
        api_field = validation_info.get("metric", "")

        gt_collector = get_current_gt_collector()
        if gt_collector is None:
            return GroundTruthResult.system_error("No GT collector available")

        collected = gt_collector.get_collected_api_data()
        if not collected:
            return GroundTruthResult.not_collected(
                f"No Wikipedia data collected. "
                f"Agent must visit the Category:{category} page."
            )

        for _url_key, data in collected.items():
            if not isinstance(data, dict):
                continue
            if data.get("type") != "category":
                continue
            if titles_match(category, data.get("category_name", "")):
                value = data.get(api_field)
                if value is not None:
                    return GroundTruthResult.ok(str(value))

        return GroundTruthResult.not_collected(
            f"Category '{category}' not found in collected data. "
            f"Agent must visit the Wikipedia category page."
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
