"""Recent works template for TMDB - MEDIUM-HARD DIFFICULTY (Anti-memorization)"""

import random
from datetime import datetime
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig, GroundTruthResult
)
from ..api_client import TMDBClient


class RecentWorksPersonSpec:
    """Person spec for recent works queries"""
    def __init__(self, person_id: str, name: str, role: str):
        self.person_id = person_id
        self.name = name
        self.role = role  # "directing" or "acting"


class RecentWorksPersonVariable:
    """Variable for people with recent work"""

    PERSONS: List[RecentWorksPersonSpec] = [
        # Active directors
        RecentWorksPersonSpec("525", "Christopher Nolan", "directing"),
        RecentWorksPersonSpec("138", "Quentin Tarantino", "directing"),
        RecentWorksPersonSpec("1032", "Martin Scorsese", "directing"),
        RecentWorksPersonSpec("5655", "Ridley Scott", "directing"),
        RecentWorksPersonSpec("1884", "Denis Villeneuve", "directing"),
        RecentWorksPersonSpec("7467", "David Fincher", "directing"),
        RecentWorksPersonSpec("488", "Steven Spielberg", "directing"),
        RecentWorksPersonSpec("5174", "Guillermo del Toro", "directing"),
        RecentWorksPersonSpec("17825", "Jordan Peele", "directing"),
        RecentWorksPersonSpec("62861", "Greta Gerwig", "directing"),
        # Active actors with many recent films
        RecentWorksPersonSpec("6193", "Leonardo DiCaprio", "acting"),
        RecentWorksPersonSpec("500", "Tom Cruise", "acting"),
        RecentWorksPersonSpec("2524", "Robert Downey Jr.", "acting"),
        RecentWorksPersonSpec("17419", "Bryan Cranston", "acting"),
        RecentWorksPersonSpec("17052", "Scarlett Johansson", "acting"),
        RecentWorksPersonSpec("1136406", "Tom Holland", "acting"),
        RecentWorksPersonSpec("1245", "Scarlett Johansson", "acting"),
        RecentWorksPersonSpec("74568", "Chris Hemsworth", "acting"),
        RecentWorksPersonSpec("103", "Mark Ruffalo", "acting"),
        RecentWorksPersonSpec("505710", "Zendaya", "acting"),
        RecentWorksPersonSpec("1253360", "Timothée Chalamet", "acting"),
        RecentWorksPersonSpec("234352", "Margot Robbie", "acting"),
        RecentWorksPersonSpec("90633", "Gal Gadot", "acting"),
        RecentWorksPersonSpec("1373737", "Florence Pugh", "acting"),
    ]

    def __init__(self, role_filter: str = None):
        if role_filter:
            self.persons = [p for p in self.PERSONS if p.role == role_filter]
        else:
            self.persons = self.PERSONS

    def sample(self, rng: random.Random) -> RecentWorksPersonSpec:
        return rng.choice(self.persons)


class RecentWorksQueryType:
    """Types of recent works queries"""
    COUNT_SINCE_YEAR = "count_since_year"      # Movies since 2020, 2018, etc.
    COUNT_IN_DECADE = "count_in_decade"        # Movies in 2020s, 2010s
    MOST_RECENT = "most_recent"                # Most recent movie


@register_template("tmdb_recent_works")
class TMDBRecentWorksTemplate(QuestionTemplate):
    """
    Template for time-sensitive queries - MEDIUM-HARD DIFFICULTY.

    These questions cannot be memorized because:
    1. New movies are constantly being added
    2. The counts change over time
    3. The "most recent" movie changes

    Examples:
    - How many movies has Christopher Nolan directed since 2015?
    - How many films has Tom Cruise appeared in during the 2020s?
    - What is Leonardo DiCaprio's most recent movie?
    """

    COUNT_SINCE_PATTERNS = [
        "How many movies has {person} {action} since {year}?",
        "How many films has {person} {action} from {year} onwards?",
        "Count the movies {person} has {action} since {year}.",
    ]

    COUNT_DECADE_PATTERNS = [
        "How many movies has {person} {action} in the {decade}?",
        "How many films did {person} {action_past} during the {decade}?",
    ]

    MOST_RECENT_PATTERNS = [
        "What is {person}'s most recent movie as {role}?",
        "What is the latest film {person} has {action}?",
        "Name {person}'s newest movie.",
    ]

    # Years for "since X" queries
    SINCE_YEARS = [2015, 2017, 2018, 2019, 2020, 2021]

    # Decades
    DECADES = ["2010s", "2020s"]

    QUERY_TYPES = [
        RecentWorksQueryType.COUNT_SINCE_YEAR,
        RecentWorksQueryType.COUNT_IN_DECADE,
        RecentWorksQueryType.MOST_RECENT,
    ]

    def __init__(self):
        super().__init__("tmdb_recent_works")
        self._person_var = RecentWorksPersonVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)

        person = self._person_var.sample(rng)

        if variant is not None:
            query_type = self.QUERY_TYPES[variant % len(self.QUERY_TYPES)]
        else:
            query_type = rng.choice(self.QUERY_TYPES)

        # Select year or decade based on query type
        if query_type == RecentWorksQueryType.COUNT_SINCE_YEAR:
            year = rng.choice(self.SINCE_YEARS)
            decade = None
        elif query_type == RecentWorksQueryType.COUNT_IN_DECADE:
            year = None
            decade = rng.choice(self.DECADES)
        else:
            year = None
            decade = None

        question_text = self._build_question(person, query_type, year, decade, rng)

        # Start at TMDB search (need to find person)
        start_url = "https://www.themoviedb.org/"

        validation_info = {
            "person_id": person.person_id,
            "person_name": person.name,
            "role": person.role,
            "query_type": query_type,
            "year": year,
            "decade": decade,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"person": person, "query_type": query_type, "year": year},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=12,
        )

    def _build_question(
        self,
        person: RecentWorksPersonSpec,
        query_type: str,
        year: Optional[int],
        decade: Optional[str],
        rng: random.Random,
    ) -> str:
        is_director = person.role == "directing"
        action = "directed" if is_director else "appeared in"
        action_past = "direct" if is_director else "appear in"
        role = "director" if is_director else "actor"

        if query_type == RecentWorksQueryType.COUNT_SINCE_YEAR:
            pattern = rng.choice(self.COUNT_SINCE_PATTERNS)
            return pattern.format(person=person.name, action=action, year=year)

        elif query_type == RecentWorksQueryType.COUNT_IN_DECADE:
            pattern = rng.choice(self.COUNT_DECADE_PATTERNS)
            return pattern.format(
                person=person.name,
                action=action,
                action_past=action_past,
                decade=decade
            )

        else:  # MOST_RECENT
            pattern = rng.choice(self.MOST_RECENT_PATTERNS)
            return pattern.format(person=person.name, action=action, role=role)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        query_type = validation_info.get("query_type")
        person = validation_info.get("person_name", "the person")
        year = validation_info.get("year")
        decade = validation_info.get("decade")

        if query_type == RecentWorksQueryType.COUNT_SINCE_YEAR:
            return f"""Task-Specific Rules (TMDB - Movies Since {year}):
- Count movies {person} has worked on since {year} (inclusive)
- Score 1.0: Exact count or within 1 movie
- Score 0.5: Within 3 movies
- Score 0.0: Off by more than 3
- Only count released movies, not upcoming"""

        if query_type == RecentWorksQueryType.COUNT_IN_DECADE:
            return f"""Task-Specific Rules (TMDB - Movies in {decade}):
- Count movies {person} has worked on in the {decade}
- Score 1.0: Exact count or within 1 movie
- Score 0.5: Within 3 movies
- Score 0.0: Off by more than 3
- {decade} means 2010-2019 or 2020-2029"""

        return f"""Task-Specific Rules (TMDB - Most Recent Movie):
- Find {person}'s most recently released movie
- Score 1.0: Correct movie title
- Score 0.5: A recent movie but not the absolute latest
- Score 0.0: Wrong or old movie
- Only count released movies, not upcoming"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        person_id = validation_info.get("person_id", "")
        query_type = validation_info.get("query_type")
        role = validation_info.get("role", "acting")
        year = validation_info.get("year")
        decade = validation_info.get("decade")

        if not person_id:
            return GroundTruthResult.fail("No person_id provided")

        try:
            data = await TMDBClient.get(f"/person/{person_id}/movie_credits")
            if not data:
                return GroundTruthResult.retry("No data returned from TMDB API")

            if role == "directing":
                crew = data.get("crew", [])
                movies = [
                    m for m in crew
                    if m.get("job") == "Director" and m.get("release_date")
                ]
            else:
                movies = [
                    m for m in data.get("cast", [])
                    if m.get("release_date")
                ]

            if not movies:
                return GroundTruthResult.fail("No movies found")

            # Filter by date
            today = datetime.now().strftime("%Y-%m-%d")

            if query_type == RecentWorksQueryType.COUNT_SINCE_YEAR:
                filtered = [
                    m for m in movies
                    if m.get("release_date", "")[:4] >= str(year)
                    and m.get("release_date", "") <= today
                ]
                # Deduplicate by movie ID
                unique_ids = set(m.get("id") for m in filtered)
                return GroundTruthResult.ok(str(len(unique_ids)))

            elif query_type == RecentWorksQueryType.COUNT_IN_DECADE:
                if decade == "2010s":
                    start, end = "2010", "2019"
                else:  # 2020s
                    start, end = "2020", "2029"

                filtered = [
                    m for m in movies
                    if start <= m.get("release_date", "")[:4] <= end
                    and m.get("release_date", "") <= today
                ]
                unique_ids = set(m.get("id") for m in filtered)
                return GroundTruthResult.ok(str(len(unique_ids)))

            else:  # MOST_RECENT
                released = [
                    m for m in movies
                    if m.get("release_date", "") <= today
                ]
                if not released:
                    return GroundTruthResult.fail("No released movies found")

                released_sorted = sorted(
                    released,
                    key=lambda m: m.get("release_date", ""),
                    reverse=True
                )
                latest = released_sorted[0]
                return GroundTruthResult.ok(
                    f"{latest.get('title')} ({latest.get('release_date', '')[:4]})"
                )

        except Exception as e:
            return GroundTruthResult.retry(f"TMDB API error: {e}")

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        result = await self.get_ground_truth(validation_info)

        if not result.success:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details=f"Ground truth unavailable: {result.error}",
            )

        query_type = validation_info.get("query_type")

        if query_type in [RecentWorksQueryType.COUNT_SINCE_YEAR, RecentWorksQueryType.COUNT_IN_DECADE]:
            return self._validate_count(answer, result.value)
        else:
            return self._validate_movie_title(answer, result.value)

    def _validate_count(self, answer: str, expected: str) -> ValidationResult:
        import re

        try:
            exp_count = int(expected)
        except ValueError:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected count",
            )

        num_match = re.search(r"\b(\d+)\b", answer)
        if not num_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not find number in answer",
            )

        ans_count = int(num_match.group(1))
        diff = abs(ans_count - exp_count)

        if diff <= 1:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details=f"Count within 1 (diff: {diff})",
            )
        elif diff <= 3:
            return ValidationResult(
                score=0.5, is_correct=False, expected=expected,
                actual=answer, details=f"Count within 3 (diff: {diff})",
            )
        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details=f"Count off by {diff}",
        )

    def _validate_movie_title(self, answer: str, expected: str) -> ValidationResult:
        import re

        answer_lower = answer.lower().strip()

        match = re.match(r"(.+?)\s*\(\d{4}\)", expected)
        exp_title = match.group(1).lower() if match else expected.lower()

        if exp_title in answer_lower:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Movie title matches",
            )

        # Check key words
        title_words = [w for w in exp_title.split() if len(w) > 3]
        if title_words:
            matches = sum(1 for w in title_words if w in answer_lower)
            if matches >= len(title_words) * 0.6:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details="Most title words match",
                )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Movie title not found",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        person_id = validation_info.get("person_id", "")
        trigger = UrlPatternTrigger(
            domains=["themoviedb.org"],
            url_contains=f"/person/{person_id}" if person_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
