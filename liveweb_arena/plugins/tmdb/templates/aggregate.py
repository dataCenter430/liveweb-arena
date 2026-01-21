"""Aggregate query template for TMDB - HARD DIFFICULTY (Anti-memorization)"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig, GroundTruthResult
)
from ..api_client import TMDBClient


@dataclass
class MovieSeriesSpec:
    """A series of movies for aggregation queries"""
    name: str
    movie_ids: List[str]  # TMDB movie IDs
    display_name: str     # Human readable name


class MovieSeriesVariable:
    """Variable for movie series/trilogies"""

    SERIES: List[MovieSeriesSpec] = [
        MovieSeriesSpec(
            "lotr",
            ["120", "121", "122"],
            "The Lord of the Rings trilogy"
        ),
        MovieSeriesSpec(
            "dark_knight",
            ["272", "155", "49026"],
            "The Dark Knight trilogy"
        ),
        MovieSeriesSpec(
            "godfather",
            ["238", "240", "242"],
            "The Godfather trilogy"
        ),
        MovieSeriesSpec(
            "back_to_future",
            ["105", "165", "196"],
            "The Back to the Future trilogy"
        ),
        MovieSeriesSpec(
            "matrix_original",
            ["603", "604", "605"],
            "The original Matrix trilogy"
        ),
        MovieSeriesSpec(
            "toy_story_first3",
            ["862", "863", "10193"],
            "The first three Toy Story movies"
        ),
        MovieSeriesSpec(
            "indiana_jones_original",
            ["85", "87", "89"],
            "The original Indiana Jones trilogy"
        ),
        MovieSeriesSpec(
            "bourne_original",
            ["2501", "2502", "2503"],
            "The original Bourne trilogy"
        ),
        MovieSeriesSpec(
            "nolan_batman",
            ["272", "155", "49026"],
            "Christopher Nolan's Batman films"
        ),
        MovieSeriesSpec(
            "iron_man",
            ["1726", "10138", "68721"],
            "The Iron Man trilogy"
        ),
        MovieSeriesSpec(
            "captain_america",
            ["1771", "100402", "271110"],
            "The Captain America trilogy"
        ),
        MovieSeriesSpec(
            "thor_first3",
            ["10195", "76338", "284053"],
            "The first three Thor movies"
        ),
        MovieSeriesSpec(
            "hunger_games_first3",
            ["70160", "101299", "131631"],
            "The first three Hunger Games movies"
        ),
        MovieSeriesSpec(
            "john_wick_first3",
            ["245891", "324552", "458156"],
            "The first three John Wick movies"
        ),
        MovieSeriesSpec(
            "spiderman_raimi",
            ["557", "558", "559"],
            "Sam Raimi's Spider-Man trilogy"
        ),
    ]

    def sample(self, rng: random.Random) -> MovieSeriesSpec:
        return rng.choice(self.SERIES)


class AggregateQueryType:
    """Types of aggregate queries"""
    TOTAL_RUNTIME = "total_runtime"      # Sum of all runtimes
    AVERAGE_RUNTIME = "average_runtime"  # Average runtime
    LONGEST_MOVIE = "longest_movie"      # Which movie is longest
    SHORTEST_MOVIE = "shortest_movie"    # Which movie is shortest
    EARLIEST_RELEASE = "earliest_release"  # First released


@register_template("tmdb_aggregate")
class TMDBAggregateTemplate(QuestionTemplate):
    """
    Template for aggregate/computed queries - HARD DIFFICULTY.

    These questions CANNOT be answered by memorization because they require:
    1. Visiting multiple movie pages
    2. Performing calculations (sum, average, comparison)

    Examples:
    - What is the total runtime of The Lord of the Rings trilogy?
    - What is the average runtime of The Dark Knight trilogy?
    - Which movie in the Godfather trilogy has the longest runtime?
    """

    TOTAL_RUNTIME_PATTERNS = [
        "What is the total runtime of {series} in minutes?",
        "How many minutes long are {series} combined?",
        "What is the combined runtime of {series}?",
    ]

    AVERAGE_RUNTIME_PATTERNS = [
        "What is the average runtime of {series} in minutes?",
        "On average, how long are the movies in {series}?",
    ]

    LONGEST_PATTERNS = [
        "Which movie in {series} has the longest runtime?",
        "What is the longest movie in {series}?",
        "Which of {series} is the longest?",
    ]

    SHORTEST_PATTERNS = [
        "Which movie in {series} has the shortest runtime?",
        "What is the shortest movie in {series}?",
        "Which of {series} is the shortest?",
    ]

    EARLIEST_PATTERNS = [
        "Which movie in {series} was released first?",
        "What is the first movie in {series} by release date?",
    ]

    QUERY_TYPES = [
        AggregateQueryType.TOTAL_RUNTIME,
        AggregateQueryType.AVERAGE_RUNTIME,
        AggregateQueryType.LONGEST_MOVIE,
        AggregateQueryType.SHORTEST_MOVIE,
    ]

    def __init__(self):
        super().__init__("tmdb_aggregate")
        self._series_var = MovieSeriesVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)

        series = self._series_var.sample(rng)

        if variant is not None:
            query_type = self.QUERY_TYPES[variant % len(self.QUERY_TYPES)]
        else:
            query_type = rng.choice(self.QUERY_TYPES)

        question_text = self._build_question(series, query_type, rng)

        # Start at first movie in series
        start_url = f"https://www.themoviedb.org/movie/{series.movie_ids[0]}"

        validation_info = {
            "series_name": series.name,
            "series_display": series.display_name,
            "movie_ids": series.movie_ids,
            "query_type": query_type,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"series": series, "query_type": query_type},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=15,  # Need to visit multiple pages
        )

    def _build_question(
        self,
        series: MovieSeriesSpec,
        query_type: str,
        rng: random.Random,
    ) -> str:
        if query_type == AggregateQueryType.TOTAL_RUNTIME:
            patterns = self.TOTAL_RUNTIME_PATTERNS
        elif query_type == AggregateQueryType.AVERAGE_RUNTIME:
            patterns = self.AVERAGE_RUNTIME_PATTERNS
        elif query_type == AggregateQueryType.LONGEST_MOVIE:
            patterns = self.LONGEST_PATTERNS
        else:
            patterns = self.SHORTEST_PATTERNS

        pattern = rng.choice(patterns)
        return pattern.format(series=series.display_name)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        query_type = validation_info.get("query_type", AggregateQueryType.TOTAL_RUNTIME)
        series = validation_info.get("series_display", "the series")

        if query_type == AggregateQueryType.TOTAL_RUNTIME:
            return f"""Task-Specific Rules (TMDB - Total Runtime):
- Calculate the sum of runtimes for all movies in {series}
- Score 1.0: Total within 5 minutes of expected
- Score 0.5: Total within 15 minutes of expected
- Score 0.0: Total off by more than 15 minutes
- Accept formats: 558 minutes, 558 min, 9h 18m"""

        if query_type == AggregateQueryType.AVERAGE_RUNTIME:
            return f"""Task-Specific Rules (TMDB - Average Runtime):
- Calculate the average runtime of movies in {series}
- Score 1.0: Average within 3 minutes of expected
- Score 0.5: Average within 10 minutes of expected
- Score 0.0: Average off by more than 10 minutes
- Round to nearest minute"""

        if query_type == AggregateQueryType.LONGEST_MOVIE:
            return f"""Task-Specific Rules (TMDB - Longest Movie):
- Find which movie in {series} has the longest runtime
- Score 1.0: Correct movie title identified
- Score 0.0: Wrong movie"""

        return f"""Task-Specific Rules (TMDB - Shortest Movie):
- Find which movie in {series} has the shortest runtime
- Score 1.0: Correct movie title identified
- Score 0.0: Wrong movie"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        movie_ids = validation_info.get("movie_ids", [])
        query_type = validation_info.get("query_type", AggregateQueryType.TOTAL_RUNTIME)

        if not movie_ids:
            return GroundTruthResult.fail("No movie IDs provided")

        try:
            # Fetch all movies
            movies_data = []
            for movie_id in movie_ids:
                data = await TMDBClient.get_movie(movie_id)
                if data and data.get("runtime"):
                    movies_data.append({
                        "id": movie_id,
                        "title": data.get("title"),
                        "runtime": data.get("runtime"),
                        "release_date": data.get("release_date", ""),
                    })

            if len(movies_data) != len(movie_ids):
                return GroundTruthResult.retry("Could not fetch all movie data")

            runtimes = [m["runtime"] for m in movies_data]

            if query_type == AggregateQueryType.TOTAL_RUNTIME:
                total = sum(runtimes)
                return GroundTruthResult.ok(f"{total} minutes")

            elif query_type == AggregateQueryType.AVERAGE_RUNTIME:
                avg = round(sum(runtimes) / len(runtimes))
                return GroundTruthResult.ok(f"{avg} minutes")

            elif query_type == AggregateQueryType.LONGEST_MOVIE:
                longest = max(movies_data, key=lambda m: m["runtime"])
                return GroundTruthResult.ok(f"{longest['title']} ({longest['runtime']} min)")

            elif query_type == AggregateQueryType.SHORTEST_MOVIE:
                shortest = min(movies_data, key=lambda m: m["runtime"])
                return GroundTruthResult.ok(f"{shortest['title']} ({shortest['runtime']} min)")

            return GroundTruthResult.fail(f"Unknown query type: {query_type}")

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

        ground_truth = result.value
        query_type = validation_info.get("query_type", AggregateQueryType.TOTAL_RUNTIME)

        if query_type in [AggregateQueryType.TOTAL_RUNTIME, AggregateQueryType.AVERAGE_RUNTIME]:
            return self._validate_runtime_calc(answer, ground_truth, query_type)
        else:
            return self._validate_movie_title(answer, ground_truth)

    def _validate_runtime_calc(
        self,
        answer: str,
        expected: str,
        query_type: str
    ) -> ValidationResult:
        import re

        # Parse expected (format: "XXX minutes")
        exp_match = re.search(r"(\d+)", expected)
        if not exp_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected value",
            )
        exp_val = int(exp_match.group(1))

        # Parse answer - handle various formats
        answer_lower = answer.lower()
        ans_val = None

        # Try hours + minutes format
        hm_match = re.search(r"(\d+)\s*h(?:ours?)?\s*(?:and\s*)?(\d+)?\s*m", answer_lower)
        if hm_match:
            hours = int(hm_match.group(1))
            mins = int(hm_match.group(2)) if hm_match.group(2) else 0
            ans_val = hours * 60 + mins
        else:
            # Try just minutes
            min_match = re.search(r"(\d+)\s*(?:min|minutes?)?", answer)
            if min_match:
                ans_val = int(min_match.group(1))

        if ans_val is None:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse number from answer",
            )

        diff = abs(ans_val - exp_val)

        # Different tolerances for total vs average
        if query_type == AggregateQueryType.TOTAL_RUNTIME:
            if diff <= 5:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details=f"Within 5 min tolerance (diff: {diff})",
                )
            elif diff <= 15:
                return ValidationResult(
                    score=0.5, is_correct=False, expected=expected,
                    actual=answer, details=f"Within 15 min tolerance (diff: {diff})",
                )
        else:  # AVERAGE
            if diff <= 3:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details=f"Within 3 min tolerance (diff: {diff})",
                )
            elif diff <= 10:
                return ValidationResult(
                    score=0.5, is_correct=False, expected=expected,
                    actual=answer, details=f"Within 10 min tolerance (diff: {diff})",
                )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details=f"Outside tolerance (diff: {diff})",
        )

    def _validate_movie_title(self, answer: str, expected: str) -> ValidationResult:
        import re

        answer_lower = answer.lower().strip()

        # Expected format: "Movie Title (XXX min)"
        match = re.match(r"(.+?)\s*\(\d+\s*min\)", expected)
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
            actual=answer, details="Movie title not found in answer",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        # Trigger on any movie in the series
        movie_ids = validation_info.get("movie_ids", [])
        if movie_ids:
            # Trigger on last movie (agent likely visits all)
            trigger = UrlPatternTrigger(
                domains=["themoviedb.org"],
                url_contains=f"/movie/{movie_ids[-1]}",
            )
        else:
            trigger = UrlPatternTrigger(domains=["themoviedb.org"])
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
