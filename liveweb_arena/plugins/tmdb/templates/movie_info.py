"""Movie info template for TMDB - EASY DIFFICULTY"""

import random
from typing import Any, Dict, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .variables import MovieVariable, MetricVariable, MovieSpec, MetricSpec, MovieMetric
from ..api_client import TMDBClient


# Language code to name mapping
LANGUAGE_NAMES = {
    "en": "English",
    "ja": "Japanese",
    "ko": "Korean",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "zh": "Chinese",
    "pt": "Portuguese",
    "ru": "Russian",
    "hi": "Hindi",
}


@register_template("tmdb_movie_info")
class TMDBMovieInfoTemplate(QuestionTemplate):
    """
    Template for movie information queries - EASY DIFFICULTY.

    Single-hop queries about a movie's metadata.

    Examples:
    - What is the release date of Oppenheimer?
    - How long is Inception in minutes?
    - Who directed The Godfather?
    - What is the original language of Parasite?
    """

    RELEASE_DATE_PATTERNS = [
        "What is the release date of {movie}?",
        "When was {movie} released?",
        "What date did {movie} come out?",
    ]

    RUNTIME_PATTERNS = [
        "How long is {movie} in minutes?",
        "What is the runtime of {movie}?",
        "How many minutes is {movie}?",
    ]

    LANGUAGE_PATTERNS = [
        "What is the original language of {movie}?",
        "In what language was {movie} originally made?",
        "What language is {movie} in?",
    ]

    DIRECTOR_PATTERNS = [
        "Who directed {movie}?",
        "Who is the director of {movie}?",
        "Who was the director of {movie}?",
    ]

    def __init__(self):
        super().__init__("tmdb_movie_info")
        self._movie_var = MovieVariable()
        self._metric_var = MetricVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a movie info question."""
        rng = random.Random(seed)

        movie = self._movie_var.sample(rng)

        if variant is not None:
            metric = self._metric_var.sample_by_index(variant)
        else:
            metric = self._metric_var.sample(rng)

        question_text = self._build_question(movie, metric, rng)
        start_url = f"https://www.themoviedb.org/movie/{movie.movie_id}"

        validation_info = {
            "movie_id": movie.movie_id,
            "movie_title": movie.title,
            "metric_type": metric.metric.value,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"movie": movie, "metric": metric},
            validation_info=validation_info,
            template_name=self.name,
        )

    def _build_question(
        self,
        movie: MovieSpec,
        metric: MetricSpec,
        rng: random.Random,
    ) -> str:
        """Build question text based on metric type."""
        if metric.metric == MovieMetric.RELEASE_DATE:
            patterns = self.RELEASE_DATE_PATTERNS
        elif metric.metric == MovieMetric.RUNTIME:
            patterns = self.RUNTIME_PATTERNS
        elif metric.metric == MovieMetric.ORIGINAL_LANGUAGE:
            patterns = self.LANGUAGE_PATTERNS
        else:  # DIRECTOR
            patterns = self.DIRECTOR_PATTERNS

        pattern = rng.choice(patterns)
        return pattern.format(movie=movie.title)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        """Get validation rules based on metric type."""
        metric_type = validation_info.get("metric_type", "release_date")

        if metric_type == "release_date":
            return """Task-Specific Rules (TMDB - Release Date):
- Score 1.0: Date matches exactly (any format accepted: YYYY-MM-DD, Month DD YYYY, etc.)
- Score 0.5: Year and month match but day differs by 1-2 days
- Score 0.0: Date is significantly different
- Accept formats: 2023-07-21, July 21, 2023, 21/07/2023"""

        if metric_type == "runtime":
            return """Task-Specific Rules (TMDB - Runtime):
- Score 1.0: Runtime matches within 2 minutes
- Score 0.5: Runtime matches within 5 minutes
- Score 0.0: Runtime differs by more than 5 minutes
- Accept formats: 180 minutes, 180 min, 180m, 3h 0m, 3 hours"""

        if metric_type == "original_language":
            return """Task-Specific Rules (TMDB - Original Language):
- Score 1.0: Language matches (accept code or full name: en/English, ja/Japanese)
- Score 0.0: Language doesn't match
- Common codes: en=English, ja=Japanese, ko=Korean, es=Spanish"""

        if metric_type == "director":
            return """Task-Specific Rules (TMDB - Director):
- Score 1.0: Director name matches (case insensitive, partial name ok if unique)
- Score 0.0: Wrong director or unable to identify
- For multiple directors, any correct name scores 1.0"""

        return ""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch movie data from TMDB API."""
        movie_id = validation_info.get("movie_id", "")
        metric_type = validation_info.get("metric_type", "release_date")

        if not movie_id:
            return None

        try:
            if metric_type == "director":
                data = await TMDBClient.get_movie_with_credits(movie_id)
            else:
                data = await TMDBClient.get_movie(movie_id)

            if not data:
                return None

            if metric_type == "release_date":
                release_date = data.get("release_date")
                if release_date:
                    return release_date  # YYYY-MM-DD format

            elif metric_type == "runtime":
                runtime = data.get("runtime")
                if runtime is not None:
                    return f"{runtime} minutes"

            elif metric_type == "original_language":
                lang_code = data.get("original_language")
                if lang_code:
                    lang_name = LANGUAGE_NAMES.get(lang_code, lang_code.upper())
                    return f"{lang_name} ({lang_code})"

            elif metric_type == "director":
                credits = data.get("credits", {})
                crew = credits.get("crew", [])
                directors = [p["name"] for p in crew if p.get("job") == "Director"]
                if directors:
                    return ", ".join(directors)

            return None

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate movie info answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        metric_type = validation_info.get("metric_type", "release_date")

        if metric_type == "release_date":
            return self._validate_date(answer, ground_truth)
        elif metric_type == "runtime":
            return self._validate_runtime(answer, ground_truth)
        elif metric_type == "original_language":
            return self._validate_language(answer, ground_truth)
        else:  # director
            return self._validate_director(answer, ground_truth)

    def _validate_date(self, answer: str, expected: str) -> ValidationResult:
        """Validate release date answer."""
        import re
        from datetime import datetime

        # Parse expected date (YYYY-MM-DD format from API)
        try:
            exp_date = datetime.strptime(expected, "%Y-%m-%d")
        except ValueError:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected date",
            )

        # Try to parse answer in various formats
        answer_clean = answer.strip().lower()
        act_date = None

        # Common date patterns
        patterns = [
            (r"(\d{4})-(\d{1,2})-(\d{1,2})", "%Y-%m-%d"),
            (r"(\d{1,2})/(\d{1,2})/(\d{4})", "%m/%d/%Y"),
            (r"(\d{1,2})-(\d{1,2})-(\d{4})", "%d-%m-%Y"),
        ]

        for pattern, fmt in patterns:
            match = re.search(pattern, answer)
            if match:
                try:
                    act_date = datetime.strptime(match.group(), fmt)
                    break
                except ValueError:
                    continue

        # Try month name formats
        if act_date is None:
            month_patterns = [
                r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2}),?\s+(\d{4})",
                r"(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})",
            ]
            months = {
                "january": 1, "february": 2, "march": 3, "april": 4,
                "may": 5, "june": 6, "july": 7, "august": 8,
                "september": 9, "october": 10, "november": 11, "december": 12,
            }

            for i, pattern in enumerate(month_patterns):
                match = re.search(pattern, answer_clean)
                if match:
                    try:
                        if i == 0:  # Month DD, YYYY
                            month = months[match.group(1)]
                            day = int(match.group(2))
                            year = int(match.group(3))
                        else:  # DD Month YYYY
                            day = int(match.group(1))
                            month = months[match.group(2)]
                            year = int(match.group(3))
                        act_date = datetime(year, month, day)
                        break
                    except (ValueError, KeyError):
                        continue

        if act_date is None:
            # Check if at least year is mentioned
            year_match = re.search(r"\b(19|20)\d{2}\b", answer)
            if year_match and int(year_match.group()) == exp_date.year:
                return ValidationResult(
                    score=0.5, is_correct=False, expected=expected,
                    actual=answer, details="Year correct but full date not parsed",
                )
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse date from answer",
            )

        # Compare dates
        diff_days = abs((act_date - exp_date).days)

        if diff_days == 0:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Exact date match",
            )
        elif diff_days <= 2:
            return ValidationResult(
                score=0.5, is_correct=False, expected=expected,
                actual=answer, details=f"Date off by {diff_days} day(s)",
            )
        else:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details=f"Date off by {diff_days} days",
            )

    def _validate_runtime(self, answer: str, expected: str) -> ValidationResult:
        """Validate runtime answer."""
        import re

        # Parse expected (format: "XXX minutes")
        exp_match = re.search(r"(\d+)", expected)
        if not exp_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected runtime",
            )
        exp_minutes = int(exp_match.group(1))

        # Parse answer
        answer_lower = answer.lower()
        act_minutes = None

        # Try hours and minutes format (e.g., "2h 30m", "2 hours 30 minutes")
        hm_match = re.search(r"(\d+)\s*h(?:ours?)?\s*(?:and\s*)?(\d+)?\s*m?(?:in)?", answer_lower)
        if hm_match:
            hours = int(hm_match.group(1))
            minutes = int(hm_match.group(2)) if hm_match.group(2) else 0
            act_minutes = hours * 60 + minutes
        else:
            # Try just minutes
            min_match = re.search(r"(\d+)\s*(?:min|minutes?|m\b)", answer_lower)
            if min_match:
                act_minutes = int(min_match.group(1))
            else:
                # Try just a number
                num_match = re.search(r"\b(\d{2,3})\b", answer)
                if num_match:
                    act_minutes = int(num_match.group(1))

        if act_minutes is None:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse runtime from answer",
            )

        diff = abs(act_minutes - exp_minutes)

        if diff <= 2:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details=f"Within 2 minute tolerance (diff: {diff})",
            )
        elif diff <= 5:
            return ValidationResult(
                score=0.5, is_correct=False, expected=expected,
                actual=answer, details=f"Within 5 minute tolerance (diff: {diff})",
            )
        else:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details=f"Outside tolerance (diff: {diff} minutes)",
            )

    def _validate_language(self, answer: str, expected: str) -> ValidationResult:
        """Validate language answer."""
        answer_lower = answer.lower().strip()
        expected_lower = expected.lower()

        # Expected format: "Language Name (code)" e.g., "English (en)"
        # Extract both name and code
        import re
        match = re.match(r"(.+?)\s*\((\w+)\)", expected)
        if match:
            exp_name = match.group(1).lower()
            exp_code = match.group(2).lower()
        else:
            exp_name = expected_lower
            exp_code = expected_lower

        # Check if answer contains the language name or code
        if exp_name in answer_lower or exp_code in answer_lower:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Language matches",
            )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Language doesn't match",
        )

    def _validate_director(self, answer: str, expected: str) -> ValidationResult:
        """Validate director answer."""
        answer_lower = answer.lower().strip()

        # Expected format: "Name1, Name2" for multiple directors
        directors = [d.strip().lower() for d in expected.split(",")]

        for director in directors:
            if director in answer_lower:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details="Director name matches",
                )

            # Check last name match (common for famous directors)
            parts = director.split()
            if len(parts) > 1:
                last_name = parts[-1]
                if last_name in answer_lower and len(last_name) > 3:
                    return ValidationResult(
                        score=1.0, is_correct=True, expected=expected,
                        actual=answer, details="Director last name matches",
                    )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Director name not found in answer",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when agent visits the movie's TMDB page."""
        movie_id = validation_info.get("movie_id", "")
        trigger = UrlPatternTrigger(
            domains=["themoviedb.org"],
            url_contains=f"/movie/{movie_id}" if movie_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
