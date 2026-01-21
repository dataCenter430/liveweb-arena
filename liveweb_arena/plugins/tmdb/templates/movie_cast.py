"""Movie cast template for TMDB - MEDIUM DIFFICULTY"""

import random
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .variables import MovieVariable, CastPositionVariable, MovieSpec
from ..api_client import TMDBClient


@register_template("tmdb_movie_cast")
class TMDBMovieCastTemplate(QuestionTemplate):
    """
    Template for movie cast queries - MEDIUM DIFFICULTY.

    Requires navigating to cast/credits section and identifying actors.

    Examples:
    - Who plays the lead role in Inception?
    - Name the top 3 billed actors in The Godfather.
    - Who are the main cast members of Avengers: Endgame?
    """

    LEAD_PATTERNS = [
        "Who plays the lead role in {movie}?",
        "Who is the main actor in {movie}?",
        "Who stars in {movie}?",
    ]

    TOP_3_PATTERNS = [
        "Name the top 3 billed actors in {movie}.",
        "Who are the top 3 cast members of {movie}?",
        "List the first 3 actors credited in {movie}.",
    ]

    TOP_5_PATTERNS = [
        "Name the top 5 billed actors in {movie}.",
        "Who are the top 5 cast members of {movie}?",
        "List the first 5 actors credited in {movie}.",
    ]

    def __init__(self):
        super().__init__("tmdb_movie_cast")
        self._movie_var = MovieVariable()
        self._position_var = CastPositionVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a movie cast question."""
        rng = random.Random(seed)

        movie = self._movie_var.sample(rng)

        if variant is not None:
            position = self._position_var.sample_by_index(variant)
        else:
            position = self._position_var.sample(rng)

        question_text = self._build_question(movie, position, rng)
        start_url = f"https://www.themoviedb.org/movie/{movie.movie_id}"

        validation_info = {
            "movie_id": movie.movie_id,
            "movie_title": movie.title,
            "cast_position": position,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"movie": movie, "position": position},
            validation_info=validation_info,
            template_name=self.name,
        )

    def _build_question(
        self,
        movie: MovieSpec,
        position: str,
        rng: random.Random,
    ) -> str:
        """Build question text based on cast position."""
        if position == "lead":
            patterns = self.LEAD_PATTERNS
        elif position == "top_3":
            patterns = self.TOP_3_PATTERNS
        else:  # top_5
            patterns = self.TOP_5_PATTERNS

        pattern = rng.choice(patterns)
        return pattern.format(movie=movie.title)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        """Get validation rules based on cast position."""
        position = validation_info.get("cast_position", "lead")

        if position == "lead":
            return """Task-Specific Rules (TMDB - Lead Actor):
- Score 1.0: The first billed actor's name is mentioned (case insensitive)
- Score 0.5: A top-3 billed actor is mentioned but not the lead
- Score 0.0: No top-3 cast member mentioned
- Accept partial names if uniquely identifying"""

        if position == "top_3":
            return """Task-Specific Rules (TMDB - Top 3 Cast):
- Score 1.0: All 3 actors mentioned correctly
- Score 0.67: 2 of 3 actors mentioned correctly
- Score 0.33: 1 of 3 actors mentioned correctly
- Score 0.0: None of the top 3 mentioned
- Order doesn't matter, names must match"""

        return """Task-Specific Rules (TMDB - Top 5 Cast):
- Score 1.0: All 5 actors mentioned correctly
- Score 0.8/0.6/0.4/0.2: 4/3/2/1 actors mentioned
- Score 0.0: None of the top 5 mentioned
- Order doesn't matter, names must match"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch movie credits from TMDB API."""
        movie_id = validation_info.get("movie_id", "")
        position = validation_info.get("cast_position", "lead")

        if not movie_id:
            return None

        try:
            data = await TMDBClient.get_movie_credits(movie_id)
            if not data:
                return None

            cast = data.get("cast", [])
            if not cast:
                return None

            if position == "lead":
                return cast[0]["name"] if cast else None
            elif position == "top_3":
                names = [c["name"] for c in cast[:3]]
                return ", ".join(names)
            else:  # top_5
                names = [c["name"] for c in cast[:5]]
                return ", ".join(names)

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate movie cast answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        position = validation_info.get("cast_position", "lead")

        if position == "lead":
            return self._validate_lead(answer, ground_truth)
        elif position == "top_3":
            return self._validate_top_n(answer, ground_truth, 3)
        else:
            return self._validate_top_n(answer, ground_truth, 5)

    def _validate_lead(self, answer: str, expected: str) -> ValidationResult:
        """Validate lead actor answer."""
        answer_lower = answer.lower().strip()
        expected_lower = expected.lower()

        # Check full name match
        if expected_lower in answer_lower:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Lead actor name matches",
            )

        # Check last name match (for famous actors)
        parts = expected_lower.split()
        if len(parts) > 1:
            last_name = parts[-1]
            if last_name in answer_lower and len(last_name) > 3:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details="Lead actor last name matches",
                )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Lead actor not found in answer",
        )

    def _validate_top_n(self, answer: str, expected: str, n: int) -> ValidationResult:
        """Validate top N cast members answer."""
        answer_lower = answer.lower().strip()
        expected_names = [name.strip().lower() for name in expected.split(",")]

        matched = 0
        matched_names = []

        for name in expected_names:
            if name in answer_lower:
                matched += 1
                matched_names.append(name)
                continue

            # Check last name
            parts = name.split()
            if len(parts) > 1:
                last_name = parts[-1]
                if last_name in answer_lower and len(last_name) > 3:
                    matched += 1
                    matched_names.append(name)

        score = matched / n
        is_correct = score >= 0.99

        if matched == n:
            details = f"All {n} actors matched"
        elif matched > 0:
            details = f"{matched}/{n} actors matched: {', '.join(matched_names)}"
        else:
            details = f"No actors from top {n} found in answer"

        return ValidationResult(
            score=round(score, 2),
            is_correct=is_correct,
            expected=expected,
            actual=answer,
            details=details,
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when agent visits the movie's TMDB page or cast page."""
        movie_id = validation_info.get("movie_id", "")
        trigger = UrlPatternTrigger(
            domains=["themoviedb.org"],
            url_contains=f"/movie/{movie_id}" if movie_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
