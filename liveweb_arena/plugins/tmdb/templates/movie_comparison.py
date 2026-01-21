"""Movie comparison template for TMDB - HARD DIFFICULTY"""

import random
from typing import Any, Dict, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .variables import MovieVariable, MovieSpec
from ..api_client import TMDBClient


@register_template("tmdb_movie_comparison")
class TMDBMovieComparisonTemplate(QuestionTemplate):
    """
    Template for comparing two movies - HARD DIFFICULTY.

    Requires visiting two movie pages and comparing values.

    Examples:
    - Which movie is longer, Inception or Interstellar?
    - Which was released first, The Godfather or The Godfather Part II?
    - How many minutes longer is Avengers: Endgame than Avengers: Infinity War?
    """

    RUNTIME_WHICH_PATTERNS = [
        "Which movie is longer, {movie1} or {movie2}?",
        "Between {movie1} and {movie2}, which has a longer runtime?",
        "Which film runs longer, {movie1} or {movie2}?",
    ]

    RUNTIME_DIFF_PATTERNS = [
        "How many minutes longer is {movie1} than {movie2}?",
        "What is the runtime difference between {movie1} and {movie2} in minutes?",
        "By how many minutes does {movie1} exceed {movie2} in length?",
    ]

    RELEASE_WHICH_PATTERNS = [
        "Which was released first, {movie1} or {movie2}?",
        "Which movie came out earlier, {movie1} or {movie2}?",
        "Between {movie1} and {movie2}, which premiered first?",
    ]

    def __init__(self):
        super().__init__("tmdb_movie_comparison")
        self._movie_var = MovieVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a movie comparison question."""
        rng = random.Random(seed)

        movie1, movie2 = self._movie_var.sample_pair(rng)

        # Select comparison type
        comp_types = ["runtime_which", "runtime_diff", "release_which"]
        if variant is not None:
            comp_type = comp_types[variant % len(comp_types)]
        else:
            comp_type = rng.choice(comp_types)

        question_text = self._build_question(movie1, movie2, comp_type, rng)
        start_url = f"https://www.themoviedb.org/movie/{movie1.movie_id}"

        validation_info = {
            "movie1_id": movie1.movie_id,
            "movie1_title": movie1.title,
            "movie2_id": movie2.movie_id,
            "movie2_title": movie2.title,
            "comparison_type": comp_type,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"movie1": movie1, "movie2": movie2, "comp_type": comp_type},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=15,  # Harder task needs more steps
        )

    def _build_question(
        self,
        movie1: MovieSpec,
        movie2: MovieSpec,
        comp_type: str,
        rng: random.Random,
    ) -> str:
        """Build question text based on comparison type."""
        if comp_type == "runtime_which":
            patterns = self.RUNTIME_WHICH_PATTERNS
        elif comp_type == "runtime_diff":
            patterns = self.RUNTIME_DIFF_PATTERNS
        else:  # release_which
            patterns = self.RELEASE_WHICH_PATTERNS

        pattern = rng.choice(patterns)
        return pattern.format(movie1=movie1.title, movie2=movie2.title)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        """Get validation rules based on comparison type."""
        movie1 = validation_info.get("movie1_title", "Movie1")
        movie2 = validation_info.get("movie2_title", "Movie2")
        comp_type = validation_info.get("comparison_type", "runtime_which")

        if comp_type == "runtime_which":
            return f"""Task-Specific Rules (TMDB - Runtime Comparison):
- The answer must clearly state which movie ({movie1} or {movie2}) is longer
- Score 1.0: Correct movie identified
- Score 0.0: Wrong movie identified or unclear answer
- Accept formats: "{movie1}", "{movie1} is longer", "the first one" (context-dependent)"""

        if comp_type == "runtime_diff":
            return f"""Task-Specific Rules (TMDB - Runtime Difference):
- The answer must state the difference in minutes between {movie1} and {movie2}
- Score 1.0: Correct difference within 2 minutes
- Score 0.5: Correct difference within 5 minutes
- Score 0.0: Difference off by more than 5 minutes or wrong direction
- If {movie1} is shorter, the difference should be negative or stated as such"""

        return f"""Task-Specific Rules (TMDB - Release Date Comparison):
- The answer must clearly state which movie ({movie1} or {movie2}) was released first
- Score 1.0: Correct movie identified
- Score 0.0: Wrong movie identified or unclear answer
- Accept formats: "{movie1}", "{movie1} came first", "the first one" (context-dependent)"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch comparison data from TMDB API."""
        movie1_id = validation_info.get("movie1_id", "")
        movie2_id = validation_info.get("movie2_id", "")
        movie1_title = validation_info.get("movie1_title", "")
        movie2_title = validation_info.get("movie2_title", "")
        comp_type = validation_info.get("comparison_type", "runtime_which")

        if not movie1_id or not movie2_id:
            return None

        try:
            data1 = await TMDBClient.get_movie(movie1_id)
            data2 = await TMDBClient.get_movie(movie2_id)

            if not data1 or not data2:
                return None

            if comp_type in ["runtime_which", "runtime_diff"]:
                runtime1 = data1.get("runtime", 0)
                runtime2 = data2.get("runtime", 0)

                if runtime1 is None or runtime2 is None:
                    return None

                if comp_type == "runtime_which":
                    if runtime1 > runtime2:
                        return f"{movie1_title} ({runtime1} min vs {runtime2} min)"
                    elif runtime2 > runtime1:
                        return f"{movie2_title} ({runtime2} min vs {runtime1} min)"
                    else:
                        return f"Same length ({runtime1} min each)"
                else:  # runtime_diff
                    diff = runtime1 - runtime2
                    if diff > 0:
                        return f"{diff} minutes longer"
                    elif diff < 0:
                        return f"{abs(diff)} minutes shorter (or -{abs(diff)} minutes)"
                    else:
                        return "0 minutes (same length)"

            else:  # release_which
                date1 = data1.get("release_date", "")
                date2 = data2.get("release_date", "")

                if not date1 or not date2:
                    return None

                if date1 < date2:
                    return f"{movie1_title} ({date1} vs {date2})"
                elif date2 < date1:
                    return f"{movie2_title} ({date2} vs {date1})"
                else:
                    return f"Same date ({date1})"

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate movie comparison answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        comp_type = validation_info.get("comparison_type", "runtime_which")

        if comp_type == "runtime_which":
            return self._validate_which(answer, ground_truth, validation_info)
        elif comp_type == "runtime_diff":
            return self._validate_diff(answer, ground_truth, validation_info)
        else:  # release_which
            return self._validate_which(answer, ground_truth, validation_info)

    def _validate_which(
        self,
        answer: str,
        expected: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate which-movie comparison answer."""
        answer_lower = answer.lower().strip()
        movie1_title = validation_info.get("movie1_title", "").lower()
        movie2_title = validation_info.get("movie2_title", "").lower()

        # Handle "same" case
        if "same" in expected.lower():
            if "same" in answer_lower or "equal" in answer_lower:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details="Correctly identified as same",
                )
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Movies have same value but answer differs",
            )

        # Extract winner from ground truth (format: "Movie Title (details)")
        winner = expected.split(" (")[0].lower()

        # Check if answer mentions the correct movie
        if winner in answer_lower:
            # Check for negation or wrong context
            loser = movie2_title if winner == movie1_title else movie1_title
            if loser in answer_lower:
                # Both mentioned - check which appears first or in affirmative context
                winner_pos = answer_lower.find(winner)
                loser_pos = answer_lower.find(loser)
                if winner_pos < loser_pos:
                    return ValidationResult(
                        score=1.0, is_correct=True, expected=expected,
                        actual=answer, details="Correct movie identified first",
                    )
                # If loser appears first, likely wrong
                return ValidationResult(
                    score=0.0, is_correct=False, expected=expected,
                    actual=answer, details="Wrong movie may be indicated",
                )
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Correct movie identified",
            )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Correct movie not found in answer",
        )

    def _validate_diff(
        self,
        answer: str,
        expected: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate runtime difference answer."""
        import re

        # Parse expected difference
        # Format: "X minutes longer" or "X minutes shorter (or -X minutes)"
        is_shorter = "shorter" in expected.lower() or "-" in expected
        exp_match = re.search(r"(\d+)", expected)
        if not exp_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected difference",
            )
        exp_diff = int(exp_match.group(1))
        if is_shorter:
            exp_diff = -exp_diff

        # Parse answer
        answer_lower = answer.lower()
        ans_shorter = "shorter" in answer_lower or "less" in answer_lower
        ans_negative = "-" in answer and not ans_shorter

        num_match = re.search(r"(\d+)", answer)
        if not num_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse difference from answer",
            )

        ans_diff = int(num_match.group(1))
        if ans_shorter or ans_negative:
            ans_diff = -ans_diff

        # Compare
        diff = abs(ans_diff - exp_diff)

        if diff <= 2:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details=f"Difference within 2 minutes (off by {diff})",
            )
        elif diff <= 5:
            return ValidationResult(
                score=0.5, is_correct=False, expected=expected,
                actual=answer, details=f"Difference within 5 minutes (off by {diff})",
            )
        else:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details=f"Difference off by {diff} minutes",
            )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when agent visits the second movie's page."""
        movie2_id = validation_info.get("movie2_id", "")
        trigger = UrlPatternTrigger(
            domains=["themoviedb.org"],
            url_contains=f"/movie/{movie2_id}" if movie2_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
