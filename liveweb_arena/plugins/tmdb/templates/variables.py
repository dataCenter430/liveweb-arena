"""Variable definitions for TMDB templates"""

import random
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


class MovieMetric(Enum):
    """Types of movie metrics"""
    RELEASE_DATE = "release_date"
    RUNTIME = "runtime"
    ORIGINAL_LANGUAGE = "original_language"
    DIRECTOR = "director"


@dataclass
class MovieSpec:
    """Specification of a movie"""
    movie_id: str  # TMDB movie ID
    title: str     # Movie title
    slug: str      # URL slug (optional, usually lowercase with dashes)


@dataclass
class MetricSpec:
    """Specification of a movie metric"""
    metric: MovieMetric
    display_name: str
    api_field: str


class MovieVariable:
    """Variable for movie selection - 40+ diverse movies"""

    # Movies with stable TMDB IDs covering different eras, genres, languages
    MOVIES: List[MovieSpec] = [
        # 2020s hits
        MovieSpec("872585", "Oppenheimer", "oppenheimer"),
        MovieSpec("569094", "Spider-Man: Across the Spider-Verse", "spider-man-across-the-spider-verse"),
        MovieSpec("385687", "Fast X", "fast-x"),
        MovieSpec("447365", "Guardians of the Galaxy Vol. 3", "guardians-of-the-galaxy-vol-3"),
        MovieSpec("502356", "The Super Mario Bros. Movie", "the-super-mario-bros-movie"),
        MovieSpec("603692", "John Wick: Chapter 4", "john-wick-chapter-4"),
        MovieSpec("926393", "The Equalizer 3", "the-equalizer-3"),
        MovieSpec("667538", "Transformers: Rise of the Beasts", "transformers-rise-of-the-beasts"),
        MovieSpec("346698", "Barbie", "barbie"),
        MovieSpec("614930", "Teenage Mutant Ninja Turtles: Mutant Mayhem", "teenage-mutant-ninja-turtles-mutant-mayhem"),
        # 2010s blockbusters
        MovieSpec("299536", "Avengers: Infinity War", "avengers-infinity-war"),
        MovieSpec("299534", "Avengers: Endgame", "avengers-endgame"),
        MovieSpec("27205", "Inception", "inception"),
        MovieSpec("157336", "Interstellar", "interstellar"),
        MovieSpec("284053", "Thor: Ragnarok", "thor-ragnarok"),
        MovieSpec("284052", "Doctor Strange", "doctor-strange"),
        MovieSpec("118340", "Guardians of the Galaxy", "guardians-of-the-galaxy"),
        MovieSpec("281957", "The Revenant", "the-revenant"),
        MovieSpec("68718", "Django Unchained", "django-unchained"),
        MovieSpec("24428", "The Avengers", "the-avengers"),
        # Classic films
        MovieSpec("238", "The Godfather", "the-godfather"),
        MovieSpec("240", "The Godfather Part II", "the-godfather-part-ii"),
        MovieSpec("278", "The Shawshank Redemption", "the-shawshank-redemption"),
        MovieSpec("155", "The Dark Knight", "the-dark-knight"),
        MovieSpec("550", "Fight Club", "fight-club"),
        MovieSpec("680", "Pulp Fiction", "pulp-fiction"),
        MovieSpec("13", "Forrest Gump", "forrest-gump"),
        MovieSpec("578", "Jaws", "jaws"),
        MovieSpec("597", "Titanic", "titanic"),
        MovieSpec("429", "The Good, the Bad and the Ugly", "the-good-the-bad-and-the-ugly"),
        # Award winners & critically acclaimed
        MovieSpec("496243", "Parasite", "parasite"),
        MovieSpec("359724", "Ford v Ferrari", "ford-v-ferrari"),
        MovieSpec("466272", "Once Upon a Time in Hollywood", "once-upon-a-time-in-hollywood"),
        MovieSpec("497", "The Green Mile", "the-green-mile"),
        MovieSpec("389", "12 Angry Men", "12-angry-men"),
        MovieSpec("122", "The Lord of the Rings: The Return of the King", "the-lord-of-the-rings-the-return-of-the-king"),
        MovieSpec("120", "The Lord of the Rings: The Fellowship of the Ring", "the-lord-of-the-rings-the-fellowship-of-the-ring"),
        MovieSpec("121", "The Lord of the Rings: The Two Towers", "the-lord-of-the-rings-the-two-towers"),
        # Animation & Family
        MovieSpec("862", "Toy Story", "toy-story"),
        MovieSpec("105", "Back to the Future", "back-to-the-future"),
        MovieSpec("324857", "Spider-Man: Into the Spider-Verse", "spider-man-into-the-spider-verse"),
        MovieSpec("508947", "Turning Red", "turning-red"),
        # International films
        MovieSpec("372058", "Your Name.", "your-name"),
        MovieSpec("129", "Spirited Away", "spirited-away"),
        MovieSpec("311324", "The Handmaiden", "the-handmaiden"),
    ]

    def __init__(self, allowed_movies: List[str] = None):
        if allowed_movies:
            self.movies = [m for m in self.MOVIES if m.movie_id in allowed_movies]
        else:
            self.movies = self.MOVIES

    def sample(self, rng: random.Random) -> MovieSpec:
        """Sample a single movie."""
        return rng.choice(self.movies)

    def sample_pair(self, rng: random.Random) -> tuple:
        """Sample two different movies for comparison."""
        movies = rng.sample(self.movies, 2)
        return movies[0], movies[1]


class MetricVariable:
    """Variable for metric selection"""

    METRICS: Dict[MovieMetric, MetricSpec] = {
        MovieMetric.RELEASE_DATE: MetricSpec(
            MovieMetric.RELEASE_DATE, "release date", "release_date"
        ),
        MovieMetric.RUNTIME: MetricSpec(
            MovieMetric.RUNTIME, "runtime", "runtime"
        ),
        MovieMetric.ORIGINAL_LANGUAGE: MetricSpec(
            MovieMetric.ORIGINAL_LANGUAGE, "original language", "original_language"
        ),
        MovieMetric.DIRECTOR: MetricSpec(
            MovieMetric.DIRECTOR, "director", "director"
        ),
    }

    def __init__(self, allowed_metrics: List[MovieMetric] = None):
        self.allowed_metrics = allowed_metrics or list(MovieMetric)

    def sample(self, rng: random.Random) -> MetricSpec:
        """Sample a random metric."""
        metric = rng.choice(self.allowed_metrics)
        return self.METRICS[metric]

    def sample_by_index(self, index: int) -> MetricSpec:
        """Sample a specific metric by index."""
        metric = self.allowed_metrics[index % len(self.allowed_metrics)]
        return self.METRICS[metric]


class CastPositionVariable:
    """Variable for cast position selection"""

    POSITIONS = ["lead", "top_3", "top_5"]

    def __init__(self):
        self.positions = self.POSITIONS

    def sample(self, rng: random.Random) -> str:
        """Sample a cast position."""
        return rng.choice(self.positions)

    def sample_by_index(self, index: int) -> str:
        """Sample a specific position by index."""
        return self.positions[index % len(self.positions)]
