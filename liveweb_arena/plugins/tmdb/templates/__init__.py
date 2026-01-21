"""TMDB question templates"""

# Original templates (some have static data risk)
from .movie_info import TMDBMovieInfoTemplate
from .movie_cast import TMDBMovieCastTemplate
from .movie_comparison import TMDBMovieComparisonTemplate
from .movie_crew import TMDBMovieCrewTemplate

# Dynamic/Anti-memorization templates
from .person_filmography import TMDBPersonFilmographyTemplate
from .movie_collection import TMDBMovieCollectionTemplate
from .aggregate import TMDBAggregateTemplate
from .cast_position import TMDBCastPositionTemplate
from .recent_works import TMDBRecentWorksTemplate

__all__ = [
    # Original (use with caution - some static data)
    "TMDBMovieInfoTemplate",
    "TMDBMovieCastTemplate",
    "TMDBMovieComparisonTemplate",
    "TMDBMovieCrewTemplate",
    # Dynamic/Anti-memorization (recommended)
    "TMDBPersonFilmographyTemplate",
    "TMDBMovieCollectionTemplate",
    "TMDBAggregateTemplate",
    "TMDBCastPositionTemplate",
    "TMDBRecentWorksTemplate",
]
