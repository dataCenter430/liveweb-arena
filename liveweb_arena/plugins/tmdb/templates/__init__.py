"""TMDB question templates"""

from .movie_info import TMDBMovieInfoTemplate
from .movie_cast import TMDBMovieCastTemplate
from .movie_comparison import TMDBMovieComparisonTemplate

__all__ = [
    "TMDBMovieInfoTemplate",
    "TMDBMovieCastTemplate",
    "TMDBMovieComparisonTemplate",
]
