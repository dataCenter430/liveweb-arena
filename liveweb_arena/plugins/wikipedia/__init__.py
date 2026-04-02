"""Wikipedia plugin for browsing and querying Wikipedia content."""

from .wikipedia import WikipediaPlugin

# Import templates to register them
from . import templates

__all__ = ["WikipediaPlugin"]
