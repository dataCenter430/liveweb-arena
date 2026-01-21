"""CoinGecko question templates"""

from .price import CoinGeckoPriceTemplate
from .volume import CoinGeckoVolumeTemplate
from .comparison import CoinGeckoComparisonTemplate
from .rank import CoinGeckoRankTemplate

__all__ = [
    "CoinGeckoPriceTemplate",
    "CoinGeckoVolumeTemplate",
    "CoinGeckoComparisonTemplate",
    "CoinGeckoRankTemplate",
]
