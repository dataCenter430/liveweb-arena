"""CoinGecko question templates"""

from .price import CoinGeckoPriceTemplate
from .volume import CoinGeckoVolumeTemplate
from .comparison import CoinGeckoComparisonTemplate
from .rank import CoinGeckoRankTemplate
from .top_movers import CoinGeckoTopMoversTemplate
from .supply import CoinGeckoSupplyTemplate
from .ath import CoinGeckoATHTemplate

__all__ = [
    "CoinGeckoPriceTemplate",
    "CoinGeckoVolumeTemplate",
    "CoinGeckoComparisonTemplate",
    "CoinGeckoRankTemplate",
    "CoinGeckoTopMoversTemplate",
    "CoinGeckoSupplyTemplate",
    "CoinGeckoATHTemplate",
]
