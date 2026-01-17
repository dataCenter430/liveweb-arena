"""Taostats question templates"""

from .subnet import SubnetInfoTemplate
from .network import NetworkTemplate
from .price import PriceTemplate
from .comparison import ComparisonTemplate
from .analysis import AnalysisTemplate
from .variables import SubnetVariable, MetricVariable, SubnetMetric, SubnetSpec, MetricSpec

__all__ = [
    "SubnetInfoTemplate",
    "NetworkTemplate",
    "PriceTemplate",
    "ComparisonTemplate",
    "AnalysisTemplate",
    "SubnetVariable",
    "MetricVariable",
    "SubnetMetric",
    "SubnetSpec",
    "MetricSpec",
]
