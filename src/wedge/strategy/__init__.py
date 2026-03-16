"""Strategy module for edge detection and position sizing."""

from wedge.strategy.edge import (
    calculate_ev,
    detect_edges,
    estimate_slippage,
)
from wedge.strategy.events import (
    EventDetector,
    EventHistory,
    EventSignal,
    EventType,
    WeatherEvent,
    generate_event_signals,
)
from wedge.strategy.kelly import (
    KellyResult,
    fractional_kelly,
    legacy_fractional_kelly,
)
from wedge.strategy.ladder import evaluate_ladder
from wedge.strategy.models import EdgeSignal
from wedge.strategy.tail import evaluate_tail

__all__ = [
    # Edge detection
    "calculate_ev",
    "detect_edges",
    "estimate_slippage",
    # Kelly
    "KellyResult",
    "fractional_kelly",
    "legacy_fractional_kelly",
    # Strategies
    "evaluate_ladder",
    "evaluate_tail",
    # Models
    "EdgeSignal",
    # Events
    "EventDetector",
    "EventHistory",
    "EventSignal",
    "EventType",
    "WeatherEvent",
    "generate_event_signals",
]
