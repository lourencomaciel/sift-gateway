"""Re-export structured logging and metrics primitives."""

from sift_gateway.obs.logging import (
    LogEvents,
    configure_logging,
    get_logger,
)
from sift_gateway.obs.metrics import (
    Counter,
    GatewayMetrics,
    counter_reset,
    counter_value,
    get_metrics,
)

__all__ = [
    "Counter",
    "GatewayMetrics",
    "LogEvents",
    "configure_logging",
    "counter_reset",
    "counter_value",
    "get_logger",
    "get_metrics",
]
