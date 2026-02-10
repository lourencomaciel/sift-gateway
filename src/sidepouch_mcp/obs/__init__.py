"""Re-export structured logging and metrics primitives."""

from sidepouch_mcp.obs.logging import (
    LogEvents,
    configure_logging,
    get_logger,
)
from sidepouch_mcp.obs.metrics import (
    GatewayMetrics,
    counter_reset,
    counter_value,
    get_metrics,
)

__all__ = [
    "GatewayMetrics",
    "LogEvents",
    "configure_logging",
    "counter_reset",
    "counter_value",
    "get_logger",
    "get_metrics",
]
