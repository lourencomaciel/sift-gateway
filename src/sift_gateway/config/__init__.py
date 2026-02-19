"""Re-export configuration loading and validation entry points."""

from sift_gateway.config.settings import (
    GatewayConfig,
    UpstreamConfig,
    load_gateway_config,
)

__all__ = ["GatewayConfig", "UpstreamConfig", "load_gateway_config"]
