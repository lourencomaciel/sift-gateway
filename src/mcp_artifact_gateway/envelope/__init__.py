"""Envelope models and normalisation layer.

Re-exports the core types and normalisation functions so that consuming
modules can do::

    from mcp_artifact_gateway.envelope import Envelope, normalize_success
"""

from __future__ import annotations

from mcp_artifact_gateway.envelope.model import (
    ContentPart,
    ContentPartBinaryRef,
    ContentPartJson,
    ContentPartResourceRef,
    ContentPartText,
    Envelope,
    EnvelopeMeta,
    ErrorBlock,
    UpstreamPagination,
)
from mcp_artifact_gateway.envelope.normalize import (
    normalize_error,
    normalize_success,
    normalize_timeout,
    normalize_transport_error,
)

__all__ = [
    # Models
    "ContentPart",
    "ContentPartBinaryRef",
    "ContentPartJson",
    "ContentPartResourceRef",
    "ContentPartText",
    "Envelope",
    "EnvelopeMeta",
    "ErrorBlock",
    "UpstreamPagination",
    # Normalisation
    "normalize_error",
    "normalize_success",
    "normalize_timeout",
    "normalize_transport_error",
]
