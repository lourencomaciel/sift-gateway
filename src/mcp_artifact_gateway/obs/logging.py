"""Structured logging configuration for MCP Artifact Gateway."""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def configure_logging(*, json_output: bool = True, level: str = "INFO") -> None:
    """Configure structlog with JSON or console output."""
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if json_output:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(**initial_context: Any) -> structlog.stdlib.BoundLogger:
    """Get a bound structured logger with optional initial context."""
    return structlog.get_logger(**initial_context)


# Pre-defined event names for consistency
class LogEvents:
    """Standard log event names for the gateway."""
    # Startup
    STARTUP_BEGIN = "gateway.startup.begin"
    STARTUP_UPSTREAM_DISCOVERED = "gateway.startup.upstream_discovered"
    STARTUP_COMPLETE = "gateway.startup.complete"
    STARTUP_FAILED = "gateway.startup.failed"

    # Request processing
    REQUEST_RECEIVED = "gateway.request.received"
    REQUEST_KEY_COMPUTED = "gateway.request.key_computed"
    REUSE_HIT = "gateway.reuse.hit"
    REUSE_MISS = "gateway.reuse.miss"

    # Artifact creation
    ARTIFACT_CREATED = "gateway.artifact.created"
    ARTIFACT_ENVELOPE_SIZES = "gateway.artifact.envelope_sizes"
    ARTIFACT_OVERSIZE_JSON = "gateway.artifact.oversize_json"
    ARTIFACT_BINARY_BLOB_WRITE = "gateway.artifact.binary_blob_write"
    ARTIFACT_BINARY_BLOB_DEDUPE = "gateway.artifact.binary_blob_dedupe"

    # Mapping
    MAPPING_STARTED = "gateway.mapping.started"
    MAPPING_COMPLETED = "gateway.mapping.completed"
    MAPPING_FAILED = "gateway.mapping.failed"

    # Cursor
    CURSOR_ISSUED = "gateway.cursor.issued"
    CURSOR_VERIFIED = "gateway.cursor.verified"
    CURSOR_INVALID = "gateway.cursor.invalid"
    CURSOR_EXPIRED = "gateway.cursor.expired"
    CURSOR_STALE = "gateway.cursor.stale"

    # Pruning
    PRUNE_SOFT_DELETE = "gateway.prune.soft_delete"
    PRUNE_HARD_DELETE = "gateway.prune.hard_delete"
    PRUNE_BYTES_RECLAIMED = "gateway.prune.bytes_reclaimed"
    PRUNE_FS_RECONCILE = "gateway.prune.fs_reconcile"

    # Advisory lock
    ADVISORY_LOCK_ACQUIRED = "gateway.lock.acquired"
    ADVISORY_LOCK_TIMEOUT = "gateway.lock.timeout"

    # Quota
    QUOTA_CHECK = "gateway.quota.check"
    QUOTA_BREACH = "gateway.quota.breach"
    QUOTA_PRUNE_TRIGGERED = "gateway.quota.prune_triggered"
    QUOTA_PRUNE_COMPLETE = "gateway.quota.prune_complete"
    QUOTA_EXCEEDED = "gateway.quota.exceeded"
