from __future__ import annotations

import pytest
import structlog

from mcp_artifact_gateway.obs.logging import (
    LogEvents,
    configure_logging,
    get_logger,
)


@pytest.fixture(autouse=True)
def _reset_structlog_config():
    """Save and restore structlog configuration so tests don't leak state."""
    old_config = structlog.get_config()
    yield
    structlog.configure(**old_config)


def test_configure_logging_json_does_not_raise() -> None:
    configure_logging(json_output=True, level="INFO")


def test_configure_logging_console_does_not_raise() -> None:
    configure_logging(json_output=False, level="DEBUG")


def test_get_logger_returns_bound_logger() -> None:
    configure_logging(json_output=True, level="INFO")
    log = get_logger(component="test")
    assert log is not None
    for method in ("info", "warning", "error", "debug"):
        assert callable(getattr(log, method, None)), f"missing method: {method}"


def test_log_events_startup_events() -> None:
    assert LogEvents.STARTUP_BEGIN == "gateway.startup.begin"
    assert (
        LogEvents.STARTUP_UPSTREAM_DISCOVERED
        == "gateway.startup.upstream_discovered"
    )
    assert LogEvents.STARTUP_COMPLETE == "gateway.startup.complete"
    assert LogEvents.STARTUP_FAILED == "gateway.startup.failed"


def test_log_events_request_events() -> None:
    assert LogEvents.REQUEST_RECEIVED == "gateway.request.received"
    assert LogEvents.REQUEST_KEY_COMPUTED == "gateway.request.key_computed"
    assert LogEvents.REUSE_HIT == "gateway.reuse.hit"
    assert LogEvents.REUSE_MISS == "gateway.reuse.miss"


def test_log_events_artifact_events() -> None:
    assert LogEvents.ARTIFACT_CREATED == "gateway.artifact.created"
    assert (
        LogEvents.ARTIFACT_ENVELOPE_SIZES == "gateway.artifact.envelope_sizes"
    )
    assert LogEvents.ARTIFACT_OVERSIZE_JSON == "gateway.artifact.oversize_json"
    assert (
        LogEvents.ARTIFACT_BINARY_BLOB_WRITE
        == "gateway.artifact.binary_blob_write"
    )
    assert (
        LogEvents.ARTIFACT_BINARY_BLOB_DEDUPE
        == "gateway.artifact.binary_blob_dedupe"
    )


def test_log_events_mapping_events() -> None:
    assert LogEvents.MAPPING_STARTED == "gateway.mapping.started"
    assert LogEvents.MAPPING_COMPLETED == "gateway.mapping.completed"
    assert LogEvents.MAPPING_FAILED == "gateway.mapping.failed"


def test_log_events_cursor_events() -> None:
    assert LogEvents.CURSOR_ISSUED == "gateway.cursor.issued"
    assert LogEvents.CURSOR_VERIFIED == "gateway.cursor.verified"
    assert LogEvents.CURSOR_INVALID == "gateway.cursor.invalid"
    assert LogEvents.CURSOR_EXPIRED == "gateway.cursor.expired"
    assert LogEvents.CURSOR_STALE == "gateway.cursor.stale"


def test_log_events_prune_events() -> None:
    assert LogEvents.PRUNE_SOFT_DELETE == "gateway.prune.soft_delete"
    assert LogEvents.PRUNE_HARD_DELETE == "gateway.prune.hard_delete"
    assert LogEvents.PRUNE_BYTES_RECLAIMED == "gateway.prune.bytes_reclaimed"
    assert LogEvents.PRUNE_FS_RECONCILE == "gateway.prune.fs_reconcile"


def test_log_events_advisory_lock_events() -> None:
    assert LogEvents.ADVISORY_LOCK_ACQUIRED == "gateway.lock.acquired"
    assert LogEvents.ADVISORY_LOCK_TIMEOUT == "gateway.lock.timeout"


def test_log_events_all_start_with_gateway_prefix() -> None:
    """All event constants should start with 'gateway.' prefix."""
    for attr_name in dir(LogEvents):
        if attr_name.startswith("_"):
            continue
        value = getattr(LogEvents, attr_name)
        assert isinstance(value, str), f"{attr_name} is not a string"
        assert value.startswith("gateway."), (
            f"{attr_name}={value!r} missing gateway. prefix"
        )


def test_get_logger_binds_correlation_fields() -> None:
    """get_logger should support binding correlation fields like session_id, artifact_id."""
    configure_logging(json_output=True, level="DEBUG")
    log = get_logger(
        session_id="sess_123",
        artifact_id="art_456",
        request_key="rk_789",
        payload_hash_full="ph_abc",
    )
    # The logger should be successfully created with these fields bound
    assert log is not None
    # Verify it's callable for structured log emission
    assert callable(getattr(log, "info", None))


def test_configure_logging_merge_contextvars_in_processors() -> None:
    """Structlog should use merge_contextvars for correlation field propagation."""
    configure_logging(json_output=True, level="INFO")
    config = structlog.get_config()
    processors = config.get("processors", [])
    processor_names = [getattr(p, "__name__", str(p)) for p in processors]
    # merge_contextvars should be in the processor chain
    assert any("contextvars" in name.lower() for name in processor_names), (
        f"merge_contextvars not found in processors: {processor_names}"
    )
