from mcp_artifact_gateway.envelope.normalize import (
    normalize_error,
    normalize_success,
    normalize_timeout,
    normalize_transport_error,
)


def test_normalize_success() -> None:
    env = normalize_success(
        upstream_instance_id="u1",
        upstream_prefix="github",
        tool="search",
        mcp_result=[{"type": "text", "text": "ok"}],
    )
    assert env.status == "ok"
    assert env.content[0].type == "text"


def test_normalize_error() -> None:
    env = normalize_error(
        upstream_instance_id="u1",
        upstream_prefix="github",
        tool="search",
        error_code="UPSTREAM_ERROR",
        message="boom",
    )
    assert env.status == "error"
    assert env.error is not None
    assert env.error.code == "UPSTREAM_ERROR"


def test_normalize_timeout() -> None:
    env = normalize_timeout(
        upstream_instance_id="u1",
        upstream_prefix="github",
        tool="search",
        timeout_seconds=1.5,
    )
    assert env.error is not None
    assert env.error.code == "UPSTREAM_TIMEOUT"


def test_normalize_transport_error() -> None:
    env = normalize_transport_error(
        upstream_instance_id="u1",
        upstream_prefix="github",
        tool="search",
        message="network",
    )
    assert env.error is not None
    assert env.error.code == "TRANSPORT_ERROR"
