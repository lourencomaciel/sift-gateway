from mcp_artifact_gateway.envelope.responses import GatewayError, make_error


def test_make_error_builds_gateway_error() -> None:
    err = make_error("NOT_FOUND", "missing", artifact_id="art_1")
    assert isinstance(err, GatewayError)
    assert err.code == "NOT_FOUND"
    assert err.details["artifact_id"] == "art_1"
