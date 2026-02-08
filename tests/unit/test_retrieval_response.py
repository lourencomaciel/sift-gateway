from pydantic import ValidationError

from mcp_artifact_gateway.retrieval.response import BoundedResponse, make_response


def test_make_response_defaults() -> None:
    resp = make_response(items=[1, 2], truncated=False)
    assert resp.items == [1, 2]
    assert resp.truncated is False
    assert resp.omitted == {}
    assert resp.stats == {}


def test_bounded_response_forbids_extra() -> None:
    try:
        BoundedResponse(items=[], truncated=False, extra_field=1)
    except ValidationError:
        return
    assert False, "Expected ValidationError for extra_field"
