from __future__ import annotations

import json

from sidepouch_mcp.mcp.handlers.artifact_next_page import (
    _extract_pagination_state,
)
from sidepouch_mcp.pagination.extract import PaginationState


def _envelope_json(
    pagination_data: dict | None = None,
) -> str:
    meta: dict = {"warnings": []}
    if pagination_data is not None:
        meta["_gateway_pagination"] = pagination_data
    envelope = {
        "type": "mcp_envelope",
        "upstream_instance_id": "inst_meta",
        "upstream_prefix": "meta-ads",
        "tool": "get_ads",
        "status": "ok",
        "content": [],
        "error": None,
        "meta": meta,
    }
    return json.dumps(envelope)


def test_extract_pagination_state_from_json_string() -> None:
    state_data = PaginationState(
        upstream_prefix="meta-ads",
        tool_name="get_ads",
        original_args={"account_id": "act_123"},
        next_params={"after": "CURSOR"},
        page_number=0,
    ).to_dict()
    raw = _envelope_json(state_data)
    state = _extract_pagination_state(raw)
    assert state is not None
    assert state.upstream_prefix == "meta-ads"
    assert state.tool_name == "get_ads"
    assert state.next_params == {"after": "CURSOR"}
    assert state.page_number == 0


def test_extract_pagination_state_from_dict() -> None:
    state_data = PaginationState(
        upstream_prefix="api",
        tool_name="list",
        original_args={},
        next_params={"offset": 100},
        page_number=1,
    ).to_dict()
    envelope_dict = json.loads(_envelope_json(state_data))
    state = _extract_pagination_state(envelope_dict)
    assert state is not None
    assert state.next_params == {"offset": 100}
    assert state.page_number == 1


def test_extract_pagination_state_no_pagination() -> None:
    raw = _envelope_json(None)
    state = _extract_pagination_state(raw)
    assert state is None


def test_extract_pagination_state_invalid_json() -> None:
    state = _extract_pagination_state("not valid json {{{")
    assert state is None


def test_extract_pagination_state_no_meta() -> None:
    envelope = {"type": "mcp_envelope"}
    state = _extract_pagination_state(json.dumps(envelope))
    assert state is None


def test_extract_pagination_state_non_dict_meta() -> None:
    envelope = {"meta": "not_a_dict"}
    state = _extract_pagination_state(json.dumps(envelope))
    assert state is None


def test_extract_pagination_state_non_string_non_dict() -> None:
    state = _extract_pagination_state(12345)
    assert state is None


def test_extract_pagination_state_non_dict_pagination() -> None:
    envelope = {"meta": {"_gateway_pagination": "not_dict"}}
    state = _extract_pagination_state(json.dumps(envelope))
    assert state is None
