from __future__ import annotations

from sift_gateway.envelope.responses import (
    gateway_error,
    gateway_tool_result,
    select_response_mode,
)


def test_gateway_tool_result_full_mode() -> None:
    response = gateway_tool_result(
        response_mode="full",
        artifact_id="art_1",
        payload={"ok": True},
        lineage={"scope": "single", "artifact_ids": ["art_1"]},
    )
    assert response == {
        "response_mode": "full",
        "artifact_id": "art_1",
        "payload": {"ok": True},
        "lineage": {"scope": "single", "artifact_ids": ["art_1"]},
    }


def test_gateway_tool_result_schema_ref_mode() -> None:
    response = gateway_tool_result(
        response_mode="schema_ref",
        artifact_id="art_2",
        schemas=[{"root_path": "$", "fields": []}],
        pagination={"has_more": True},
    )
    assert response == {
        "response_mode": "schema_ref",
        "artifact_id": "art_2",
        "schemas": [{"root_path": "$", "fields": []}],
        "pagination": {"has_more": True},
    }


def test_select_response_mode_pagination_forces_schema_ref() -> None:
    mode = select_response_mode(
        has_pagination=True,
        full_payload={"response_mode": "full", "payload": {"x": 1}},
        schema_ref_payload={"response_mode": "schema_ref", "schemas": []},
        max_bytes=100,
    )
    assert mode == "schema_ref"


def test_select_response_mode_uses_full_when_under_cap_and_not_smaller() -> (
    None
):
    full_payload = {"response_mode": "full", "payload": {"x": "1234567890"}}
    schema_ref_payload = {
        "response_mode": "schema_ref",
        "schemas": [
            {
                "root_path": "$",
                "fields": [{"path": "$.x", "types": ["string"]}],
            }
        ],
    }
    mode = select_response_mode(
        has_pagination=False,
        full_payload=full_payload,
        schema_ref_payload=schema_ref_payload,
        max_bytes=10_000,
    )
    assert mode == "full"


def test_select_response_mode_uses_full_even_when_schema_ref_is_smaller() -> (
    None
):
    mode = select_response_mode(
        has_pagination=False,
        full_payload={"response_mode": "full", "payload": {"rows": ["x" * 50]}},
        schema_ref_payload={"response_mode": "schema_ref", "schemas": []},
        max_bytes=10_000,
    )
    assert mode == "full"


def test_select_response_mode_uses_schema_ref_when_full_exceeds_cap() -> None:
    mode = select_response_mode(
        has_pagination=False,
        full_payload={"response_mode": "full", "payload": "x" * 200},
        schema_ref_payload={"response_mode": "schema_ref", "schemas": []},
        max_bytes=50,
    )
    assert mode == "schema_ref"


def test_gateway_error_shape() -> None:
    response = gateway_error(
        "INVALID_ARGUMENT", "bad request", details={"field": "x"}
    )
    assert response == {
        "type": "gateway_error",
        "code": "INVALID_ARGUMENT",
        "message": "bad request",
        "details": {"field": "x"},
    }
