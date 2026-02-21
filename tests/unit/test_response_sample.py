from __future__ import annotations

from sift_gateway.response_sample import (
    build_representative_item_sample,
    resolve_item_sequence,
    resolve_item_sequence_with_path,
)


def test_resolve_item_sequence_with_path_prefers_explicit_root_path() -> None:
    payload = {"data": [{"id": 1}, {"id": 2}], "items": [{"id": 99}]}
    items, root_path = resolve_item_sequence_with_path(
        payload,
        root_path="$.data",
    )

    assert root_path == "$.data"
    assert items == [{"id": 1}, {"id": 2}]


def test_resolve_item_sequence_with_path_detects_items_field() -> None:
    payload = {"items": [{"id": 1}, {"id": 2}], "meta": {"page": 1}}
    items, root_path = resolve_item_sequence_with_path(payload)

    assert root_path == "$.items"
    assert items == [{"id": 1}, {"id": 2}]


def test_resolve_item_sequence_with_path_handles_special_field_name() -> None:
    payload = {"orders-total": [{"id": 1}]}
    items, root_path = resolve_item_sequence_with_path(payload)

    assert root_path == "$['orders-total']"
    assert items == [{"id": 1}]


def test_resolve_item_sequence_with_path_returns_none_for_ambiguous_lists() -> None:
    items, root_path = resolve_item_sequence_with_path({"a": [1], "b": [2]})

    assert items is None
    assert root_path is None


def test_resolve_item_sequence_with_path_returns_root_for_list_payload() -> None:
    items, root_path = resolve_item_sequence_with_path([{"id": 1}])

    assert root_path == "$"
    assert items == [{"id": 1}]


def test_resolve_item_sequence_maintains_legacy_items_only_contract() -> None:
    payload = {"items": [{"value": "x" * 300}]}
    items = resolve_item_sequence(payload)
    sample = build_representative_item_sample(items or [])

    assert items == [{"value": "x" * 300}]
    assert sample is not None
    assert sample["sample_item_source_index"] == 0
    assert sample["sample_item_count"] == 1
    assert sample["sample_item_text_truncated"] is True
