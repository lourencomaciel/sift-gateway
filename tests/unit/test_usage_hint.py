from __future__ import annotations

from sift_gateway.tools.usage_hint import build_usage_hint


def _describe(
    *,
    map_status: str = "complete",
    map_kind: str = "full",
    roots: list | None = None,
) -> dict:
    return {
        "artifact_id": "art_test",
        "mapping": {
            "map_kind": map_kind,
            "map_status": map_status,
        },
        "roots": roots or [],
    }


def _root(
    *,
    root_path: str = "$.data",
    root_shape: str = "array",
    count_estimate: int | None = 10,
    fields_top: dict | None = None,
    sample_indices: list | None = None,
    sampled_record_count: int | None = None,
) -> dict:
    r: dict = {
        "root_path": root_path,
        "root_shape": root_shape,
        "count_estimate": count_estimate,
    }
    if fields_top is not None:
        r["fields_top"] = fields_top
    if sample_indices is not None:
        r["sample_indices"] = sample_indices
    if sampled_record_count is not None:
        r["sampled_record_count"] = sampled_record_count
    return r


def test_pending_mapping_hint() -> None:
    desc = _describe(map_status="pending")
    hint = build_usage_hint("art_1", desc)
    assert "Mapping in progress" in hint
    assert 'artifact(action="query"' in hint
    assert "art_1" in hint


def test_ready_status_treated_as_complete() -> None:
    desc = _describe(
        map_status="ready",
        roots=[_root(root_path="$.data", count_estimate=5)],
    )
    hint = build_usage_hint("art_ready", desc)
    assert "Mapping in progress" not in hint
    assert "5 records" in hint


def test_empty_roots_suggests_get() -> None:
    desc = _describe(roots=[])
    hint = build_usage_hint("art_2", desc)
    assert 'artifact(action="query"' in hint
    assert "art_2" in hint
    assert "No structured mapping" in hint


def test_array_root_with_fields() -> None:
    desc = _describe(
        roots=[
            _root(
                root_path="$.result.data",
                count_estimate=100,
                fields_top={
                    "name": {"string": 50},
                    "status": {"string": 50},
                    "id": {"string": 50},
                },
            )
        ]
    )
    hint = build_usage_hint("art_3", desc)
    assert "100 records" in hint
    assert "$.result.data" in hint
    assert "name" in hint
    assert "status" in hint
    assert 'artifact(action="query"' in hint
    assert 'query_kind="code"' in hint
    assert "no pagination" in hint
    assert "auto-wrapped to a list" in hint
    assert "Minimize context" in hint
    assert "art_3" in hint


def test_dict_root_suggests_get() -> None:
    desc = _describe(
        roots=[
            _root(
                root_path="$.config",
                root_shape="dict",
                count_estimate=None,
            )
        ]
    )
    hint = build_usage_hint("art_4", desc)
    assert "dict" in hint
    assert 'artifact(action="query"' in hint
    assert 'query_kind="code"' not in hint


def test_array_hint_includes_available_code_query_packages() -> None:
    desc = _describe(roots=[_root(root_path="$.items", count_estimate=20)])
    hint = build_usage_hint(
        "art_pkg",
        desc,
        code_query_packages=["jmespath", "numpy"],
    )
    assert (
        "Available code-query packages in this runtime: jmespath, numpy" in hint
    )


def test_array_hint_handles_empty_code_query_package_list() -> None:
    desc = _describe(roots=[_root(root_path="$.items", count_estimate=20)])
    hint = build_usage_hint(
        "art_pkg_none",
        desc,
        code_query_packages=[],
    )
    assert "No third-party code-query packages are currently available" in hint


def test_array_hint_omits_code_query_when_disabled() -> None:
    desc = _describe(roots=[_root(root_path="$.items", count_estimate=20)])
    hint = build_usage_hint(
        "art_code_disabled",
        desc,
        code_query_enabled=False,
    )
    assert 'query_kind="code"' not in hint


def test_sampled_root_mentions_sample() -> None:
    desc = _describe(
        roots=[
            _root(
                root_path="$.items",
                count_estimate=500,
                sample_indices=[0, 5, 10, 15, 20],
                sampled_record_count=5,
            )
        ]
    )
    hint = build_usage_hint("art_5", desc)
    assert "Sampled 5 of ~500" in hint


def test_multiple_roots_lists_alternatives() -> None:
    desc = _describe(
        roots=[
            _root(root_path="$.data", count_estimate=50),
            _root(
                root_path="$.paging",
                root_shape="dict",
                count_estimate=None,
            ),
        ]
    )
    hint = build_usage_hint("art_6", desc)
    assert "Also available" in hint
    assert "$.paging" in hint


def test_no_fields_top_omits_fields() -> None:
    desc = _describe(roots=[_root(root_path="$.rows", count_estimate=5)])
    hint = build_usage_hint("art_7", desc)
    assert "Fields:" not in hint
    assert "5 records" in hint


def test_array_root_suggest_where_filter() -> None:
    desc = _describe(roots=[_root(root_path="$.items", count_estimate=20)])
    hint = build_usage_hint("art_8", desc)
    assert "where" in hint.lower()


def test_hint_always_nonempty() -> None:
    desc = _describe(map_status="unknown_status", roots=[])
    hint = build_usage_hint("art_9", desc)
    assert len(hint) > 0


def test_fields_limited_to_eight() -> None:
    many_fields = {f"field_{i}": {"string": 10} for i in range(20)}
    desc = _describe(
        roots=[
            _root(
                root_path="$.data",
                count_estimate=10,
                fields_top=many_fields,
            )
        ]
    )
    hint = build_usage_hint("art_10", desc)
    # Should not list all 20 fields
    field_mentions = sum(1 for i in range(20) if f"field_{i}" in hint)
    assert field_mentions <= 8


def test_select_paths_limited_to_four() -> None:
    fields = {f"col_{i}": {"string": 10} for i in range(10)}
    desc = _describe(
        roots=[
            _root(
                root_path="$.data",
                count_estimate=5,
                fields_top=fields,
            )
        ]
    )
    hint = build_usage_hint("art_11", desc)
    # The select_paths suggestion should have at most 4 fields
    select_section = hint.split("select_paths=[")[1].split("]")[0]
    quoted_count = select_section.count('"')
    # Each field has opening and closing quotes
    assert quoted_count // 2 <= 4


def test_artifact_forwarding_tip_in_array_hint() -> None:
    desc = _describe(roots=[_root(root_path="$.data", count_estimate=5)])
    hint = build_usage_hint("art_fwd", desc)
    assert "art_fwd" in hint
    assert "pass" in hint.lower()
    assert "art_fwd:$.path" in hint
    assert "art_fwd:$.items[0].name" in hint


def test_artifact_forwarding_tip_in_dict_hint() -> None:
    desc = _describe(
        roots=[
            _root(
                root_path="$.config",
                root_shape="dict",
                count_estimate=None,
            )
        ]
    )
    hint = build_usage_hint("art_fwd2", desc)
    assert "art_fwd2" in hint
    assert "art_fwd2:$.path" in hint
