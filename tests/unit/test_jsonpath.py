import pytest

from mcp_artifact_gateway.query.jsonpath import (
    BudgetExceededError,
    IndexSegment,
    PropertySegment,
    RootSegment,
    WildcardSegment,
    evaluate_path,
    normalize_jsonpath,
    parse_jsonpath,
)


def test_parse_and_normalize() -> None:
    segments = parse_jsonpath("$['a'][0].b")
    assert isinstance(segments[0], RootSegment)
    assert isinstance(segments[1], PropertySegment)
    assert isinstance(segments[2], IndexSegment)
    assert isinstance(segments[3], PropertySegment)

    normalized = normalize_jsonpath("$['a'][0].b")
    assert normalized == "$.a[0].b"


def test_invalid_path() -> None:
    with pytest.raises(ValueError):
        parse_jsonpath("a.b")


def test_evaluate_path_order_and_wildcard() -> None:
    obj = {"a": {"b": 1, "a": 2}}
    segments = parse_jsonpath("$.a[*]")
    results = evaluate_path(obj, segments)
    # Wildcard on dict should be in lex order: 'a' then 'b'
    values = [value for _, value in results]
    assert values == [2, 1]


def test_total_wildcard_cap() -> None:
    obj = {"a": [1, 2, 3]}
    segments = parse_jsonpath("$.a[*]")
    with pytest.raises(BudgetExceededError):
        evaluate_path(obj, segments, max_wildcard_expansion_total=2)
