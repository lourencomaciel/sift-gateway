"""Unit tests for _strip_locators in worker_main."""

from __future__ import annotations

from sift_gateway.codegen.worker_main import _strip_locators


def test_dict_record_locator_removed() -> None:
    records: list = [{"mag": 4.5, "_locator": {"artifact_id": "a1"}}]
    _strip_locators(records)
    assert records == [{"mag": 4.5}]


def test_dict_record_preserves_other_keys() -> None:
    records: list = [
        {"a": 1, "b": 2, "_locator": {"artifact_id": "a1"}},
    ]
    _strip_locators(records)
    assert records == [{"a": 1, "b": 2}]


def test_scalar_wrapped_unwrapped() -> None:
    records: list = [
        {
            "_locator": {"artifact_id": "a1", "_scalar": True},
            "value": 0.8,
        },
    ]
    _strip_locators(records)
    assert records == [0.8]


def test_scalar_none_unwrapped() -> None:
    records: list = [
        {
            "_locator": {"artifact_id": "a1", "_scalar": True},
            "value": None,
        },
    ]
    _strip_locators(records)
    assert records == [None]


def test_scalar_false_unwrapped() -> None:
    records: list = [
        {
            "_locator": {"artifact_id": "a1", "_scalar": True},
            "value": False,
        },
    ]
    _strip_locators(records)
    assert records == [False]


def test_scalar_zero_unwrapped() -> None:
    records: list = [
        {
            "_locator": {"artifact_id": "a1", "_scalar": True},
            "value": 0,
        },
    ]
    _strip_locators(records)
    assert records == [0]


def test_dict_with_value_and_locator_no_scalar_not_unwrapped() -> None:
    """Real dict ``{"value": 3.2}`` — locator has no ``_scalar`` flag."""
    records: list = [
        {"_locator": {"artifact_id": "a1"}, "value": 3.2},
    ]
    _strip_locators(records)
    assert records == [{"value": 3.2}]


def test_dict_with_locator_value_and_extra_keys_no_scalar() -> None:
    """Dict with ``_locator`` + ``value`` + other keys, no ``_scalar``."""
    records: list = [
        {"_locator": {"artifact_id": "a1"}, "value": 3.2, "unit": "C"},
    ]
    _strip_locators(records)
    assert records == [{"value": 3.2, "unit": "C"}]


def test_real_dict_with_scalar_true_and_value_not_unwrapped() -> None:
    """Real dict ``{"_scalar": True, "value": 42}`` must not be unwrapped.

    After ``_with_locator`` the dict branch produces
    ``{"_scalar": True, "value": 42, "_locator": {...}}``.  The
    outer ``_scalar`` is user data; the locator has no ``_scalar``
    flag, so the record must be treated as a normal dict.
    """
    records: list = [
        {"_locator": {"artifact_id": "a1"}, "_scalar": True, "value": 42},
    ]
    _strip_locators(records)
    assert records == [{"_scalar": True, "value": 42}]


def test_dict_with_scalar_false_and_value_not_unwrapped() -> None:
    """Outer ``_scalar`` key with non-True value — not unwrapped."""
    records: list = [
        {"_locator": {"artifact_id": "a1"}, "_scalar": False, "value": 7},
    ]
    _strip_locators(records)
    assert records == [{"_scalar": False, "value": 7}]


def test_dict_with_scalar_true_no_value_not_unwrapped() -> None:
    """Real dict containing ``_scalar: True`` but no ``value`` key."""
    records: list = [
        {"_locator": {"artifact_id": "a1"}, "_scalar": True, "field_a": 10},
    ]
    _strip_locators(records)
    assert records == [{"_scalar": True, "field_a": 10}]


def test_dict_with_scalar_true_and_value_and_extra_keys_not_unwrapped() -> None:
    """Real dict containing ``_scalar``, ``value``, and extra keys."""
    records: list = [
        {
            "_locator": {"artifact_id": "a1"},
            "_scalar": True,
            "value": 42,
            "unit": "C",
        },
    ]
    _strip_locators(records)
    assert records == [{"_scalar": True, "value": 42, "unit": "C"}]


def test_malformed_scalar_wrapper_missing_value() -> None:
    """Scalar marker present but ``value`` key absent — treat as dict."""
    records: list = [
        {"_locator": {"artifact_id": "a1", "_scalar": True}},
    ]
    _strip_locators(records)
    assert records == [{}]


def test_records_without_locator_unchanged() -> None:
    records: list = [{"mag": 4.5}, {"mag": 5.0}]
    _strip_locators(records)
    assert records == [{"mag": 4.5}, {"mag": 5.0}]


def test_non_dict_records_unchanged() -> None:
    records: list = [42, "hello", None, True]
    _strip_locators(records)
    assert records == [42, "hello", None, True]


def test_empty_list_noop() -> None:
    records: list = []
    _strip_locators(records)
    assert records == []


def test_mixed_records() -> None:
    records: list = [
        {"mag": 4.5, "_locator": {"artifact_id": "a1"}},
        {
            "_locator": {"artifact_id": "a2", "_scalar": True},
            "value": 0.8,
        },
        {"no_locator": True},
        42,
    ]
    _strip_locators(records)
    assert records == [{"mag": 4.5}, 0.8, {"no_locator": True}, 42]
