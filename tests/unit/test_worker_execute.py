"""Unit tests for _execute in worker_main."""

from __future__ import annotations

from sift_gateway.codegen.worker_main import _execute


def test_async_def_run_legacy() -> None:
    payload = {
        "code": (
            "async def run(data, schema, params):\n    return [len(data)]"
        ),
        "data": [{"id": 1}, {"id": 2}],
        "schema": {"fields": []},
        "params": {},
        "allowed_import_roots": ["json"],
    }
    result = _execute(payload)
    assert result["ok"] is True
    assert result["result"] == [2]


def test_async_def_run_multi() -> None:
    payload = {
        "code": (
            "async def run(artifacts, schemas, params):\n"
            "    return list(artifacts.keys())"
        ),
        "artifacts": {"a1": [{"id": 1}], "a2": [{"id": 2}]},
        "schemas": {"a1": {"fields": []}, "a2": {"fields": []}},
        "params": {},
        "allowed_import_roots": ["json"],
    }
    result = _execute(payload)
    assert result["ok"] is True
    assert sorted(result["result"]) == ["a1", "a2"]


def test_sync_def_run_still_works() -> None:
    payload = {
        "code": "def run(data, schema, params):\n    return data",
        "data": [{"v": 42}],
        "schema": {"fields": []},
        "params": {},
        "allowed_import_roots": ["json"],
    }
    result = _execute(payload)
    assert result["ok"] is True
    assert result["result"] == [{"v": 42}]


def test_transitive_stdlib_imports_allowed_from_allowlisted_stdlib() -> None:
    payload = {
        "code": (
            "from datetime import datetime\n\n"
            "def run(data, schema, params):\n"
            "    dt = datetime.strptime('2026-03-03', '%Y-%m-%d')\n"
            "    return [dt.year, dt.month, dt.day]\n"
        ),
        "data": [],
        "schema": {"fields": []},
        "params": {},
        "allowed_import_roots": ["datetime"],
    }
    result = _execute(payload)
    assert result["ok"] is True
    assert result["result"] == [2026, 3, 3]


def test_private_stdlib_root_stays_blocked_for_direct_import() -> None:
    payload = {
        "code": (
            "import _strptime\n\n"
            "def run(data, schema, params):\n"
            "    return []\n"
        ),
        "data": [],
        "schema": {"fields": []},
        "params": {},
        "allowed_import_roots": ["datetime"],
    }
    result = _execute(payload)
    assert result["ok"] is False
    assert result["error"]["code"] == "CODE_IMPORT_NOT_ALLOWED"
    assert "import not allowed: _strptime" in result["error"]["message"]
