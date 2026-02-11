"""Tests for gateway.status tool implementation."""

from __future__ import annotations

from pathlib import Path

from sidepouch_mcp.config.settings import GatewayConfig
from sidepouch_mcp.constants import (
    CANONICALIZER_VERSION,
    CURSOR_VERSION,
    MAPPER_VERSION,
    PRNG_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
)
from sidepouch_mcp.tools.status import (
    build_status_response,
    build_status_response_with_runtime,
    probe_db,
    probe_fs,
)


def _default_config() -> GatewayConfig:
    return GatewayConfig()


def test_build_status_response_contains_all_version_constants() -> None:
    config = _default_config()
    result = build_status_response(config)

    assert result["type"] == "gateway_status"
    versions = result["versions"]
    assert versions["canonicalizer_version"] == CANONICALIZER_VERSION
    assert versions["mapper_version"] == MAPPER_VERSION
    assert versions["traversal_contract_version"] == TRAVERSAL_CONTRACT_VERSION
    assert versions["cursor_version"] == CURSOR_VERSION
    assert versions["prng_version"] == PRNG_VERSION


def test_build_status_response_contains_all_budget_fields() -> None:
    config = _default_config()
    result = build_status_response(config)

    budgets = result["budgets"]
    expected_budget_keys = {
        "max_items",
        "max_bytes_out",
        "max_wildcards",
        "max_compute_steps",
        "max_json_part_parse_bytes",
        "max_full_map_bytes",
        "max_bytes_read_partial_map",
        "max_compute_steps_partial_map",
        "max_depth_partial_map",
        "max_records_sampled_partial",
        "max_record_bytes_partial",
        "max_leaf_paths_partial",
        "artifact_search_max_limit",
    }
    assert set(budgets.keys()) == expected_budget_keys

    # Verify values match config
    assert budgets["max_items"] == config.max_items
    assert budgets["max_bytes_out"] == config.max_bytes_out
    assert budgets["max_wildcards"] == config.max_wildcards
    assert budgets["max_compute_steps"] == config.max_compute_steps
    assert (
        budgets["artifact_search_max_limit"] == config.artifact_search_max_limit
    )


def test_build_status_response_includes_where_canonicalization_mode() -> None:
    config = _default_config()
    result = build_status_response(config)

    assert (
        result["where_canonicalization_mode"]
        == config.where_canonicalization_mode.value
    )


def test_build_status_response_includes_mapping_mode() -> None:
    config = _default_config()
    result = build_status_response(config)

    assert result["mapping_mode"] == config.mapping_mode.value


def test_build_status_response_includes_storage_caps() -> None:
    config = _default_config()
    result = build_status_response(config)

    caps = result["storage_caps"]
    assert caps["max_binary_blob_bytes"] == config.max_binary_blob_bytes
    assert caps["max_payload_total_bytes"] == config.max_payload_total_bytes
    assert caps["max_total_storage_bytes"] == config.max_total_storage_bytes


def test_build_status_response_includes_cursor_settings() -> None:
    config = _default_config()
    result = build_status_response(config)

    assert result["cursor"]["cursor_ttl_minutes"] == config.cursor_ttl_minutes


def test_build_status_response_defaults_db_and_fs_to_not_probed() -> None:
    """When called without runtime probes, db/fs report 'not probed'."""
    config = _default_config()
    result = build_status_response(config)

    assert result["db"]["ok"] is False
    assert result["db"]["error"] == "not probed"
    assert result["fs"]["ok"] is False
    assert result["fs"]["error"] == "not probed"
    assert result["upstreams"] == []


def test_build_status_response_with_runtime_uses_provided_health() -> None:
    config = _default_config()
    db_health = {"ok": True}
    fs_health = {
        "ok": True,
        "paths": {"data_dir": True, "state_dir": True, "blobs_bin_dir": True},
    }
    result = build_status_response_with_runtime(
        config,
        db_health=db_health,
        fs_health=fs_health,
    )
    assert result["db"] == db_health
    assert result["fs"] == fs_health


def test_build_status_response_with_runtime_includes_cursor_secrets_info() -> (
    None
):
    config = _default_config()
    secrets_info = {
        "signing_version": "v2",
        "active_versions": ["v1", "v2"],
    }
    result = build_status_response_with_runtime(
        config,
        cursor_secrets_info=secrets_info,
    )
    cursor = result["cursor"]
    assert cursor["cursor_ttl_minutes"] == config.cursor_ttl_minutes
    assert cursor["secrets_loaded"] is True
    assert cursor["active_secret_count"] == 2


def test_build_status_response_without_cursor_secrets_omits_secret_fields() -> (
    None
):
    config = _default_config()
    result = build_status_response_with_runtime(config)
    cursor = result["cursor"]
    assert cursor["cursor_ttl_minutes"] == config.cursor_ttl_minutes
    assert "secrets_loaded" not in cursor
    assert "active_secret_count" not in cursor


def test_build_status_response_with_runtime_upstreams() -> None:
    config = _default_config()
    upstreams = [
        {
            "prefix": "demo",
            "instance_id": "inst_demo",
            "connected": True,
            "tool_count": 2,
        },
    ]
    result = build_status_response_with_runtime(config, upstreams=upstreams)
    assert result["upstreams"] == upstreams


# ---------------------------------------------------------------------------
# probe_db tests
# ---------------------------------------------------------------------------
class _FakeDbCursor:
    def fetchone(self) -> tuple[int]:
        return (1,)


class _FakeDbConn:
    def execute(self, _query: str) -> _FakeDbCursor:
        return _FakeDbCursor()


class _FakeDbConnCtx:
    def __enter__(self) -> _FakeDbConn:
        return _FakeDbConn()

    def __exit__(self, *args: object) -> None:
        pass


class _FakeDbPool:
    def connection(self) -> _FakeDbConnCtx:
        return _FakeDbConnCtx()


class _FailingDbConn:
    def execute(self, _query: str) -> None:
        raise RuntimeError("connection refused")


class _FailingDbConnCtx:
    def __enter__(self) -> _FailingDbConn:
        return _FailingDbConn()

    def __exit__(self, *args: object) -> None:
        pass


class _FailingDbPool:
    def connection(self) -> _FailingDbConnCtx:
        return _FailingDbConnCtx()


def test_probe_db_returns_ok_when_pool_responds() -> None:
    result = probe_db(_FakeDbPool())
    assert result == {"ok": True}


def test_probe_db_returns_error_when_pool_is_none() -> None:
    result = probe_db(None)
    assert result["ok"] is False
    assert "no db pool" in result["error"]


def test_probe_db_returns_error_on_exception() -> None:
    result = probe_db(_FailingDbPool())
    assert result["ok"] is False
    assert result["error"] == "db probe failed"
    assert result["error_type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# probe_fs tests
# ---------------------------------------------------------------------------
def test_probe_fs_all_dirs_exist(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    # Create required dirs
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.blobs_bin_dir.mkdir(parents=True, exist_ok=True)

    result = probe_fs(config)
    assert result["ok"] is True
    assert result["paths"]["data_dir"] is True
    assert result["paths"]["state_dir"] is True
    assert result["paths"]["blobs_bin_dir"] is True
    assert "error" not in result


def test_probe_fs_missing_dirs(tmp_path: Path) -> None:
    # Point to a non-existent subdir so data_dir itself does not exist
    config = GatewayConfig(data_dir=tmp_path / "nonexistent")

    result = probe_fs(config)
    assert result["ok"] is False
    assert result["paths"]["data_dir"] is False
    assert "error" in result
    assert "missing directories" in result["error"]


def test_probe_fs_partial_dirs(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    # Only create data_dir and state_dir, skip blobs_bin_dir
    config.state_dir.mkdir(parents=True, exist_ok=True)

    result = probe_fs(config)
    assert result["ok"] is False
    assert result["paths"]["data_dir"] is True
    assert result["paths"]["state_dir"] is True
    assert result["paths"]["blobs_bin_dir"] is False
    assert "blobs_bin_dir" in result["error"]
