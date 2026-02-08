from __future__ import annotations

from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.db.conn import create_pool, db_conn_info
from mcp_artifact_gateway.db.repos.mapping_repo import update_map_status_params
from mcp_artifact_gateway.db.repos.payloads_repo import payload_blob_params
from mcp_artifact_gateway.db.repos.prune_repo import soft_delete_expired_params
from mcp_artifact_gateway.db.repos.sessions_repo import upsert_session_params


def test_db_conn_info_uses_config_values() -> None:
    config = GatewayConfig(
        postgres_dsn="postgresql://localhost/test",
        postgres_statement_timeout_ms=1234,
    )
    info = db_conn_info(config)
    assert info.dsn.endswith("/test")
    assert info.statement_timeout_ms == 1234


def test_create_pool_uses_pool_sizes(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakePool:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr("mcp_artifact_gateway.db.conn.ConnectionPool", _FakePool)

    config = GatewayConfig(
        postgres_dsn="postgresql://localhost/test",
        postgres_statement_timeout_ms=5000,
        postgres_pool_min=3,
        postgres_pool_max=7,
    )
    pool = create_pool(config)

    assert isinstance(pool, _FakePool)
    assert captured["conninfo"] == "postgresql://localhost/test"
    assert captured["min_size"] == 3
    assert captured["max_size"] == 7
    assert captured["kwargs"] == {"options": "-c statement_timeout=5000"}


def test_repo_param_helpers_include_workspace() -> None:
    assert upsert_session_params("sess-1") == ("local", "sess-1")
    assert soft_delete_expired_params() == ("local",)

    payload_params = payload_blob_params(
        payload_hash_full="h",
        envelope=None,
        encoding="none",
        canonical_bytes=b"{}",
        canonical_len=2,
        canonicalizer_version="v1",
        payload_json_bytes=2,
        payload_binary_bytes_total=0,
        payload_total_bytes=2,
        contains_binary_refs=False,
    )
    assert payload_params[0] == "local"
    assert payload_params[1] == "h"

    mapping_params = update_map_status_params(
        artifact_id="art_1",
        map_kind="none",
        map_status="pending",
        mapper_version="v1",
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error=None,
    )
    assert mapping_params[-2] == "local"
    assert mapping_params[-1] == "art_1"
