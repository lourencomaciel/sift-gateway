from __future__ import annotations

import json
from pathlib import Path

from mcp_artifact_gateway.config.settings import (
    _SparseList,
    _deep_merge,
    GatewayConfig,
    load_gateway_config,
)


def test_gateway_config_derived_paths(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    assert config.state_dir == tmp_path / "state"
    assert config.resources_dir == tmp_path / "resources"
    assert config.blobs_bin_dir == tmp_path / "blobs" / "bin"


def test_load_gateway_config_reads_state_config(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"postgres_dsn": "postgresql://example.local/db"}),
        encoding="utf-8",
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.postgres_dsn == "postgresql://example.local/db"


def test_env_overrides_state_config(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"postgres_dsn": "postgresql://from-file/db"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCP_GATEWAY_POSTGRES_DSN", "postgresql://from-env/db")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.postgres_dsn == "postgresql://from-env/db"


def test_nested_env_overrides_state_config(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "upstreams": [
                    {
                        "prefix": "gh",
                        "transport": "http",
                        "url": "https://from-file.example",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__URL", "https://from-env.example")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].prefix == "gh"
    assert config.upstreams[0].url == "https://from-env.example"


def test_nested_env_json_leaf_values_are_decoded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__TRANSPORT", "stdio")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__COMMAND", "gh")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__ARGS", '["api","/repos/example"]')

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].args == ["api", "/repos/example"]


def test_nested_env_map_keys_preserve_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__TRANSPORT", "stdio")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__COMMAND", "gh")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__ENV__OPENAI_API_KEY", "secret")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].env == {"OPENAI_API_KEY": "secret"}


def test_nested_env_map_leaf_json_like_string_stays_string(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__TRANSPORT", "http")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__URL", "https://api.example")
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__HEADERS__X_CONFIG", '{"k":"v"}')

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].headers == {"X_CONFIG": '{"k":"v"}'}


def test_deep_merge_preserves_sparse_list_indices() -> None:
    base = [{"prefix": "a"}]
    override = _SparseList([None, None, {"prefix": "c"}])
    merged = _deep_merge(base, override)
    assert merged == [{"prefix": "a"}, None, {"prefix": "c"}]


def test_env_top_level_list_override_replaces_file_list(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "upstreams": [
                    {"prefix": "a", "transport": "http", "url": "https://a.example"},
                    {"prefix": "b", "transport": "http", "url": "https://b.example"},
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "MCP_GATEWAY_UPSTREAMS",
        '[{"prefix":"c","transport":"http","url":"https://c.example"}]',
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert [upstream.prefix for upstream in config.upstreams] == ["c"]


def test_nested_env_list_field_override_replaces_file_list(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "upstreams": [
                    {
                        "prefix": "gh",
                        "transport": "stdio",
                        "command": "gh",
                        "args": ["api", "/old/path"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCP_GATEWAY_UPSTREAMS__0__ARGS", '["api","/new/path"]')

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].args == ["api", "/new/path"]
