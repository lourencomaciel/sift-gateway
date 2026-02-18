from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError
import pytest

from sift_mcp.config.settings import (
    GatewayConfig,
    PaginationConfig,
    UpstreamConfig,
    _deep_merge,
    _SparseList,
    load_gateway_config,
)


def test_gateway_config_derived_paths(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    assert config.state_dir == tmp_path / "state"
    assert config.resources_dir == tmp_path / "resources"
    assert config.blobs_bin_dir == tmp_path / "blobs" / "bin"


def test_code_query_allowed_import_roots_defaults_none(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    assert config.code_query_allowed_import_roots is None


def test_code_query_allowed_import_roots_env_override(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_CODE_QUERY_ALLOWED_IMPORT_ROOTS",
        '["math","json","jmespath"]',
    )
    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.code_query_allowed_import_roots == [
        "math",
        "json",
        "jmespath",
    ]


def test_code_query_allowed_import_roots_reject_invalid_entries(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError):
        GatewayConfig(
            data_dir=tmp_path,
            code_query_allowed_import_roots=["math", "numpy.random"],
        )


def test_legacy_zstd_encoding_coerced_to_gzip(
    tmp_path: Path,
) -> None:
    """Existing configs with zstd encoding upgrade gracefully."""
    config = GatewayConfig(
        data_dir=tmp_path,
        envelope_canonical_encoding="zstd",
    )
    assert config.envelope_canonical_encoding.value == "gzip"


def test_legacy_zstd_encoding_from_state_file(
    tmp_path: Path,
) -> None:
    """State file with zstd encoding loads without error."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"envelope_canonical_encoding": "zstd"}),
        encoding="utf-8",
    )
    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.envelope_canonical_encoding.value == "gzip"


def test_load_gateway_config_reads_state_config(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"passthrough_max_bytes": 16384}),
        encoding="utf-8",
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.passthrough_max_bytes == 16384


def test_env_overrides_state_config(tmp_path: Path, monkeypatch) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"passthrough_max_bytes": 16384}),
        encoding="utf-8",
    )
    monkeypatch.setenv("SIFT_MCP_PASSTHROUGH_MAX_BYTES", "32768")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.passthrough_max_bytes == 32768


def test_nested_env_overrides_state_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__TRANSPORT", "http")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__URL", "https://from-env.example")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].prefix == "gh"
    assert config.upstreams[0].url == "https://from-env.example"


def test_nested_env_json_leaf_values_are_decoded(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__TRANSPORT", "stdio")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__COMMAND", "gh")
    monkeypatch.setenv(
        "SIFT_MCP_UPSTREAMS__0__ARGS", '["api","/repos/example"]'
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].args == ["api", "/repos/example"]


def test_nested_env_map_keys_preserve_case(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__TRANSPORT", "stdio")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__COMMAND", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__ENV__OPENAI_API_KEY", "secret")

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].env == {"OPENAI_API_KEY": "secret"}


def test_nested_env_map_leaf_json_like_string_stays_string(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__TRANSPORT", "http")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__URL", "https://api.example")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__HEADERS__X_CONFIG", '{"k":"v"}')

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].headers == {"X_CONFIG": '{"k":"v"}'}


def test_deep_merge_preserves_sparse_list_indices() -> None:
    base = [{"prefix": "a"}]
    override = _SparseList([None, None, {"prefix": "c"}])
    merged = _deep_merge(base, override)
    assert merged == [{"prefix": "a"}, None, {"prefix": "c"}]


def test_env_top_level_list_override_replaces_file_list(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_UPSTREAMS",
        '[{"prefix":"c","transport":"http","url":"https://c.example"}]',
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert [upstream.prefix for upstream in config.upstreams] == ["c"]


def test_nested_env_list_field_override_replaces_file_list(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__PREFIX", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__TRANSPORT", "stdio")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__COMMAND", "gh")
    monkeypatch.setenv("SIFT_MCP_UPSTREAMS__0__ARGS", '["api","/new/path"]')

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].args == ["api", "/new/path"]


def test_legacy_upstreams_key_raises_migration_error(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "upstreams": [
                    {
                        "prefix": "gh",
                        "transport": "http",
                        "url": "https://example.com",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="no longer supported"):
        load_gateway_config(data_dir_override=str(tmp_path))


def test_env_upstream_overrides_mcp_servers_format(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Env UPSTREAMS__* overrides must win over mcpServers file."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "gh": {
                        "url": "https://from-file.example",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "SIFT_MCP_UPSTREAMS__0__URL",
        "https://from-env.example",
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].url == "https://from-env.example"


def test_env_upstream_env_var_overrides_mcp_servers_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Env override adds env vars to mcpServers-defined upstream."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "gh": {
                        "command": "gh-mcp",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "SIFT_MCP_UPSTREAMS__0__ENV__GITHUB_TOKEN",
        "tok_from_env",
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.upstreams[0].env["GITHUB_TOKEN"] == "tok_from_env"


def test_gateway_sync_metadata_stripped_before_validation(
    tmp_path: Path,
) -> None:
    """_gateway_sync in config.json must not cause extra="forbid" error."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "gh": {"command": "gh-mcp"},
                },
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": "/tmp/src.json",
                    "gateway_name": "gw",
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].prefix == "gh"


def test_upstream_config_accepts_secret_ref() -> None:
    upstream = UpstreamConfig(
        prefix="gh",
        transport="stdio",
        command="gh",
        secret_ref="github",
    )
    assert upstream.secret_ref == "github"


def test_upstream_config_secret_ref_defaults_to_none() -> None:
    upstream = UpstreamConfig(
        prefix="gh",
        transport="stdio",
        command="gh",
    )
    assert upstream.secret_ref is None


@pytest.mark.parametrize(
    "bad_ref",
    [
        "",
        "   ",
        "../escape",
        "a/../b",
        "path/to/secret",
        "back\\slash",
        "/absolute",
    ],
)
def test_upstream_config_rejects_invalid_secret_ref(bad_ref: str) -> None:
    with pytest.raises(ValidationError):
        UpstreamConfig(
            prefix="gh",
            transport="stdio",
            command="gh",
            secret_ref=bad_ref,
        )


def test_upstream_config_accepts_inherit_parent_env() -> None:
    upstream = UpstreamConfig(
        prefix="gh",
        transport="stdio",
        command="gh",
        inherit_parent_env=True,
    )
    assert upstream.inherit_parent_env is True


def test_upstream_config_inherit_parent_env_defaults_to_false() -> None:
    upstream = UpstreamConfig(
        prefix="gh",
        transport="stdio",
        command="gh",
    )
    assert upstream.inherit_parent_env is False


def test_pagination_config_offset_requires_has_more_path() -> None:
    with pytest.raises(ValidationError):
        PaginationConfig(
            strategy="offset",
            offset_param_name="offset",
            page_size_param_name="limit",
        )


def test_pagination_config_page_number_requires_has_more_path() -> None:
    with pytest.raises(ValidationError):
        PaginationConfig(
            strategy="page_number",
            page_param_name="page",
        )


def test_pagination_config_cursor_allows_missing_has_more_path() -> None:
    config = PaginationConfig(
        strategy="cursor",
        cursor_response_path="$.paging.cursors.after",
        cursor_param_name="after",
    )
    assert config.has_more_response_path is None


def test_pagination_config_param_map_requires_paths() -> None:
    with pytest.raises(ValidationError):
        PaginationConfig(
            strategy="param_map",
        )


def test_pagination_config_param_map_accepts_map() -> None:
    config = PaginationConfig(
        strategy="param_map",
        next_params_response_paths={
            "cursor": "$.paging.cursors.after",
            "checkpoint": "$.paging.checkpoint",
        },
    )
    assert config.next_params_response_paths == {
        "cursor": "$.paging.cursors.after",
        "checkpoint": "$.paging.checkpoint",
    }
