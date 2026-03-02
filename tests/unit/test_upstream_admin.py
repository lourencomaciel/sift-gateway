"""Tests for upstream admin helpers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sift_gateway.config.upstream_admin import (
    _delete_oauth_cache_dir,
    _delete_secret_file,
    _load_config_server_entry,
    _oauth_login_access_token,
    _probe_one_upstream,
    _probe_upstream_configs,
    _read_secret_from_file,
    _record_from_config_server,
    _resolve_oauth_callback_url_headless,
    _secret_ref_is_still_referenced,
    inspect_upstream,
    list_upstreams,
    login_upstream,
    normalize_input_servers,
    parse_kv_pairs,
    probe_upstreams,
    reconcile_after_add,
    remove_upstream,
    resolve_upstream_data_dir,
    set_upstream_auth,
    set_upstream_enabled,
)
from sift_gateway.config.upstream_secrets import (
    oauth_cache_dir_path,
    read_secret,
    write_secret,
)


def _write_gateway_config(data_dir: Path, payload: dict[str, object]) -> None:
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def _read_gateway_config(data_dir: Path) -> dict[str, object]:
    return json.loads((data_dir / "state" / "config.json").read_text())


def test_parse_kv_pairs_empty_is_ok() -> None:
    assert parse_kv_pairs(None, option_name="--env") == {}
    assert parse_kv_pairs([], option_name="--env") == {}


def test_parse_kv_pairs_parses_updates() -> None:
    parsed = parse_kv_pairs(
        ["TOKEN=abc", "A=1", "TOKEN=def"],
        option_name="--env",
    )
    assert parsed == {"TOKEN": "def", "A": "1"}


def test_parse_kv_pairs_rejects_invalid_shape() -> None:
    with pytest.raises(ValueError, match="expected KEY=VALUE"):
        parse_kv_pairs(["MISSING"], option_name="--env")


def test_parse_kv_pairs_rejects_empty_key() -> None:
    with pytest.raises(ValueError, match="key must be non-empty"):
        parse_kv_pairs(["=value"], option_name="--env")


def test_resolve_upstream_data_dir_uses_env_when_unset(
    monkeypatch, tmp_path: Path
) -> None:
    env_dir = tmp_path / "alt-dir"
    monkeypatch.setenv("SIFT_GATEWAY_DATA_DIR", str(env_dir))

    assert resolve_upstream_data_dir() == env_dir.resolve()


def test_load_config_server_entry_returns_none_when_config_missing(
    tmp_path: Path,
) -> None:
    assert (
        _load_config_server_entry(data_dir=tmp_path, server="missing")
        is None
    )


def test_load_config_server_entry_returns_none_for_invalid_json(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text("{invalid", encoding="utf-8")

    assert _load_config_server_entry(data_dir=tmp_path, server="gh") is None


def test_load_config_server_entry_returns_none_for_non_object_json(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text("[]", encoding="utf-8")

    assert _load_config_server_entry(data_dir=tmp_path, server="gh") is None


def test_load_config_server_entry_returns_none_for_invalid_mcp_shape(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": []}),
        encoding="utf-8",
    )

    assert _load_config_server_entry(data_dir=tmp_path, server="gh") is None


def test_record_from_config_server_falls_back_to_empty_args_on_bad_json(
    monkeypatch, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh", "args": ["--ok"]}}},
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.entry_to_registry_payload",
        lambda **_kwargs: {
            "transport": "stdio",
            "command": "gh",
            "url": None,
            "args_json": "{not-json",
            "secret_ref": "gh",
            "enabled": True,
        },
    )

    record = _record_from_config_server(data_dir=tmp_path, server="gh")
    assert record is not None
    assert record["args"] == []


def test_record_from_config_server_returns_none_when_server_missing(
    tmp_path: Path,
) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    assert _record_from_config_server(data_dir=tmp_path, server="gh") is None


def test_read_secret_from_file_handles_invalid_shapes(tmp_path: Path) -> None:
    secret_path = tmp_path / "state" / "upstream_secrets" / "gh.json"
    secret_path.parent.mkdir(parents=True, exist_ok=True)

    assert _read_secret_from_file(data_dir=tmp_path, ref="gh") is None

    secret_path.write_text("{invalid", encoding="utf-8")
    assert _read_secret_from_file(data_dir=tmp_path, ref="gh") is None

    secret_path.write_text("[]", encoding="utf-8")
    assert _read_secret_from_file(data_dir=tmp_path, ref="gh") is None

    secret_path.write_text(
        json.dumps({"transport": "stdio", "env": {"TOKEN": "x"}}),
        encoding="utf-8",
    )
    payload = _read_secret_from_file(data_dir=tmp_path, ref="gh")
    assert isinstance(payload, dict)
    assert payload["transport"] == "stdio"


def test_delete_secret_and_oauth_cache_helpers_tolerate_invalid_ref(
    tmp_path: Path,
) -> None:
    _delete_secret_file(data_dir=tmp_path, ref="")
    _delete_secret_file(data_dir=tmp_path, ref=None)
    _delete_oauth_cache_dir(data_dir=tmp_path, ref="")
    _delete_oauth_cache_dir(data_dir=tmp_path, ref=None)


def test_secret_ref_is_still_referenced_matches_normalized(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: [
            {"secret_ref": 123},
            {"secret_ref": "shared.json"},
        ],
    )

    assert _secret_ref_is_still_referenced(data_dir=tmp_path, ref="shared")
    assert not _secret_ref_is_still_referenced(
        data_dir=tmp_path, ref="other"
    )


def test_list_upstreams_reads_enabled_and_secret_ref(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {
                    "command": "gh",
                    "_gateway": {"enabled": False, "secret_ref": "gh"},
                }
            }
        },
    )

    rows = list_upstreams(data_dir=tmp_path)
    assert len(rows) == 1
    assert rows[0]["name"] == "gh"
    assert rows[0]["transport"] == "stdio"
    assert rows[0]["enabled"] is False
    assert rows[0]["secret_ref"] == "gh"


def test_list_upstreams_sync_false_skips_registry_bootstrap(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(
            AssertionError("bootstrap should be skipped")
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(
            AssertionError("merge should be skipped")
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: [],
    )

    assert list_upstreams(data_dir=tmp_path, sync=False) == []


def test_inspect_upstream_reports_secret_metadata(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {
                    "command": "gh",
                    "_gateway": {"secret_ref": "gh"},
                }
            }
        },
    )
    write_secret(
        tmp_path,
        "gh",
        transport="stdio",
        env={"GITHUB_TOKEN": "abc"},
    )

    item = inspect_upstream(server="gh", data_dir=tmp_path)
    secret = item["secret"]
    assert isinstance(secret, dict)
    assert secret["ref"] == "gh"
    assert secret["transport"] == "stdio"
    assert secret["env_keys"] == ["GITHUB_TOKEN"]


def test_inspect_upstream_sync_false_skips_registry_bootstrap(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(
            AssertionError("bootstrap should be skipped")
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(
            AssertionError("merge should be skipped")
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.get_registry_upstream_record",
        lambda **_kwargs: {
            "prefix": "gh",
            "transport": "stdio",
            "enabled": False,
            "command": "gh",
            "url": None,
            "args": [],
            "secret_ref": None,
            "pagination": None,
            "auto_paginate_max_pages": None,
            "auto_paginate_max_records": None,
            "auto_paginate_timeout_seconds": None,
            "passthrough_allowed": True,
            "semantic_salt_env_keys": [],
            "semantic_salt_headers": [],
            "inherit_parent_env": False,
            "external_user_id": None,
        },
    )

    item = inspect_upstream(server="gh", data_dir=tmp_path, sync=False)
    assert item["enabled"] is False
    assert item["gateway"]["enabled"] is False


def test_inspect_upstream_records_secret_read_errors(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.get_registry_upstream_record",
        lambda **_kwargs: {
            "prefix": "api",
            "transport": "http",
            "enabled": True,
            "command": None,
            "url": "https://example.com/mcp",
            "args": [],
            "secret_ref": "api",
            "pagination": None,
            "auto_paginate_max_pages": None,
            "auto_paginate_max_records": None,
            "auto_paginate_timeout_seconds": None,
            "passthrough_allowed": True,
            "semantic_salt_env_keys": [],
            "semantic_salt_headers": [],
            "inherit_parent_env": False,
            "external_user_id": None,
        },
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.read_secret",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("secret read failed")
        ),
    )

    item = inspect_upstream(server="api", data_dir=tmp_path, sync=False)
    secret = item["secret"]
    assert isinstance(secret, dict)
    assert secret["ref"] == "api"
    assert "secret read failed" in str(secret["error"])


def test_inspect_upstream_reports_oauth_secret_metadata(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "api": {
                    "url": "https://example.com/mcp",
                    "_gateway": {"secret_ref": "api"},
                }
            }
        },
    )
    write_secret(
        tmp_path,
        "api",
        transport="http",
        headers={"Authorization": "Bearer tok"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    item = inspect_upstream(server="api", data_dir=tmp_path)
    secret = item["secret"]
    assert isinstance(secret, dict)
    assert secret["ref"] == "api"
    oauth = secret.get("oauth")
    assert isinstance(oauth, dict)
    assert oauth["enabled"] is True
    assert oauth["provider"] == "fastmcp"
    assert oauth["token_storage"] == "disk"


def test_remove_upstream_deletes_secret_file(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {
                    "command": "gh",
                    "_gateway": {"secret_ref": "gh"},
                }
            }
        },
    )
    write_secret(
        tmp_path,
        "gh",
        transport="stdio",
        env={"GITHUB_TOKEN": "abc"},
    )

    remove_upstream(server="gh", data_dir=tmp_path)

    config = _read_gateway_config(tmp_path)
    assert config["mcpServers"] == {}
    with pytest.raises(FileNotFoundError):
        read_secret(tmp_path, "gh")


def test_remove_upstream_deletes_oauth_cache_dir(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "api": {
                    "url": "https://example.com/mcp",
                    "_gateway": {"secret_ref": "api"},
                }
            }
        },
    )
    write_secret(
        tmp_path,
        "api",
        transport="http",
        headers={"Authorization": "Bearer tok"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )
    oauth_dir = oauth_cache_dir_path(tmp_path, "api")
    oauth_dir.mkdir(parents=True, exist_ok=True)
    (oauth_dir / "marker.txt").write_text("x", encoding="utf-8")

    remove_upstream(server="api", data_dir=tmp_path)

    assert not oauth_dir.exists()


def test_remove_upstream_dry_run_has_no_side_effects(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {"command": "gh", "env": {"GITHUB_TOKEN": "abc"}}
            }
        },
    )

    remove_upstream(server="gh", data_dir=tmp_path, dry_run=True)

    config = _read_gateway_config(tmp_path)
    assert config["mcpServers"]["gh"]["env"] == {"GITHUB_TOKEN": "abc"}
    assert not (tmp_path / "state" / "gateway.db").exists()
    assert not (tmp_path / "state" / "upstream_secrets" / "gh.json").exists()


def test_remove_upstream_dry_run_rejects_invalid_command(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": ""}}},
    )

    with pytest.raises(ValueError, match="command must be a non-empty string"):
        remove_upstream(server="gh", data_dir=tmp_path, dry_run=True)

    assert not (tmp_path / "state" / "gateway.db").exists()


def test_remove_upstream_dry_run_resolves_registry_without_mirror(
    tmp_path: Path,
) -> None:
    from sift_gateway.config.upstream_registry import (
        get_registry_upstream_record,
        replace_registry_from_mcp_servers,
    )

    _write_gateway_config(tmp_path, {"mcpServers": {}})
    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"gh": {"command": "gh"}},
        source_kind="manual",
    )
    # Simulate compatibility mirror drift: registry contains the upstream,
    # but config.json no longer has it.
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    result = remove_upstream(server="gh", data_dir=tmp_path, dry_run=True)

    assert result["removed"] == "gh"
    assert result["dry_run"] is True
    assert _read_gateway_config(tmp_path)["mcpServers"] == {}
    assert (
        get_registry_upstream_record(data_dir=tmp_path, prefix="gh") is not None
    )


def test_remove_upstream_preserves_shared_secret_file(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh_one": {
                    "command": "gh",
                    "_gateway": {"secret_ref": "shared"},
                },
                "gh_two": {
                    "command": "gh",
                    "_gateway": {"secret_ref": "shared"},
                },
            }
        },
    )
    write_secret(
        tmp_path,
        "shared",
        transport="stdio",
        env={"GITHUB_TOKEN": "abc"},
    )

    remove_upstream(server="gh_one", data_dir=tmp_path)

    config = _read_gateway_config(tmp_path)
    assert "gh_one" not in config["mcpServers"]
    assert config["mcpServers"]["gh_two"]["_gateway"]["secret_ref"] == "shared"
    secret = read_secret(tmp_path, "shared")
    assert secret["env"] == {"GITHUB_TOKEN": "abc"}


def test_set_upstream_enabled_roundtrip(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    set_upstream_enabled(server="gh", enabled=False, data_dir=tmp_path)
    after_disable = _read_gateway_config(tmp_path)
    assert after_disable["mcpServers"]["gh"]["_gateway"]["enabled"] is False

    set_upstream_enabled(server="gh", enabled=True, data_dir=tmp_path)
    after_enable = _read_gateway_config(tmp_path)
    assert "_gateway" not in after_enable["mcpServers"]["gh"]


def test_set_upstream_enabled_dry_run_has_no_side_effects(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    set_upstream_enabled(
        server="gh",
        enabled=False,
        data_dir=tmp_path,
        dry_run=True,
    )

    config = _read_gateway_config(tmp_path)
    assert "_gateway" not in config["mcpServers"]["gh"]
    assert not (tmp_path / "state" / "gateway.db").exists()


def test_set_upstream_enabled_dry_run_rejects_invalid_secret_ref(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {
                    "command": "gh",
                    "_gateway": {"secret_ref": 123},
                }
            }
        },
    )

    with pytest.raises(
        ValueError, match=r"_gateway\.secret_ref must be a non-empty string"
    ):
        set_upstream_enabled(
            server="gh",
            enabled=False,
            data_dir=tmp_path,
            dry_run=True,
        )

    assert not (tmp_path / "state" / "gateway.db").exists()


def test_set_upstream_auth_stdio_externalizes_env(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh", "env": {"OLD": "1"}}}},
    )

    set_upstream_auth(
        server="gh",
        env_updates={"TOKEN": "abc"},
        header_updates=None,
        data_dir=tmp_path,
    )

    config = _read_gateway_config(tmp_path)
    entry = config["mcpServers"]["gh"]
    assert "env" not in entry
    assert entry["_gateway"]["secret_ref"] == "gh"
    secret = read_secret(tmp_path, "gh")
    assert secret["env"] == {"OLD": "1", "TOKEN": "abc"}


def test_set_upstream_auth_http_externalizes_headers(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    set_upstream_auth(
        server="api",
        env_updates=None,
        header_updates={"Authorization": "Bearer tok"},
        data_dir=tmp_path,
    )

    config = _read_gateway_config(tmp_path)
    entry = config["mcpServers"]["api"]
    assert "headers" not in entry
    assert entry["_gateway"]["secret_ref"] == "api"
    secret = read_secret(tmp_path, "api")
    assert secret["headers"] == {"Authorization": "Bearer tok"}
    assert secret["oauth"] is None


def test_set_upstream_auth_http_clears_oauth_cache(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )
    oauth_dir = oauth_cache_dir_path(tmp_path, "api")
    oauth_dir.mkdir(parents=True, exist_ok=True)
    write_secret(
        tmp_path,
        "api",
        transport="http",
        headers={"Authorization": "Bearer old"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    set_upstream_auth(
        server="api",
        env_updates=None,
        header_updates={"Authorization": "Bearer tok"},
        data_dir=tmp_path,
    )

    secret = read_secret(tmp_path, "api")
    assert secret["headers"] == {"Authorization": "Bearer tok"}
    assert secret["oauth"] is None
    assert not oauth_dir.exists()


def test_set_upstream_auth_http_preserves_oauth_when_requested(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )
    oauth_dir = oauth_cache_dir_path(tmp_path, "api")
    oauth_dir.mkdir(parents=True, exist_ok=True)
    write_secret(
        tmp_path,
        "api",
        transport="http",
        headers={"Authorization": "Bearer old"},
        oauth={
            "enabled": True,
            "provider": "fastmcp",
            "token_storage": "disk",
        },
    )

    set_upstream_auth(
        server="api",
        env_updates=None,
        header_updates={"Authorization": "Bearer new"},
        clear_oauth=False,
        data_dir=tmp_path,
    )

    secret = read_secret(tmp_path, "api")
    assert secret["headers"] == {"Authorization": "Bearer new"}
    assert secret["oauth"] == {
        "enabled": True,
        "provider": "fastmcp",
        "token_storage": "disk",
    }
    assert oauth_dir.exists()


def test_set_upstream_auth_warns_and_continues_on_secret_read_error(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.read_secret",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = set_upstream_auth(
        server="gh",
        env_updates={"TOKEN": "abc"},
        header_updates=None,
        data_dir=tmp_path,
    )

    assert result["server"] == "gh"
    secret = read_secret(tmp_path, "gh")
    assert secret["env"] == {"TOKEN": "abc"}


def test_set_upstream_auth_dry_run_has_no_side_effects(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    set_upstream_auth(
        server="gh",
        env_updates={"TOKEN": "abc"},
        header_updates=None,
        data_dir=tmp_path,
        dry_run=True,
    )

    config = _read_gateway_config(tmp_path)
    assert "_gateway" not in config["mcpServers"]["gh"]
    assert not (tmp_path / "state" / "gateway.db").exists()
    assert not (tmp_path / "state" / "upstream_secrets").exists()


def test_set_upstream_auth_dry_run_rejects_invalid_url(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": ""}}},
    )

    with pytest.raises(ValueError, match="url must be a non-empty string"):
        set_upstream_auth(
            server="api",
            env_updates=None,
            header_updates={"Authorization": "Bearer tok"},
            data_dir=tmp_path,
            dry_run=True,
        )

    assert not (tmp_path / "state" / "gateway.db").exists()
    assert not (tmp_path / "state" / "upstream_secrets").exists()


def test_set_upstream_auth_rejects_transport_mismatch(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    with pytest.raises(ValueError, match="stdio upstreams"):
        set_upstream_auth(
            server="api",
            env_updates={"TOKEN": "abc"},
            header_updates=None,
            data_dir=tmp_path,
        )


def test_set_upstream_auth_rejects_header_updates_for_stdio(
    tmp_path: Path,
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    with pytest.raises(ValueError, match="only supported for http"):
        set_upstream_auth(
            server="gh",
            env_updates=None,
            header_updates={"Authorization": "Bearer tok"},
            data_dir=tmp_path,
        )


def test_set_upstream_auth_not_found_raises(tmp_path: Path) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    with pytest.raises(ValueError, match="not found"):
        set_upstream_auth(
            server="missing",
            env_updates={"TOKEN": "abc"},
            header_updates=None,
            data_dir=tmp_path,
        )


def test_set_upstream_auth_requires_updates(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    with pytest.raises(ValueError, match="at least one of --env or --header"):
        set_upstream_auth(
            server="gh",
            env_updates=None,
            header_updates=None,
            data_dir=tmp_path,
        )


def test_resolve_oauth_callback_url_headless_follows_redirect_chain(
    monkeypatch,
) -> None:
    authorization_url = "https://auth.example.test/authorize?client_id=abc"
    callback_port = 45789
    callback_url = (
        f"http://localhost:{callback_port}/callback?code=tok&state=state_123"
    )
    responses = {
        authorization_url: SimpleNamespace(
            status_code=302,
            headers={"location": "/step-one"},
        ),
        "https://auth.example.test/step-one": SimpleNamespace(
            status_code=302,
            headers={"location": "https://idp.example.test/consent"},
        ),
        "https://idp.example.test/consent": SimpleNamespace(
            status_code=302,
            headers={"location": callback_url},
        ),
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def get(self, url: str, *, follow_redirects: bool):
            assert follow_redirects is False
            key = str(url)
            if key not in responses:
                msg = f"unexpected request URL in test: {key}"
                raise AssertionError(msg)
            return responses[key]

    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)

    resolved = asyncio.run(
        _resolve_oauth_callback_url_headless(
            authorization_url=authorization_url,
            callback_port=callback_port,
        )
    )

    assert resolved == callback_url


def test_resolve_oauth_callback_url_headless_rejects_interactive_login(
    monkeypatch,
) -> None:
    authorization_url = "https://auth.example.test/authorize?client_id=abc"
    responses = {
        authorization_url: SimpleNamespace(status_code=200, headers={}),
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def get(self, url: str, *, follow_redirects: bool):
            assert follow_redirects is False
            key = str(url)
            if key not in responses:
                msg = f"unexpected request URL in test: {key}"
                raise AssertionError(msg)
            return responses[key]

    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)

    with pytest.raises(
        RuntimeError,
        match="requires interactive browser login",
    ):
        asyncio.run(
            _resolve_oauth_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=45789,
            )
        )


def test_resolve_oauth_callback_url_headless_requires_location_header(
    monkeypatch,
) -> None:
    authorization_url = "https://auth.example.test/authorize?client_id=abc"
    responses = {
        authorization_url: SimpleNamespace(status_code=302, headers={}),
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def get(self, url: str, *, follow_redirects: bool):
            assert follow_redirects is False
            key = str(url)
            if key not in responses:
                msg = f"unexpected request URL in test: {key}"
                raise AssertionError(msg)
            return responses[key]

    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)

    with pytest.raises(RuntimeError, match="redirect missing location header"):
        asyncio.run(
            _resolve_oauth_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=45789,
            )
        )


def test_resolve_oauth_callback_url_headless_rejects_error_status(
    monkeypatch,
) -> None:
    authorization_url = "https://auth.example.test/authorize?client_id=abc"
    responses = {
        authorization_url: SimpleNamespace(status_code=403, headers={}),
    }

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def get(self, url: str, *, follow_redirects: bool):
            assert follow_redirects is False
            key = str(url)
            if key not in responses:
                msg = f"unexpected request URL in test: {key}"
                raise AssertionError(msg)
            return responses[key]

    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)

    with pytest.raises(RuntimeError, match="OAuth authorization failed: 403"):
        asyncio.run(
            _resolve_oauth_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=45789,
            )
        )


def test_resolve_oauth_callback_url_headless_rejects_too_many_redirects(
    monkeypatch,
) -> None:
    authorization_url = "https://auth.example.test/authorize?client_id=abc"

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def get(self, url: str, *, follow_redirects: bool):
            _ = url
            assert follow_redirects is False
            return SimpleNamespace(status_code=302, headers={"location": "/loop"})

    monkeypatch.setattr("httpx.AsyncClient", _FakeAsyncClient)

    with pytest.raises(RuntimeError, match="too many redirects"):
        asyncio.run(
            _resolve_oauth_callback_url_headless(
                authorization_url=authorization_url,
                callback_port=45789,
            )
        )


def _patch_oauth_runtime(
    monkeypatch,
    *,
    callback_runner,
    inferred_transport: str = "streamable-http",
) -> None:
    class _FakeOAuthBase:
        def __init__(self, _mcp_url: str, *, token_storage=None) -> None:
            self.redirect_port = 45789
            self.token_storage = token_storage
            self.context = SimpleNamespace(
                current_tokens=SimpleNamespace(access_token=None)
            )

    class _FakeTransport:
        def __init__(self, *, url: str, auth, headers=None) -> None:
            self.url = url
            self.auth = auth
            self.headers = headers

    class _FakeClient:
        def __init__(self, transport, timeout: float = 30.0) -> None:
            _ = timeout
            self.transport = transport

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def list_tools(self) -> list[object]:
            await callback_runner(self.transport.auth)
            return []

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuthBase)
    monkeypatch.setattr("fastmcp.client.transports.SSETransport", _FakeTransport)
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: inferred_transport,
    )
    monkeypatch.setattr("fastmcp.Client", _FakeClient)


def test_oauth_login_access_token_headless_errors_when_redirect_never_starts(
    monkeypatch,
) -> None:
    async def _runner(auth) -> None:
        await auth.callback_handler()

    _patch_oauth_runtime(monkeypatch, callback_runner=_runner)

    with pytest.raises(RuntimeError, match="OAuth redirect did not start"):
        asyncio.run(
            _oauth_login_access_token(
                url="https://example.com/mcp",
                headless=True,
            )
        )


def test_oauth_login_access_token_headless_rejects_error_callback(
    monkeypatch,
) -> None:
    async def _runner(auth) -> None:
        auth._callback_url = (
            "http://localhost:45789/callback?"
            "error=access_denied&error_description=Denied"
        )
        await auth.callback_handler()

    _patch_oauth_runtime(monkeypatch, callback_runner=_runner)

    with pytest.raises(RuntimeError, match="access_denied - Denied"):
        asyncio.run(
            _oauth_login_access_token(
                url="https://example.com/mcp",
                headless=True,
            )
        )


def test_oauth_login_access_token_headless_requires_code_param(
    monkeypatch,
) -> None:
    async def _runner(auth) -> None:
        auth._callback_url = "http://localhost:45789/callback?state=abc"
        await auth.callback_handler()

    _patch_oauth_runtime(monkeypatch, callback_runner=_runner)

    with pytest.raises(RuntimeError, match="missing authorization code"):
        asyncio.run(
            _oauth_login_access_token(
                url="https://example.com/mcp",
                headless=True,
            )
        )


def test_oauth_login_access_token_headless_succeeds_without_state(
    monkeypatch,
) -> None:
    callback_url = "http://localhost:45789/callback?code=headless_code"

    async def _runner(auth) -> None:
        await auth.redirect_handler("https://auth.example.test/start")
        code, state = await auth.callback_handler()
        assert code == "headless_code"
        assert state is None
        auth.context.current_tokens.access_token = "tok_headless"

    _patch_oauth_runtime(monkeypatch, callback_runner=_runner)

    async def _fake_resolve(
        *, authorization_url: str, callback_port: int
    ) -> str:
        assert authorization_url == "https://auth.example.test/start"
        assert callback_port == 45789
        return callback_url

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._resolve_oauth_callback_url_headless",
        _fake_resolve,
    )

    token = asyncio.run(
        _oauth_login_access_token(
            url="https://example.com/mcp",
            headless=True,
        )
    )
    assert token == "tok_headless"


def test_oauth_login_access_token_uses_sse_transport_when_inferred(
    monkeypatch,
) -> None:
    seen: dict[str, object] = {}

    async def _runner(auth) -> None:
        _ = auth
        seen["called"] = True

    _patch_oauth_runtime(
        monkeypatch,
        callback_runner=_runner,
        inferred_transport="sse",
    )

    class _OAuthWithToken:
        def __init__(self, _mcp_url: str, *, token_storage=None) -> None:
            _ = token_storage
            self.context = SimpleNamespace(
                current_tokens=SimpleNamespace(access_token="tok_sse")
            )

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _OAuthWithToken)

    token = asyncio.run(
        _oauth_login_access_token(url="https://example.com/sse")
    )
    assert token == "tok_sse"
    assert seen["called"] is True


def test_oauth_login_access_token_rejects_missing_access_token(
    monkeypatch,
) -> None:
    class _FakeOAuth:
        def __init__(self, *_args, **_kwargs) -> None:
            self.context = SimpleNamespace(
                current_tokens=SimpleNamespace(access_token=None)
            )

    class _FakeTransport:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    class _FakeClient:
        def __init__(self, _transport, timeout: float = 30.0) -> None:
            _ = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc, _tb) -> bool:
            return False

        async def list_tools(self) -> list[object]:
            return []

    monkeypatch.setattr("fastmcp.client.auth.OAuth", _FakeOAuth)
    monkeypatch.setattr("fastmcp.client.transports.SSETransport", _FakeTransport)
    monkeypatch.setattr(
        "fastmcp.client.transports.StreamableHttpTransport", _FakeTransport
    )
    monkeypatch.setattr(
        "fastmcp.mcp_config.infer_transport_type_from_url",
        lambda _url: "streamable-http",
    )
    monkeypatch.setattr("fastmcp.Client", _FakeClient)

    with pytest.raises(RuntimeError, match="no access token"):
        asyncio.run(
            _oauth_login_access_token(url="https://example.com/mcp")
        )


def test_login_upstream_http_persists_authorization_header(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    async def _fake_oauth(
        *,
        url: str,
        headless: bool = False,
        token_storage: object | None = None,
    ) -> str:
        assert url == "https://example.com/mcp"
        assert headless is False
        assert token_storage is not None
        return "tok_123"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth,
    )

    result = login_upstream(server="api", data_dir=tmp_path)

    assert result["server"] == "api"
    assert result["login"] == "oauth"
    assert result["updated_header_keys"] == ["Authorization"]
    assert result["oauth_enabled"] is True
    secret = read_secret(tmp_path, "api")
    assert secret["headers"] == {"Authorization": "Bearer tok_123"}
    assert secret["oauth"] == {
        "enabled": True,
        "provider": "fastmcp",
        "token_storage": "disk",
    }
    assert oauth_cache_dir_path(tmp_path, "api").exists()


def test_login_upstream_rejects_non_http_transport(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    with pytest.raises(ValueError, match="only supported for http"):
        login_upstream(server="gh", data_dir=tmp_path)


def test_login_upstream_not_found_raises(tmp_path: Path) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    with pytest.raises(ValueError, match="not found"):
        login_upstream(server="missing", data_dir=tmp_path)


def test_login_upstream_requires_http_url(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._resolve_mutation_record",
        lambda **_kwargs: {
            "prefix": "api",
            "transport": "http",
            "url": "",
            "secret_ref": None,
        },
    )

    with pytest.raises(ValueError, match="has no HTTP url configured"):
        login_upstream(server="api", data_dir=tmp_path)


def test_login_upstream_normalizes_secret_ref_suffix(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._resolve_mutation_record",
        lambda **_kwargs: {
            "prefix": "api",
            "transport": "http",
            "url": "https://example.com/mcp",
            "secret_ref": "shared.json",
        },
    )

    async def _fake_oauth(
        *,
        url: str,
        headless: bool = False,
        token_storage: object | None = None,
    ) -> str:
        _ = headless
        assert url == "https://example.com/mcp"
        assert token_storage is not None
        return "tok_shared"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth,
    )

    result = login_upstream(server="api", data_dir=tmp_path)
    assert result["secret_ref"] == "shared"
    secret = read_secret(tmp_path, "shared")
    assert secret["headers"]["Authorization"] == "Bearer tok_shared"


def test_login_upstream_dry_run_skips_oauth(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    async def _fail_oauth(
        *,
        url: str,
        headless: bool = False,
        token_storage: object | None = None,
    ) -> str:
        raise AssertionError(
            "oauth helper should not run in dry-run: "
            f"{url} {headless} {token_storage}"
        )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fail_oauth,
    )

    result = login_upstream(server="api", data_dir=tmp_path, dry_run=True)
    assert result["server"] == "api"
    assert result["dry_run"] is True
    assert result["updated_header_keys"] == ["Authorization"]
    assert result["oauth_enabled"] is True
    assert result["login"] == "oauth"
    assert not (tmp_path / "state" / "upstream_secrets").exists()


def test_login_upstream_headless_passes_flag(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )
    seen: dict[str, object] = {}

    async def _fake_oauth(
        *,
        url: str,
        headless: bool = False,
        token_storage: object | None = None,
    ) -> str:
        seen["url"] = url
        seen["headless"] = headless
        seen["has_token_storage"] = token_storage is not None
        return "tok_456"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth,
    )

    login_upstream(server="api", data_dir=tmp_path, headless=True)

    assert seen["url"] == "https://example.com/mcp"
    assert seen["headless"] is True
    assert seen["has_token_storage"] is True


def test_inspect_upstream_not_found_raises(tmp_path: Path) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    with pytest.raises(ValueError, match="not found"):
        inspect_upstream(server="missing", data_dir=tmp_path)


def test_remove_upstream_not_found_raises(tmp_path: Path) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    with pytest.raises(ValueError, match="not found"):
        remove_upstream(server="missing", data_dir=tmp_path)


def test_set_upstream_enabled_not_found_raises(tmp_path: Path) -> None:
    _write_gateway_config(tmp_path, {"mcpServers": {}})

    with pytest.raises(ValueError, match="not found"):
        set_upstream_enabled(
            server="missing",
            enabled=True,
            data_dir=tmp_path,
        )


def test_probe_upstreams_requires_server_or_all() -> None:
    with pytest.raises(
        ValueError, match="one of --server or --all is required"
    ):
        probe_upstreams(server=None, all_servers=False)


def test_probe_upstreams_rejects_server_and_all() -> None:
    with pytest.raises(
        ValueError, match="--server and --all are mutually exclusive"
    ):
        probe_upstreams(server="gh", all_servers=True)


def test_probe_upstreams_reports_not_found(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.load_gateway_config",
        lambda **_kwargs: SimpleNamespace(upstreams=[]),
    )

    with pytest.raises(ValueError, match="not found"):
        probe_upstreams(server="missing", data_dir=tmp_path)


def test_test_upstreams_reports_disabled_server(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "gh": {"command": "gh", "_gateway": {"enabled": False}}
            }
        },
    )

    monkeypatch.setattr(
        "sift_gateway.config.load_gateway_config",
        lambda **_kwargs: SimpleNamespace(upstreams=[]),
    )

    with pytest.raises(ValueError, match="is disabled"):
        probe_upstreams(server="gh", data_dir=tmp_path)


def test_test_upstreams_uses_probe_results(tmp_path: Path, monkeypatch) -> None:
    upstream = SimpleNamespace(prefix="gh")

    monkeypatch.setattr(
        "sift_gateway.config.load_gateway_config",
        lambda **_kwargs: SimpleNamespace(upstreams=[upstream]),
    )

    async def _fake_probe_upstream_configs(*, upstreams, data_dir):
        assert upstreams == [upstream]
        assert data_dir == tmp_path
        return [{"name": "gh", "ok": True, "tool_count": 3}]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._probe_upstream_configs",
        _fake_probe_upstream_configs,
    )

    report = probe_upstreams(server="gh", data_dir=tmp_path)
    assert report["ok"] is True
    assert report["ok_count"] == 1
    assert report["total"] == 1


def test_probe_upstreams_all_scope_passes_all_active(
    tmp_path: Path, monkeypatch
) -> None:
    upstreams = [SimpleNamespace(prefix="gh"), SimpleNamespace(prefix="api")]
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_gateway.config.load_gateway_config",
        lambda **_kwargs: SimpleNamespace(upstreams=upstreams),
    )

    async def _fake_probe_upstream_configs(*, upstreams, data_dir):
        seen["upstreams"] = upstreams
        seen["data_dir"] = data_dir
        return [
            {"name": "gh", "ok": True, "tool_count": 2},
            {"name": "api", "ok": True, "tool_count": 1},
        ]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._probe_upstream_configs",
        _fake_probe_upstream_configs,
    )

    report = probe_upstreams(all_servers=True, data_dir=tmp_path)
    assert report["ok"] is True
    assert report["ok_count"] == 2
    assert report["total"] == 2
    assert seen["upstreams"] == upstreams
    assert seen["data_dir"] == tmp_path


def test_normalize_input_servers_non_strict_wrapped_invalid_returns_empty(
) -> None:
    assert normalize_input_servers({"mcpServers": []}, strict=False) == {}


def test_normalize_input_servers_non_strict_wrapped_filters_non_dict(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.extract_mcp_servers",
        lambda _raw: {"ok": {"command": "echo"}, "skip": "x"},
    )

    normalized = normalize_input_servers({"mcpServers": {}}, strict=False)
    assert normalized == {"ok": {"command": "echo"}}


def test_normalize_input_servers_strict_wrapped_uses_extracted_map(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.extract_mcp_servers",
        lambda _raw: {"x": {"command": "echo"}, "y": "keep-as-is"},
    )

    normalized = normalize_input_servers({"mcpServers": {}}, strict=True)
    assert normalized == {"x": {"command": "echo"}, "y": "keep-as-is"}


def test_normalize_input_servers_strict_unwrapped_returns_input(
) -> None:
    raw = {"x": {"command": "echo"}}
    assert normalize_input_servers(raw, strict=True) is raw


def test_normalize_input_servers_non_strict_unwrapped_filters_invalid(
) -> None:
    normalized = normalize_input_servers(
        {
            "good": {"command": "echo"},
            "bad-value": "nope",
            42: {"command": "ignored"},
        },
        strict=False,
    )
    assert normalized == {"good": {"command": "echo"}}


def test_reconcile_after_add_noop_when_added_names_empty(tmp_path: Path) -> None:
    warnings: list[str] = []
    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={"mcpServers": {"x": {"command": "echo"}}},
        added_names=set(),
        warnings=warnings,
    )
    assert warnings == []


def test_reconcile_after_add_warns_when_bootstrap_fails_without_snapshot(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(ValueError("invalid mirror")),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: [],
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={"new": {"command": "echo"}},
        added_names={"new"},
        warnings=warnings,
    )

    assert len(warnings) == 2
    assert "invalid mcpServers mirror" in warnings[0]
    assert "bootstrap did not establish a canonical snapshot" in warnings[1]


def test_reconcile_after_add_warns_when_snapshot_load_errors(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(RuntimeError("sync boom")),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("load boom")
        ),
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={"new": {"command": "echo"}},
        added_names={"new"},
        warnings=warnings,
    )

    assert len(warnings) == 2
    assert "runtime error: sync boom" in warnings[0]
    assert "snapshot could not be loaded: load boom" in warnings[1]


def test_reconcile_after_add_falls_back_to_source_and_warns_on_upsert_error(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_gateway_config_dict",
        lambda _config_path: {"mcpServers": {}},
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("upsert boom")),
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={"new": {"command": "echo"}},
        added_names={"new"},
        warnings=warnings,
    )

    assert len(warnings) == 1
    assert "registry reconciliation failed: upsert boom" in warnings[0]


def test_reconcile_after_add_fallback_upsert_success_mirrors_registry(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    seen: dict[str, object] = {}
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    # Invalid wrapped shape forces fallback to raw_input source servers.
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_gateway_config_dict",
        lambda _config_path: {"mcpServers": []},
    )

    def _capture_upsert(**kwargs) -> None:
        seen["servers"] = kwargs["servers"]
        seen["source_kind"] = kwargs["source_kind"]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        _capture_upsert,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.mirror_registry_to_config",
        lambda _data_dir: seen.setdefault("mirrored", True),
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={"new": {"command": "echo"}},
        added_names={"new"},
        warnings=warnings,
    )

    assert warnings == []
    assert seen["servers"] == {"new": {"command": "echo"}}
    assert seen["source_kind"] == "snippet_add"
    assert seen["mirrored"] is True


def test_reconcile_after_add_prefers_config_added_servers(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    seen: dict[str, object] = {}
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_gateway_config_dict",
        lambda _config_path: {"mcpServers": {"new": {"command": "echo"}}},
    )

    def _capture_upsert(**kwargs) -> None:
        seen["servers"] = kwargs["servers"]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        _capture_upsert,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.mirror_registry_to_config",
        lambda _data_dir: None,
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={},
        added_names={"new"},
        warnings=warnings,
    )

    assert warnings == []
    assert seen["servers"] == {"new": {"command": "echo"}}


def test_reconcile_after_add_skips_upsert_when_no_added_servers_found(
    tmp_path: Path, monkeypatch
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_gateway_config_dict",
        lambda _config_path: {"mcpServers": {}},
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("upsert should be skipped")
        ),
    )

    reconcile_after_add(
        data_dir=tmp_path,
        raw_input={},
        added_names={"missing"},
        warnings=warnings,
    )
    assert warnings == []


def test_probe_one_upstream_error_payload(tmp_path: Path, monkeypatch) -> None:
    upstream = SimpleNamespace(prefix="api")
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.discover_tools",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("noauth")),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.classify_upstream_exception",
        lambda _exc: "auth_error",
    )

    result = asyncio.run(_probe_one_upstream(upstream, tmp_path))
    assert result == {
        "name": "api",
        "ok": False,
        "error_code": "auth_error",
        "error": "noauth",
    }


def test_probe_one_upstream_success_payload(tmp_path: Path, monkeypatch) -> None:
    upstream = SimpleNamespace(prefix="api")

    async def _fake_discover_tools(*_args, **_kwargs):
        return ["a", "b"]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.discover_tools",
        _fake_discover_tools,
    )

    result = asyncio.run(_probe_one_upstream(upstream, tmp_path))
    assert result == {"name": "api", "ok": True, "tool_count": 2}


def test_probe_upstream_configs_runs_for_each_upstream(
    tmp_path: Path, monkeypatch
) -> None:
    upstreams = [SimpleNamespace(prefix="a"), SimpleNamespace(prefix="b")]

    async def _fake_probe_one(upstream, _data_dir):
        return {"name": upstream.prefix, "ok": True, "tool_count": 1}

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._probe_one_upstream",
        _fake_probe_one,
    )

    result = asyncio.run(
        _probe_upstream_configs(upstreams=upstreams, data_dir=tmp_path)
    )
    assert result == [
        {"name": "a", "ok": True, "tool_count": 1},
        {"name": "b", "ok": True, "tool_count": 1},
    ]
