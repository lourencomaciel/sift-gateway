"""Tests for upstream admin helpers."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sift_gateway.config.upstream_admin import (
    _resolve_oauth_callback_url_headless,
    inspect_upstream,
    list_upstreams,
    login_upstream,
    parse_kv_pairs,
    probe_upstreams,
    remove_upstream,
    set_upstream_auth,
    set_upstream_enabled,
)
from sift_gateway.config.upstream_secrets import read_secret, write_secret


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


def test_login_upstream_http_persists_authorization_header(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    async def _fake_oauth(*, url: str, headless: bool = False) -> str:
        assert url == "https://example.com/mcp"
        assert headless is False
        return "tok_123"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth,
    )

    result = login_upstream(server="api", data_dir=tmp_path)

    assert result["server"] == "api"
    assert result["login"] == "oauth"
    assert result["updated_header_keys"] == ["Authorization"]
    secret = read_secret(tmp_path, "api")
    assert secret["headers"] == {"Authorization": "Bearer tok_123"}


def test_login_upstream_rejects_non_http_transport(tmp_path: Path) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"gh": {"command": "gh"}}},
    )

    with pytest.raises(ValueError, match="only supported for http"):
        login_upstream(server="gh", data_dir=tmp_path)


def test_login_upstream_dry_run_skips_oauth(
    tmp_path: Path, monkeypatch
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    async def _fail_oauth(*, url: str, headless: bool = False) -> str:
        raise AssertionError(
            f"oauth helper should not run in dry-run: {url} {headless}"
        )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fail_oauth,
    )

    result = login_upstream(server="api", data_dir=tmp_path, dry_run=True)
    assert result["server"] == "api"
    assert result["dry_run"] is True
    assert result["updated_header_keys"] == ["Authorization"]
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

    async def _fake_oauth(*, url: str, headless: bool = False) -> str:
        seen["url"] = url
        seen["headless"] = headless
        return "tok_456"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth,
    )

    login_upstream(server="api", data_dir=tmp_path, headless=True)

    assert seen["url"] == "https://example.com/mcp"
    assert seen["headless"] is True


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
