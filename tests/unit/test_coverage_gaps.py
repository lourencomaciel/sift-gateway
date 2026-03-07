from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from sift_gateway.config.settings import UpstreamConfig
from sift_gateway.config.upstream_admin import (
    _collapse_unshared_secret_ref_oauth_state,
    _delete_oauth_server_auth_config,
    _delete_one_upstream_oauth_state,
    _oauth_config_provider,
    _oauth_config_scope,
    _oauth_inspect_metadata,
    _oauth_login_access_token,
    login_upstream,
)
from sift_gateway.config.upstream_admin import (
    _secret_ref_is_shared as admin_secret_ref_is_shared,
)
from sift_gateway.config.upstream_secrets import (
    effective_oauth_server_auth_config,
    oauth_server_auth_config_path,
    read_oauth_server_auth_config,
    write_oauth_server_auth_config,
    write_secret,
)
from sift_gateway.mcp.upstream import (
    UpstreamInstance,
    _effective_auth_config,
    _google_adc_authorized_headers,
    _secret_auth_config,
    _secret_auth_mode,
    _secret_oauth_enabled,
    call_upstream_tool,
    compute_auth_fingerprint,
)
from sift_gateway.mcp.upstream import (
    _secret_ref_is_shared as runtime_secret_ref_is_shared,
)


def _write_gateway_config(data_dir: Path, payload: dict[str, object]) -> None:
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )


def test_upstream_secret_ref_is_shared_fallbacks(
    monkeypatch, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "api_a": {
                    "url": "https://a.example.com/mcp",
                    "_gateway": {"secret_ref": "shared"},
                },
                "api_b": {
                    "url": "https://b.example.com/mcp",
                    "_gateway": {"secret_ref": "shared"},
                },
            }
        },
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: [],
    )
    assert (
        admin_secret_ref_is_shared(
            data_dir=tmp_path,
            ref="shared",
            server="api_a",
        )
        is True
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.extract_mcp_servers",
        lambda _raw: (_ for _ in ()).throw(ValueError("broken")),
    )
    assert (
        admin_secret_ref_is_shared(
            data_dir=tmp_path,
            ref="shared",
            server="api_a",
        )
        is False
    )


def test_delete_oauth_server_auth_config_and_state_guards(
    monkeypatch, caplog
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._delete_oauth_server_auth_config_impl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not be called")
        ),
    )
    _delete_oauth_server_auth_config(
        data_dir=Path("/tmp"),
        ref=None,
        server_url=None,
    )

    _delete_one_upstream_oauth_state(
        data_dir=Path("/tmp"),
        ref=None,
        server_url=None,
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._delete_oauth_server_auth_config_impl",
        lambda *_args, **_kwargs: None,
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.clear_oauth_session",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.oauth_token_storage",
        lambda *_args, **_kwargs: object(),
    )
    with caplog.at_level(logging.DEBUG):
        _delete_one_upstream_oauth_state(
            data_dir=Path("/tmp"),
            ref="shared",
            server_url="https://example.com/mcp",
        )
    assert "skipped oauth session cleanup" in caplog.text


def test_oauth_metadata_and_provider_helpers() -> None:
    oauth = {
        "enabled": True,
        "mode": "oauth",
        "token_storage": "disk",
        "token_endpoint_auth_method": "client_secret_basic",
        "client_metadata_url": "https://client.example/meta",
    }
    metadata = _oauth_inspect_metadata(oauth)
    assert metadata is not None
    assert metadata["token_endpoint_auth_method"] == "client_secret_basic"
    assert metadata["client_metadata_url"] == "https://client.example/meta"
    assert _oauth_config_provider({"enabled": True, "provider": "fastmcp"}) == (
        "oauth"
    )
    assert (
        _oauth_config_scope({"enabled": True, "scope": "scope.a"}) == "scope.a"
    )


def test_collapse_unshared_secret_ref_oauth_state_early_returns_and_logs(
    monkeypatch, caplog, tmp_path: Path
) -> None:
    _collapse_unshared_secret_ref_oauth_state(data_dir=tmp_path, ref=None)

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._records_for_secret_ref",
        lambda **_kwargs: [
            {"transport": "http", "url": "https://a.example.com/mcp"},
            {"transport": "http", "url": "https://b.example.com/mcp"},
        ],
    )
    _collapse_unshared_secret_ref_oauth_state(data_dir=tmp_path, ref="shared")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._records_for_secret_ref",
        lambda **_kwargs: [{"transport": "http", "url": None}],
    )
    _collapse_unshared_secret_ref_oauth_state(data_dir=tmp_path, ref="shared")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._records_for_secret_ref",
        lambda **_kwargs: [
            {"transport": "http", "url": "https://a.example.com/mcp"}
        ],
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.read_secret",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("broken")),
    )
    with caplog.at_level(logging.DEBUG):
        _collapse_unshared_secret_ref_oauth_state(
            data_dir=tmp_path, ref="shared"
        )
    assert "skipped oauth sidecar collapse" in caplog.text


@pytest.mark.asyncio
async def test_upstream_admin_oauth_login_access_token_typeerror_handling(
    monkeypatch,
) -> None:
    async def _raise_other_type_error(**_kwargs):
        raise TypeError("different failure")

    async def _raise_token_storage_type_error(**_kwargs):
        raise TypeError("token_storage unsupported")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token_impl",
        _raise_other_type_error,
    )
    with pytest.raises(TypeError, match="different failure"):
        await _oauth_login_access_token(url="https://example.com/mcp")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token_impl",
        _raise_token_storage_type_error,
    )
    with pytest.raises(RuntimeError, match="configurable token storage"):
        await _oauth_login_access_token(url="https://example.com/mcp")


def test_login_upstream_validation_and_warning_paths(
    monkeypatch, caplog, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )

    def _fake_set_upstream_auth(**_kwargs):
        return {"server": "api", "transport": "http", "secret_ref": "api"}

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.set_upstream_auth",
        _fake_set_upstream_auth,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._read_secret_from_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    with caplog.at_level(logging.WARNING):
        result = login_upstream(server="api", data_dir=tmp_path, dry_run=True)
    assert result["login"] == "oauth"
    assert "failed to read existing secret" in caplog.text

    with pytest.raises(ValueError, match="must be one of"):
        login_upstream(
            server="api",
            data_dir=tmp_path,
            dry_run=True,
            oauth_registration="invalid",
        )
    with pytest.raises(ValueError, match="cannot be combined"):
        login_upstream(
            server="api",
            data_dir=tmp_path,
            dry_run=True,
            oauth_registration="dynamic",
            oauth_client_id="client-123",
        )
    with pytest.raises(ValueError, match="requires --oauth-client-id"):
        login_upstream(
            server="api",
            data_dir=tmp_path,
            dry_run=True,
            oauth_registration="preregistered",
        )


def test_login_upstream_auth_method_none_drops_client_secret_and_missing_callback(
    monkeypatch, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )
    write_secret(
        tmp_path,
        "api",
        transport="http",
        oauth={
            "enabled": True,
            "mode": "oauth",
            "registration": "preregistered",
            "token_storage": "disk",
            "client_id": "client-123",
            "client_secret": "secret-456",
            "callback_port": 46000,
        },
    )

    seen: dict[str, object] = {}

    def _fake_set_upstream_auth(**kwargs):
        seen.update(kwargs)
        return {
            "server": "api",
            "transport": "http",
            "secret_ref": "api",
            "oauth_enabled": True,
        }

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.set_upstream_auth",
        _fake_set_upstream_auth,
    )
    login_upstream(
        server="api",
        data_dir=tmp_path,
        dry_run=True,
        oauth_auth_method="none",
    )
    assert seen["oauth"]["token_endpoint_auth_method"] == "none"
    assert "client_secret" not in seen["oauth"]

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_config_callback_port",
        lambda _oauth: None,
    )
    with pytest.raises(RuntimeError, match="callback port is required"):
        login_upstream(
            server="api",
            data_dir=tmp_path,
            dry_run=True,
        )


def test_login_upstream_logs_debug_when_client_registration_clear_fails(
    monkeypatch, caplog, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {"mcpServers": {"api": {"url": "https://example.com/mcp"}}},
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.clear_oauth_client_registration",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("cannot clear")),
    )

    async def _fake_oauth_login_access_token(**_kwargs) -> str:
        return "tok_123"

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin._oauth_login_access_token",
        _fake_oauth_login_access_token,
    )
    with caplog.at_level(logging.DEBUG):
        login_upstream(server="api", data_dir=tmp_path)
    assert "skipped oauth client registration reset" in caplog.text


def test_upstream_secrets_invalid_sidecar_paths_and_write_cleanup(
    monkeypatch, tmp_path: Path
) -> None:
    original_fchmod = os.fchmod
    path = oauth_server_auth_config_path(
        tmp_path,
        "shared",
        "https://example.com/mcp",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{invalid", encoding="utf-8")
    assert (
        read_oauth_server_auth_config(
            tmp_path,
            "shared",
            server_url="https://example.com/mcp",
        )
        is None
    )

    path.write_text("[]", encoding="utf-8")
    assert (
        read_oauth_server_auth_config(
            tmp_path,
            "shared",
            server_url="https://example.com/mcp",
        )
        is None
    )

    original_read_text = Path.read_text
    monkeypatch.setattr(
        "pathlib.Path.read_text",
        lambda self, *args, **kwargs: (
            (_ for _ in ()).throw(OSError("boom"))
            if self == path
            else original_read_text(self, *args, **kwargs)
        ),
    )
    assert (
        read_oauth_server_auth_config(
            tmp_path,
            "shared",
            server_url="https://example.com/mcp",
        )
        is None
    )

    no_op_path = oauth_server_auth_config_path(
        tmp_path,
        "shared",
        "https://example.com/no-op",
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_secrets.normalize_auth_config",
        lambda _oauth: None,
    )
    write_oauth_server_auth_config(
        tmp_path,
        "shared",
        server_url="https://example.com/no-op",
        oauth_config={"enabled": True},
    )
    assert not no_op_path.exists()

    monkeypatch.setattr(
        "os.fchmod",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("fchmod boom")),
    )
    with pytest.raises(OSError, match="fchmod boom"):
        write_oauth_server_auth_config(
            tmp_path,
            "shared",
            server_url="https://example.com/close-fd",
            oauth_config={"enabled": False},
        )

    monkeypatch.setattr(
        "os.fchmod",
        original_fchmod,
    )
    monkeypatch.setattr(
        "os.replace",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("boom")),
    )
    with pytest.raises(OSError, match="boom"):
        write_oauth_server_auth_config(
            tmp_path,
            "shared",
            server_url="https://example.com/mcp",
            oauth_config={"enabled": False},
        )
    assert not any(path.parent.glob("*.tmp"))


def test_effective_oauth_server_auth_config_returns_normalized_without_server_url(
    tmp_path: Path,
) -> None:
    assert effective_oauth_server_auth_config(
        tmp_path,
        "shared",
        server_url=None,
        oauth_config={"enabled": True, "provider": "fastmcp"},
    ) == {
        "enabled": True,
        "mode": "oauth",
        "registration": "dynamic",
    }


@pytest.mark.asyncio
async def test_upstream_runtime_helper_branches(
    monkeypatch, tmp_path: Path
) -> None:
    assert _secret_auth_mode(None) is None
    assert _secret_auth_config(None) is None
    assert (
        _secret_oauth_enabled(
            {"oauth": {"enabled": True, "provider": "fastmcp"}}
        )
        is True
    )
    assert _secret_auth_mode(
        {"oauth": {"enabled": True, "provider": "fastmcp"}}
    ) == ("oauth")

    async def _fake_google_adc_headers(**kwargs):
        return {"Authorization": "Bearer tok", "seen": str(bool(kwargs))}

    monkeypatch.setattr(
        "sift_gateway.auth.google_adc.google_adc_authorized_headers",
        _fake_google_adc_headers,
    )
    headers = await _google_adc_authorized_headers(
        method="GET",
        url="https://example.com/mcp",
    )
    assert headers["Authorization"] == "Bearer tok"

    assert (
        runtime_secret_ref_is_shared(
            SimpleNamespace(transport="stdio", prefix="gh", secret_ref=None)
        )
        is False
    )
    assert (
        runtime_secret_ref_is_shared(
            SimpleNamespace(transport="http", prefix="", secret_ref="")
        )
        is False
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_registry.load_registry_upstream_records",
        lambda *_args, **_kwargs: [
            {"prefix": "api", "secret_ref": "shared"},
            {"prefix": 7, "secret_ref": "shared"},
            {"prefix": "other", "secret_ref": "shared"},
        ],
    )
    assert (
        runtime_secret_ref_is_shared(
            SimpleNamespace(transport="http", prefix="api", secret_ref="shared")
        )
        is True
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_registry.load_registry_upstream_records",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "sift_gateway.config.shared.load_gateway_config_dict",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "sift_gateway.config.mcp_servers.extract_mcp_servers",
        lambda _raw: (_ for _ in ()).throw(ValueError("broken")),
    )
    assert (
        runtime_secret_ref_is_shared(
            SimpleNamespace(
                transport="http", prefix="api", secret_ref="shared"
            ),
            str(tmp_path),
        )
        is False
    )


def test_effective_auth_config_uses_sidecar_and_runtime_errors(
    monkeypatch, tmp_path: Path
) -> None:
    _write_gateway_config(
        tmp_path,
        {
            "mcpServers": {
                "api": {
                    "url": "https://example.com/mcp",
                    "_gateway": {"secret_ref": "shared"},
                },
                "other": {
                    "url": "https://other.example.com/mcp",
                    "_gateway": {"secret_ref": "shared"},
                },
            }
        },
    )
    cfg = UpstreamConfig(
        prefix="api",
        transport="http",
        url="https://example.com/mcp",
        secret_ref="shared",
    )
    write_secret(
        tmp_path,
        "shared",
        transport="http",
        oauth={"enabled": True, "provider": "fastmcp"},
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream.auth_mode",
        lambda _auth: "unsupported-mode",
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._effective_auth_config",
        lambda *_args, **_kwargs: {"enabled": True, "mode": "unsupported-mode"},
    )
    from sift_gateway.mcp.upstream import _build_runtime_oauth_auth

    with pytest.raises(RuntimeError, match="Unsupported auth mode"):
        _build_runtime_oauth_auth(cfg, str(tmp_path))

    monkeypatch.setattr(
        "sift_gateway.mcp.upstream.auth_mode",
        lambda auth: "oauth" if auth else None,
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._effective_auth_config",
        _effective_auth_config,
    )
    assert _effective_auth_config(cfg, str(tmp_path)) == {
        "enabled": True,
        "mode": "oauth",
        "registration": "dynamic",
    }


def test_compute_auth_fingerprint_includes_scope_and_call_upstream_reraises(
    monkeypatch, tmp_path: Path
) -> None:
    cfg = UpstreamConfig(
        prefix="api",
        transport="http",
        url="https://example.com/mcp",
        secret_ref="api",
    )
    write_secret(
        tmp_path,
        "api",
        transport="http",
        headers={"Authorization": "Bearer tok"},
        oauth={
            "enabled": True,
            "mode": "oauth",
            "token_storage": "disk",
            "scope": "scope.a",
        },
    )
    assert compute_auth_fingerprint(cfg, str(tmp_path)) is not None

    async def _raise_once(**_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "sift_gateway.mcp.upstream._call_tool_once", _raise_once
    )
    instance = UpstreamInstance(
        config=cfg,
        instance_id="inst",
        tools=[],
        secret_data={
            "headers": {"Authorization": "Bearer tok"},
            "oauth": {
                "enabled": True,
                "mode": "oauth",
                "token_storage": "disk",
            },
        },
    )
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(
            call_upstream_tool(
                instance,
                "tool",
                {},
                data_dir=str(tmp_path),
            )
        )
