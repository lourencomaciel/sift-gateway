from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from sidepouch_mcp.config.settings import GatewayConfig
from sidepouch_mcp.lifecycle import CheckResult
from sidepouch_mcp.main import _parse_args, serve


class _FakeConnectionContext:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self) -> None:
        self.closed = False

    def connection(self) -> _FakeConnectionContext:
        return _FakeConnectionContext()

    def close(self) -> None:
        self.closed = True


class _FakeApp:
    def __init__(self) -> None:
        self.called = False
        self.kwargs: dict[str, object] = {}

    def run(self, **kwargs: object) -> None:
        self.called = True
        self.kwargs = dict(kwargs)


class _FakeServer:
    def __init__(self, app: _FakeApp) -> None:
        self._app = app

    def build_fastmcp_app(self) -> _FakeApp:
        return self._app


def test_serve_check_mode_prints_startup_report(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=True, data_dir=None),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "fs_ok=True" in captured.out
    assert "db_ok=True" in captured.out
    assert "upstream_ok=True" in captured.out
    assert "versions:" in captured.out
    assert "canonicalizer=" in captured.out
    assert "budgets:" in captured.out
    assert "max_items=" in captured.out


def test_serve_returns_one_when_startup_check_fails(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(
        fs_ok=False,
        db_ok=True,
        upstream_ok=True,
        details=["FS write failed"],
    )

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=False, data_dir=None),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "FS write failed" in captured.err


def test_serve_runs_bootstrap_and_closes_pool(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    server = _FakeServer(app)

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=None,
            transport="stdio",
            host="127.0.0.1",
            port=8080,
            path="/mcp",
            auth_token=None,
        ),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sidepouch_mcp.app.build_app",
        lambda *, config, startup_report: (server, pool),
    )

    exit_code = serve()

    assert exit_code == 0
    assert pool.closed is True
    assert app.called is True
    assert app.kwargs == {"show_banner": False}


def test_serve_drains_mapping_tasks_on_shutdown(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    drain_called = {"called": False}

    class _DrainableServer:
        def __init__(self, app_obj: _FakeApp) -> None:
            self._app = app_obj

        def build_fastmcp_app(self) -> _FakeApp:
            return self._app

        async def drain_mapping_tasks(self, *, timeout: float = 30.0) -> int:
            drain_called["called"] = True
            return 0

    server = _DrainableServer(app)

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=None,
            transport="stdio",
            host="127.0.0.1",
            port=8080,
            path="/mcp",
            auth_token=None,
        ),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sidepouch_mcp.app.build_app",
        lambda *, config, startup_report: (server, pool),
    )

    exit_code = serve()

    assert exit_code == 0
    assert pool.closed is True
    assert drain_called["called"] is True


def test_serve_dispatches_init_command(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source = tmp_path / "claude_desktop_config.json"
    source.write_text(
        json.dumps(
            {
                "mcpServers": {"gh": {"command": "gh"}},
            }
        ),
        encoding="utf-8",
    )
    data_dir = tmp_path / "gateway"

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command="init",
            source=str(source),
            revert=False,
            dry_run=True,
            data_dir=str(data_dir),
            gateway_name="artifact-gateway",
            gateway_url=None,
            postgres_dsn=None,
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "gh" in captured.out
    # Dry run — source should be unchanged
    assert json.loads(source.read_text())["mcpServers"]["gh"]["command"] == "gh"


def test_init_accepts_postgres_dsn_flag(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps(
            {
                "mcpServers": {"gh": {"command": "gh"}},
            }
        ),
        encoding="utf-8",
    )
    data_dir = tmp_path / "gateway"

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command="init",
            source=str(source),
            revert=False,
            dry_run=False,
            data_dir=str(data_dir),
            gateway_name="artifact-gateway",
            gateway_url=None,
            postgres_dsn="postgresql://custom:pass@host:5432/db",
        ),
    )

    exit_code = serve()

    assert exit_code == 0
    gw_config = json.loads((data_dir / "state" / "config.json").read_text())
    assert gw_config["postgres_dsn"] == "postgresql://custom:pass@host:5432/db"


# ---- Database backend configuration tests ----


def test_gateway_config_default_backend_is_sqlite(tmp_path: Path) -> None:
    """Default db_backend is 'sqlite' (zero-config)."""
    config = GatewayConfig(data_dir=tmp_path)
    assert config.db_backend == "sqlite"


def test_gateway_config_postgres_backend(tmp_path: Path) -> None:
    """db_backend can be set to 'postgres'."""
    config = GatewayConfig(data_dir=tmp_path, db_backend="postgres")
    assert config.db_backend == "postgres"


def test_gateway_config_sqlite_path_derived(tmp_path: Path) -> None:
    """sqlite_path property resolves to {state_dir}/gateway.db."""
    config = GatewayConfig(data_dir=tmp_path)
    expected = tmp_path / "state" / "gateway.db"
    assert config.sqlite_path == expected


def test_gateway_config_sqlite_busy_timeout_default(tmp_path: Path) -> None:
    """sqlite_busy_timeout_ms defaults to 5000."""
    config = GatewayConfig(data_dir=tmp_path)
    assert config.sqlite_busy_timeout_ms == 5000


def test_gateway_config_sqlite_busy_timeout_customizable(
    tmp_path: Path,
) -> None:
    """sqlite_busy_timeout_ms can be overridden."""
    config = GatewayConfig(data_dir=tmp_path, sqlite_busy_timeout_ms=10000)
    assert config.sqlite_busy_timeout_ms == 10000


def test_gateway_config_db_backend_env_override(
    tmp_path: Path, monkeypatch
) -> None:
    """SIDEPOUCH_MCP_DB_BACKEND env var overrides default."""
    monkeypatch.setenv("SIDEPOUCH_MCP_DB_BACKEND", "postgres")
    from sidepouch_mcp.config.settings import load_gateway_config

    config = load_gateway_config(data_dir_override=str(tmp_path))
    assert config.db_backend == "postgres"


# ---- Transport / HTTP bind CLI flag tests ----


def test_parse_args_transport_default_is_stdio(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sys.argv", ["sidepouch-mcp"])
    args = _parse_args()
    assert args.transport == "stdio"


def test_parse_args_transport_accepts_sse(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sys.argv", ["sidepouch-mcp", "--transport", "sse"])
    args = _parse_args()
    assert args.transport == "sse"


def test_parse_args_transport_accepts_streamable_http(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["sidepouch-mcp", "--transport", "streamable-http"],
    )
    args = _parse_args()
    assert args.transport == "streamable-http"


def test_parse_args_host_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sidepouch-mcp"])
    args = _parse_args()
    assert args.host == "127.0.0.1"


def test_parse_args_port_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sidepouch-mcp"])
    args = _parse_args()
    assert args.port == 8080


def test_serve_http_transport_calls_run_with_transport_args(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    server = _FakeServer(app)

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=None,
            transport="sse",
            host="127.0.0.1",
            port=9090,
            path="/v1/mcp",
            auth_token=None,
        ),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.app.build_app",
        lambda *, config, startup_report: (server, pool),
    )

    exit_code = serve()

    assert exit_code == 0
    assert app.called is True
    assert app.kwargs == {
        "transport": "sse",
        "host": "127.0.0.1",
        "port": 9090,
        "path": "/v1/mcp",
    }


def test_serve_http_transport_with_token_wraps_asgi_and_runs_uvicorn(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()

    class _FakeHttpApp(_FakeApp):
        def __init__(self) -> None:
            super().__init__()
            self.http_kwargs: dict[str, object] = {}
            self.http_asgi = object()

        def http_app(self, **kwargs: object) -> object:
            self.http_kwargs = dict(kwargs)
            return self.http_asgi

    app = _FakeHttpApp()
    server = _FakeServer(app)

    wrapped_asgi = object()
    middleware_seen: dict[str, object] = {}
    uvicorn_seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=str(tmp_path),
            transport="sse",
            host="127.0.0.1",
            port=9090,
            path="/v1/mcp",
            auth_token="token-123",
        ),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.app.build_app",
        lambda *, config, startup_report: (server, pool),
    )

    def _fake_validate_http_bind(host: str, token: str | None) -> None:
        middleware_seen["validated_host"] = host
        middleware_seen["validated_token"] = token

    def _fake_bearer_auth_middleware(asgi: object, token: str) -> object:
        middleware_seen["inner_asgi"] = asgi
        middleware_seen["middleware_token"] = token
        return wrapped_asgi

    monkeypatch.setattr(
        "sidepouch_mcp.mcp.http_auth.validate_http_bind",
        _fake_validate_http_bind,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.mcp.http_auth.bearer_auth_middleware",
        _fake_bearer_auth_middleware,
    )

    import uvicorn

    def _fake_uvicorn_run(asgi: object, *, host: str, port: int) -> None:
        uvicorn_seen["asgi"] = asgi
        uvicorn_seen["host"] = host
        uvicorn_seen["port"] = port

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    exit_code = serve()

    assert exit_code == 0
    assert app.called is False
    assert app.http_kwargs == {"transport": "sse", "path": "/v1/mcp"}
    assert middleware_seen == {
        "validated_host": "127.0.0.1",
        "validated_token": "token-123",
        "inner_asgi": app.http_asgi,
        "middleware_token": "token-123",
    }
    assert uvicorn_seen == {
        "asgi": wrapped_asgi,
        "host": "127.0.0.1",
        "port": 9090,
    }


def test_serve_nonlocal_host_without_token_exits(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])

    monkeypatch.setattr(
        "sidepouch_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=None,
            transport="sse",
            host="0.0.0.0",
            port=8080,
            path="/mcp",
            auth_token=None,
        ),
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sidepouch_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.delenv("SIDEPOUCH_MCP_AUTH_TOKEN", raising=False)

    with pytest.raises(SystemExit, match="Security error"):
        serve()
