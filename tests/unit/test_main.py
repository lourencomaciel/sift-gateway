from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from sift_mcp import __version__
from sift_mcp.config.settings import GatewayConfig
from sift_mcp.lifecycle import CheckResult
from sift_mcp.main import _parse_args, _run_upstream_add, serve


@pytest.fixture(autouse=True)
def _isolate_instances_registry(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )


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
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=True, data_dir=None),
    )
    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
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
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=False, data_dir=None),
    )
    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "FS write failed" in captured.err


def test_serve_check_mode_uses_latest_managed_instance_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_mcp.config.instances import upsert_instance

    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    data_dir = tmp_path / "instance-data"
    (data_dir / "state").mkdir(parents=True)
    (data_dir / "state" / "config.json").write_text("{}", encoding="utf-8")
    upsert_instance(source_path=source, data_dir=data_dir)

    config = GatewayConfig(data_dir=data_dir)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=True, data_dir=None),
    )

    def _fake_load_gateway_config(**kwargs: object) -> GatewayConfig:
        seen["data_dir_override"] = kwargs.get("data_dir_override")
        return config

    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()

    assert exit_code == 0
    assert seen["data_dir_override"] == str(data_dir.resolve())


def test_serve_check_mode_prefers_env_data_dir_over_managed_instance(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_mcp.config.instances import upsert_instance

    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    managed_data_dir = tmp_path / "instance-data"
    (managed_data_dir / "state").mkdir(parents=True)
    (managed_data_dir / "state" / "config.json").write_text(
        "{}", encoding="utf-8"
    )
    upsert_instance(source_path=source, data_dir=managed_data_dir)

    env_data_dir = tmp_path / "env-data"
    monkeypatch.setenv("SIFT_MCP_DATA_DIR", str(env_data_dir))

    config = GatewayConfig(data_dir=env_data_dir)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(command=None, check=True, data_dir=None),
    )

    def _fake_load_gateway_config(**kwargs: object) -> GatewayConfig:
        seen["data_dir_override"] = kwargs.get("data_dir_override")
        return config

    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()

    assert exit_code == 0
    assert seen["data_dir_override"] == str(env_data_dir.resolve())


def test_serve_check_mode_loads_config_from_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    source = (tmp_path / "source.json").resolve()
    source.write_text(
        json.dumps(
            {"mcpServers": {"artifact-gateway": {"command": "sift-mcp"}}}
        ),
        encoding="utf-8",
    )

    seed_data_dir = (tmp_path / "seed-data").resolve()
    redirected_data_dir = (tmp_path / "redirected-data").resolve()
    (seed_data_dir / "state").mkdir(parents=True)
    (redirected_data_dir / "state").mkdir(parents=True)
    (seed_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(redirected_data_dir),
                }
            }
        ),
        encoding="utf-8",
    )
    (redirected_data_dir / "state" / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    config = GatewayConfig(data_dir=redirected_data_dir)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=True,
            data_dir=str(seed_data_dir),
        ),
    )

    def _fake_load_gateway_config(**kwargs: object) -> GatewayConfig:
        seen["data_dir_override"] = kwargs.get("data_dir_override")
        return config

    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()

    assert exit_code == 0
    assert seen["data_dir_override"] == str(redirected_data_dir.resolve())


def test_serve_check_mode_ignores_stale_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    source = (tmp_path / "source.json").resolve()
    source.write_text(
        json.dumps(
            {"mcpServers": {"artifact-gateway": {"command": "sift-mcp"}}}
        ),
        encoding="utf-8",
    )

    seed_data_dir = (tmp_path / "seed-data").resolve()
    stale_redirect_data_dir = (tmp_path / "missing-redirect").resolve()
    (seed_data_dir / "state").mkdir(parents=True)
    (seed_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(stale_redirect_data_dir),
                },
                "mcpServers": {"gh": {"command": "gh"}},
            }
        ),
        encoding="utf-8",
    )

    config = GatewayConfig(data_dir=seed_data_dir)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=True,
            data_dir=str(seed_data_dir),
        ),
    )

    def _fake_load_gateway_config(**kwargs: object) -> GatewayConfig:
        seen["data_dir_override"] = kwargs.get("data_dir_override")
        return config

    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()

    assert exit_code == 0
    assert seen["data_dir_override"] == str(seed_data_dir.resolve())


def test_serve_non_check_mode_loads_config_from_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    source = (tmp_path / "source.json").resolve()
    source.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "artifact-gateway": {"command": "sift-mcp"},
                    "new-upstream": {"command": "npx", "args": ["-y"]},
                }
            }
        ),
        encoding="utf-8",
    )

    seed_data_dir = (tmp_path / "seed-data").resolve()
    redirected_data_dir = (tmp_path / "redirected-data").resolve()
    (seed_data_dir / "state").mkdir(parents=True)
    (redirected_data_dir / "state").mkdir(parents=True)
    (seed_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(redirected_data_dir),
                },
            }
        ),
        encoding="utf-8",
    )
    (redirected_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {
                "mcpServers": {},
                "_gateway_sync": {
                    "enabled": True,
                    "source_path": str(source),
                    "gateway_name": "artifact-gateway",
                    "data_dir": str(redirected_data_dir),
                },
            }
        ),
        encoding="utf-8",
    )

    config = GatewayConfig(data_dir=redirected_data_dir)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command=None,
            check=False,
            data_dir=str(seed_data_dir),
            transport="stdio",
            host="127.0.0.1",
            port=8080,
            path="/mcp",
            auth_token=None,
        ),
    )

    def _fake_load_gateway_config(**kwargs: object) -> GatewayConfig:
        seen["data_dir_override"] = kwargs.get("data_dir_override")
        return config

    monkeypatch.setattr(
        "sift_mcp.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sift_mcp.main._run_server",
        lambda _config, _report, _args: 0,
    )

    exit_code = serve()

    assert exit_code == 0
    assert seen["data_dir_override"] == str(redirected_data_dir)


def test_serve_runs_bootstrap_and_closes_pool(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    server = _FakeServer(app)

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
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
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sift_mcp.app.build_app",
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
        "sift_mcp.main._parse_args",
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
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sift_mcp.app.build_app",
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
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command="init",
            source=str(source),
            revert=False,
            dry_run=True,
            data_dir=str(data_dir),
            gateway_name="artifact-gateway",
            gateway_url=None,
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "gh" in captured.out
    # Dry run — source should be unchanged
    assert json.loads(source.read_text())["mcpServers"]["gh"]["command"] == "gh"


def test_serve_dispatches_init_command_with_source_shortcut(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr("platform.system", lambda: "Darwin")
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    source = (
        home
        / "Library"
        / "Application Support"
        / "Claude"
        / "claude_desktop_config.json"
    )
    source.parent.mkdir(parents=True, exist_ok=True)
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
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command="init",
            source="claude",
            revert=False,
            dry_run=True,
            data_dir=str(data_dir),
            gateway_name="artifact-gateway",
            gateway_url=None,
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert str(source.resolve()) in captured.out



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


# ---- Transport / HTTP bind CLI flag tests ----


def test_parse_args_transport_default_is_stdio(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sys.argv", ["sift-mcp"])
    args = _parse_args()
    assert args.transport == "stdio"


def test_parse_args_version_prints_package_version(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr("sys.argv", ["sift-mcp", "--version"])
    with pytest.raises(SystemExit) as exc_info:
        _parse_args()
    captured = capsys.readouterr()
    assert exc_info.value.code == 0
    assert captured.out.strip() == f"sift-mcp {__version__}"


def test_parse_args_transport_accepts_sse(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sys.argv", ["sift-mcp", "--transport", "sse"])
    args = _parse_args()
    assert args.transport == "sse"


def test_parse_args_transport_accepts_streamable_http(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["sift-mcp", "--transport", "streamable-http"],
    )
    args = _parse_args()
    assert args.transport == "streamable-http"


def test_parse_args_upstream_add_accepts_from_shortcut(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "upstream",
            "add",
            '{"gh":{"command":"npx"}}',
            "--from",
            "claude",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "add"
    assert args.source == "claude"
    assert args.instance_id is None


def test_parse_args_upstream_add_accepts_from_with_data_dir(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "upstream",
            "add",
            '{"gh":{"command":"npx"}}',
            "--from",
            "claude",
            "--data-dir",
            "/tmp/custom",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "add"
    assert args.source == "claude"
    assert args.instance_id is None
    assert args.data_dir == "/tmp/custom"


def test_parse_args_upstream_add_rejects_multiple_targets(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "upstream",
            "add",
            '{"gh":{"command":"npx"}}',
            "--from",
            "claude",
            "--instance",
            "abc123",
        ],
    )
    with pytest.raises(SystemExit):
        _parse_args()


def test_parse_args_instances_list_json(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "instances",
            "list",
            "--json",
        ],
    )
    args = _parse_args()
    assert args.command == "instances"
    assert args.instances_command == "list"
    assert args.json is True


def test_parse_args_global_data_dir_reaches_install(
    monkeypatch,
) -> None:
    """Global --data-dir before 'install' is visible to the handler."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "--data-dir",
            "/tmp/my-instance",
            "install",
            "pandas",
        ],
    )
    args = _parse_args()
    assert args.command == "install"
    assert args.data_dir == "/tmp/my-instance"


def test_parse_args_data_dir_after_install_subcommand(
    monkeypatch,
) -> None:
    """--data-dir after 'install' subcommand is accepted."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "install",
            "pandas",
            "--data-dir",
            "/tmp/sub-instance",
        ],
    )
    args = _parse_args()
    assert args.command == "install"
    assert args.data_dir == "/tmp/sub-instance"


def test_parse_args_data_dir_after_uninstall_subcommand(
    monkeypatch,
) -> None:
    """--data-dir after 'uninstall' subcommand is accepted."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-mcp",
            "uninstall",
            "pandas",
            "--data-dir",
            "/tmp/sub-instance",
        ],
    )
    args = _parse_args()
    assert args.command == "uninstall"
    assert args.data_dir == "/tmp/sub-instance"


def test_parse_args_host_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sift-mcp"])
    args = _parse_args()
    assert args.host == "127.0.0.1"


def test_parse_args_port_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sift-mcp"])
    args = _parse_args()
    assert args.port == 8080


def test_run_upstream_add_resolves_data_dir_from_source(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    instance_data_dir = tmp_path / "instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    upsert_instance(source_path=source, data_dir=instance_data_dir)

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["raw"] = raw
        seen["data_dir"] = data_dir
        seen["dry_run"] = dry_run
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()
    assert seen["dry_run"] is True


def test_run_upstream_add_from_source_falls_back_to_gateway_data_dir_arg(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    instance_data_dir = tmp_path / "custom-instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )
    source.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "artifact-gateway": {
                        "command": "sift-mcp",
                        "args": [
                            "--data-dir",
                            str(instance_data_dir.resolve()),
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["raw"] = raw
        seen["data_dir"] = data_dir
        seen["dry_run"] = dry_run
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()
    assert seen["dry_run"] is True


def test_run_upstream_add_from_source_prefers_gateway_data_dir_arg_over_stale_registry(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    active_data_dir = tmp_path / "active-instance-data"
    stale_registry_data_dir = tmp_path / "stale-instance-data"
    for path in (active_data_dir, stale_registry_data_dir):
        (path / "state").mkdir(parents=True)
        (path / "state" / "config.json").write_text(
            "{}",
            encoding="utf-8",
        )
    source.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "artifact-gateway": {
                        "command": "sift-mcp",
                        "args": [
                            "--data-dir",
                            str(active_data_dir.resolve()),
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    upsert_instance(
        source_path=source,
        data_dir=stale_registry_data_dir,
    )

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["raw"] = raw
        seen["data_dir"] = data_dir
        seen["dry_run"] = dry_run
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == active_data_dir.resolve()
    assert seen["dry_run"] is True


def test_run_upstream_add_from_source_prefers_gateway_data_dir_arg_when_uninitialized(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    source_data_dir = tmp_path / "custom-instance-data"
    source.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "artifact-gateway": {
                        "command": "sift-mcp",
                        "args": [
                            "--data-dir",
                            str(source_data_dir.resolve()),
                        ],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    from sift_mcp.constants import (
        CONFIG_FILENAME,
        DEFAULT_DATA_DIR,
        STATE_SUBDIR,
    )

    monkeypatch.chdir(tmp_path)
    legacy_data_dir = Path(DEFAULT_DATA_DIR).resolve()
    (legacy_data_dir / STATE_SUBDIR).mkdir(parents=True)
    (legacy_data_dir / STATE_SUBDIR / CONFIG_FILENAME).write_text(
        "{}",
        encoding="utf-8",
    )

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["raw"] = raw
        seen["data_dir"] = data_dir
        seen["dry_run"] = dry_run
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == source_data_dir.resolve()
    assert seen["dry_run"] is True


def test_run_upstream_add_with_source_respects_explicit_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps(
            {"mcpServers": {"artifact-gateway": {"command": "sift-mcp"}}}
        ),
        encoding="utf-8",
    )
    explicit_data_dir = tmp_path / "explicit-data"

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["raw"] = raw
        seen["data_dir"] = data_dir
        seen["dry_run"] = dry_run
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        data_dir=str(explicit_data_dir),
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == explicit_data_dir.resolve()
    assert seen["dry_run"] is True


def test_run_upstream_add_from_source_rejects_unresolved_target(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_mcp.constants import (
        CONFIG_FILENAME,
        DEFAULT_DATA_DIR,
        STATE_SUBDIR,
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SIFT_MCP_DATA_DIR", raising=False)

    source = tmp_path / "config.json"
    source.write_text(
        json.dumps(
            {"mcpServers": {"artifact-gateway": {"command": "sift-mcp"}}}
        ),
        encoding="utf-8",
    )

    legacy_data_dir = Path(DEFAULT_DATA_DIR).resolve()
    (legacy_data_dir / STATE_SUBDIR).mkdir(parents=True)
    (legacy_data_dir / STATE_SUBDIR / CONFIG_FILENAME).write_text(
        "{}",
        encoding="utf-8",
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        data_dir=None,
        dry_run=True,
    )
    with pytest.raises(
        ValueError,
        match="No initialized Sift instance found for source",
    ):
        _run_upstream_add(args)


def test_run_upstream_add_rejects_instance_with_explicit_data_dir(
    tmp_path: Path,
) -> None:
    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=None,
        instance_id="abc123",
        data_dir=str(tmp_path / "manual-data"),
        dry_run=True,
    )
    with pytest.raises(
        ValueError,
        match="--instance cannot be combined with --data-dir",
    ):
        _run_upstream_add(args)


def test_run_upstream_add_resolves_data_dir_from_instance_id(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    instance_data_dir = tmp_path / "instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    entry = upsert_instance(source_path=source, data_dir=instance_data_dir)

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["data_dir"] = data_dir
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=None,
        instance_id=entry["id"],
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()


def test_run_upstream_add_defaults_to_latest_managed_instance(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    monkeypatch.delenv("SIFT_MCP_DATA_DIR", raising=False)
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    instance_data_dir = tmp_path / "instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    upsert_instance(source_path=source, data_dir=instance_data_dir)

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["data_dir"] = data_dir
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=None,
        instance_id=None,
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()


def test_run_upstream_add_continues_when_source_registry_update_fails(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    instance_data_dir = tmp_path / "instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    upsert_instance(source_path=source, data_dir=instance_data_dir)

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["data_dir"] = data_dir
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    def _raise_registry_error(**_kwargs: object) -> dict[str, object]:
        msg = "read-only registry"
        raise OSError(msg)

    monkeypatch.setattr(
        "sift_mcp.config.instances.upsert_instance",
        _raise_registry_error,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=str(source),
        instance_id=None,
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()
    assert "upstream add completed but failed to update instance registry" in (
        captured.err
    )
    assert "read-only registry" in captured.err


def test_run_upstream_add_continues_when_instance_touch_fails(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    instance_data_dir = tmp_path / "instance-data"
    (instance_data_dir / "state").mkdir(parents=True)
    (instance_data_dir / "state" / "config.json").write_text(
        "{}",
        encoding="utf-8",
    )

    from sift_mcp.config.instances import upsert_instance

    entry = upsert_instance(source_path=source, data_dir=instance_data_dir)

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["data_dir"] = data_dir
        return {"added": [], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_mcp.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    def _raise_touch_error(_instance_id: str) -> None:
        msg = "read-only registry"
        raise OSError(msg)

    monkeypatch.setattr(
        "sift_mcp.config.instances.touch_instance_by_id",
        _raise_touch_error,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"npx","args":[]}}',
        source=None,
        instance_id=entry["id"],
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert seen["data_dir"] == instance_data_dir.resolve()
    assert "upstream add completed but failed to update instance registry" in (
        captured.err
    )
    assert "read-only registry" in captured.err


def test_serve_instances_list_command(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    source.write_text(
        json.dumps({"mcpServers": {"gh": {"command": "gh"}}}),
        encoding="utf-8",
    )
    data_dir = tmp_path / "instance-data"
    (data_dir / "state").mkdir(parents=True)
    (data_dir / "state" / "config.json").write_text("{}", encoding="utf-8")

    from sift_mcp.config.instances import upsert_instance

    entry = upsert_instance(source_path=source, data_dir=data_dir)

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
        lambda: argparse.Namespace(
            command="instances",
            instances_command="list",
            json=True,
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert entry["id"] in captured.out


def test_serve_http_transport_calls_run_with_transport_args(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    server = _FakeServer(app)

    monkeypatch.setattr(
        "sift_mcp.main._parse_args",
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
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sift_mcp.app.build_app",
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
        "sift_mcp.main._parse_args",
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
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sift_mcp.app.build_app",
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
        "sift_mcp.mcp.http_auth.validate_http_bind",
        _fake_validate_http_bind,
    )
    monkeypatch.setattr(
        "sift_mcp.mcp.http_auth.bearer_auth_middleware",
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
        "sift_mcp.main._parse_args",
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
        "sift_mcp.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_mcp.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.delenv("SIFT_MCP_AUTH_TOKEN", raising=False)

    with pytest.raises(SystemExit, match="Security error"):
        serve()
