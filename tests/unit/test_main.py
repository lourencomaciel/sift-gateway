from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from sift_gateway import __version__
from sift_gateway.config.settings import GatewayConfig
from sift_gateway.lifecycle import CheckResult
from sift_gateway.main import (
    _extract_logs_flag,
    _parse_args,
    _run_upstream_add,
    _run_upstream_auth_check,
    _run_upstream_auth_set,
    _run_upstream_inspect,
    _run_upstream_list,
    _run_upstream_login,
    _run_upstream_remove,
    _run_upstream_set_enabled,
    _run_upstream_test,
    cli,
    serve,
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
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(command=None, check=True, data_dir=None),
    )
    monkeypatch.setattr(
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
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
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(command=None, check=False, data_dir=None),
    )
    monkeypatch.setattr(
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "FS write failed" in captured.err


def test_serve_check_mode_loads_config_from_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    source = (tmp_path / "source.json").resolve()
    source.write_text(
        json.dumps(
            {"mcpServers": {"artifact-gateway": {"command": "sift-gateway"}}}
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
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
            {"mcpServers": {"artifact-gateway": {"command": "sift-gateway"}}}
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
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
                    "artifact-gateway": {"command": "sift-gateway"},
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        _fake_load_gateway_config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_server",
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check", lambda _config: report
    )
    monkeypatch.setattr(
        "sift_gateway.app.build_app",
        lambda *, config, startup_report: (server, pool),
    )

    exit_code = serve()

    assert exit_code == 0
    assert pool.closed is True
    assert app.called is True
    assert app.kwargs == {"show_banner": False}


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
        "sift_gateway.main._parse_args",
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


def test_serve_dispatches_upstream_list_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="list",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_list",
        lambda _args: 17,
    )

    exit_code = serve()
    assert exit_code == 17


def test_serve_dispatches_upstream_inspect_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="inspect",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_inspect",
        lambda _args: 18,
    )

    exit_code = serve()
    assert exit_code == 18


def test_serve_dispatches_upstream_test_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="test",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_test",
        lambda _args: 22,
    )

    exit_code = serve()
    assert exit_code == 22


def test_serve_dispatches_upstream_remove_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="remove",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_remove",
        lambda _args: 24,
    )

    exit_code = serve()
    assert exit_code == 24


def test_serve_dispatches_upstream_enable_command(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_run(args: argparse.Namespace, *, enabled: bool) -> int:
        seen["enabled"] = enabled
        return 26

    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="enable",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_set_enabled", _fake_run
    )

    exit_code = serve()
    assert exit_code == 26
    assert seen["enabled"] is True


def test_serve_dispatches_upstream_disable_command(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_run(args: argparse.Namespace, *, enabled: bool) -> int:
        seen["enabled"] = enabled
        return 28

    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="disable",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_set_enabled", _fake_run
    )

    exit_code = serve()
    assert exit_code == 28
    assert seen["enabled"] is False


def test_serve_unknown_upstream_command_prints_usage(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="unknown",
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert (
        "upstream {add,list,inspect,test,remove,enable,disable,login,auth}"
        in captured.err
    )


def test_run_upstream_list_resolves_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    seed_data_dir = tmp_path / "seed"
    runtime_data_dir = tmp_path / "runtime"
    (seed_data_dir / "state").mkdir(parents=True)
    (runtime_data_dir / "state").mkdir(parents=True)
    (seed_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {"_gateway_sync": {"data_dir": str(runtime_data_dir.resolve())}}
        ),
        encoding="utf-8",
    )
    (runtime_data_dir / "state" / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    seen: dict[str, object] = {}

    def _fake_list_upstreams(*, data_dir: Path | None = None):
        seen["data_dir"] = data_dir
        return []

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.list_upstreams",
        _fake_list_upstreams,
    )

    args = argparse.Namespace(data_dir=str(seed_data_dir), json=True)
    exit_code = _run_upstream_list(args)

    assert exit_code == 0
    assert seen["data_dir"] == runtime_data_dir.resolve()


def test_run_upstream_inspect_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.inspect_upstream",
        lambda **_kwargs: {
            "name": "gh",
            "enabled": True,
            "transport": "stdio",
            "command": "gh",
            "url": None,
            "args": ["mcp"],
            "secret": {"ref": "gh"},
        },
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        json=True,
    )

    exit_code = _run_upstream_inspect(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["name"] == "gh"
    assert payload["transport"] == "stdio"


def test_run_upstream_inspect_prints_text_secret_metadata(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.inspect_upstream",
        lambda **_kwargs: {
            "name": "gh",
            "enabled": False,
            "transport": "stdio",
            "command": "gh",
            "url": None,
            "args": ["mcp"],
            "secret": {
                "ref": "gh",
                "transport": "stdio",
                "env_keys": ["GITHUB_TOKEN"],
                "header_keys": [],
            },
        },
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        json=False,
    )

    exit_code = _run_upstream_inspect(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "name: gh" in captured.out
    assert "enabled: False" in captured.out
    assert "secret_ref: gh" in captured.out
    assert "secret_env_keys: ['GITHUB_TOKEN']" in captured.out


def test_run_upstream_test_returns_non_zero_when_probe_fails(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.probe_upstreams",
        lambda **_kwargs: {
            "results": [],
            "ok": False,
            "ok_count": 0,
            "total": 0,
        },
    )
    args = argparse.Namespace(
        server="gh",
        all=False,
        data_dir=str(tmp_path),
        json=True,
    )

    exit_code = _run_upstream_test(args)
    captured = capsys.readouterr()

    assert exit_code == 1
    assert json.loads(captured.out)["ok"] is False


def test_run_upstream_test_prints_text_summary(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.probe_upstreams",
        lambda **_kwargs: {
            "results": [
                {"name": "gh", "ok": True, "tool_count": 3},
                {
                    "name": "api",
                    "ok": False,
                    "error_code": "UPSTREAM_FAILED",
                    "error": "boom",
                },
            ],
            "ok": False,
            "ok_count": 1,
            "total": 2,
        },
    )
    args = argparse.Namespace(
        server=None,
        all=True,
        data_dir=str(tmp_path),
        json=False,
    )

    exit_code = _run_upstream_test(args)
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ok gh tool_count=3" in captured.out
    assert "fail api error_code=UPSTREAM_FAILED error=boom" in captured.out
    assert "summary: 1/2 ok" in captured.out


def test_run_upstream_remove_prints_dry_run_message(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.remove_upstream",
        lambda **_kwargs: {"removed": "gh"},
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        dry_run=True,
        json=False,
    )

    exit_code = _run_upstream_remove(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "[dry run] would remove upstream: gh" in captured.out


def test_run_upstream_remove_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.remove_upstream",
        lambda **_kwargs: {"removed": "gh", "dry_run": False},
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        dry_run=False,
        json=True,
    )

    exit_code = _run_upstream_remove(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured.out)["removed"] == "gh"


def test_run_upstream_set_enabled_prints_dry_run_message(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.set_upstream_enabled",
        lambda **_kwargs: {"server": "gh", "enabled": False},
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        dry_run=True,
        json=False,
    )

    exit_code = _run_upstream_set_enabled(args, enabled=False)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "[dry run] would disable upstream: gh" in captured.out


def test_run_upstream_set_enabled_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.set_upstream_enabled",
        lambda **_kwargs: {"server": "gh", "enabled": True},
    )
    args = argparse.Namespace(
        server="gh",
        data_dir=str(tmp_path),
        dry_run=False,
        json=True,
    )

    exit_code = _run_upstream_set_enabled(args, enabled=True)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured.out)["enabled"] is True


def test_run_upstream_auth_set_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.set_upstream_auth",
        lambda **_kwargs: {
            "server": "gh",
            "transport": "stdio",
            "secret_ref": "gh",
        },
    )
    args = argparse.Namespace(
        server="gh",
        env_pairs=["TOKEN=abc"],
        header_pairs=None,
        data_dir=str(tmp_path),
        dry_run=False,
        json=True,
    )

    exit_code = _run_upstream_auth_set(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["server"] == "gh"
    assert payload["secret_ref"] == "gh"


def test_run_upstream_auth_check_prints_text_summary(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.probe_oauth_upstreams",
        lambda **_kwargs: {
            "results": [
                {
                    "name": "oauth-api",
                    "ok": True,
                    "tool_count": 2,
                    "forced_refresh": True,
                },
                {
                    "name": "broken-api",
                    "ok": False,
                    "error_code": "UPSTREAM_RUNTIME_FAILURE",
                    "error": "boom",
                },
            ],
            "ok": False,
            "ok_count": 1,
            "total": 2,
        },
    )
    args = argparse.Namespace(
        server=None,
        all=True,
        data_dir=str(tmp_path),
        json=False,
    )

    exit_code = _run_upstream_auth_check(args)
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ok oauth-api tool_count=2 forced_refresh=True" in captured.out
    assert (
        "fail broken-api error_code=UPSTREAM_RUNTIME_FAILURE error=boom"
        in captured.out
    )
    assert "summary: 1/2 ok" in captured.out


def test_run_upstream_auth_check_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.probe_oauth_upstreams",
        lambda **_kwargs: {
            "results": [],
            "ok": True,
            "ok_count": 0,
            "total": 0,
        },
    )
    args = argparse.Namespace(
        server=None,
        all=True,
        data_dir=str(tmp_path),
        json=True,
    )

    exit_code = _run_upstream_auth_check(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    assert json.loads(captured.out)["ok"] is True


def test_run_upstream_login_prints_json(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    seen: dict[str, object] = {}

    def _fake_login_upstream(**kwargs: object) -> dict[str, object]:
        seen.update(kwargs)
        return {
            "server": "api",
            "transport": "http",
            "secret_ref": "api",
            "login": "oauth",
        }

    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.login_upstream",
        _fake_login_upstream,
    )
    args = argparse.Namespace(
        server="api",
        data_dir=str(tmp_path),
        dry_run=False,
        headless=True,
        json=True,
    )

    exit_code = _run_upstream_login(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["server"] == "api"
    assert payload["login"] == "oauth"
    assert seen["headless"] is True


def test_serve_dispatches_upstream_auth_set_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="auth",
            auth_command="set",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_auth_set",
        lambda _args: 19,
    )

    exit_code = serve()
    assert exit_code == 19


def test_serve_dispatches_upstream_auth_check_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="auth",
            auth_command="check",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_auth_check",
        lambda _args: 29,
    )

    exit_code = serve()
    assert exit_code == 29


def test_serve_dispatches_upstream_login_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="login",
        ),
    )
    monkeypatch.setattr(
        "sift_gateway.main._run_upstream_login",
        lambda _args: 31,
    )

    exit_code = serve()
    assert exit_code == 31


def test_serve_upstream_auth_requires_subcommand(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
        lambda: argparse.Namespace(
            command="upstream",
            upstream_command="auth",
            auth_command=None,
        ),
    )

    exit_code = serve()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "upstream auth {set,check}" in captured.err


def test_serve_dispatches_artifact_cli_mode(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_cli_serve(argv: list[str] | None = None) -> int:
        seen["argv"] = argv
        return 23

    monkeypatch.setattr(
        "sift_gateway.cli_main.serve",
        _fake_cli_serve,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "--data-dir",
            "/tmp/demo",
            "run",
            "--",
            "echo",
            "ok",
        ],
    )

    exit_code = serve()

    assert exit_code == 23
    assert seen["argv"] == [
        "--data-dir",
        "/tmp/demo",
        "run",
        "--",
        "echo",
        "ok",
    ]


def test_serve_dispatches_artifact_cli_mode_with_logs_flag(
    monkeypatch,
) -> None:
    seen: dict[str, object] = {}

    def _fake_cli_serve(argv: list[str] | None = None) -> int:
        seen["argv"] = argv
        return 23

    monkeypatch.setattr(
        "sift_gateway.cli_main.serve",
        _fake_cli_serve,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "--logs",
            "--data-dir",
            "/tmp/demo",
            "run",
            "--",
            "echo",
            "ok",
        ],
    )

    exit_code = serve()

    assert exit_code == 23
    assert seen["argv"] == [
        "--data-dir",
        "/tmp/demo",
        "run",
        "--",
        "echo",
        "ok",
    ]


def test_extract_logs_flag_does_not_strip_after_run_separator() -> None:
    logs_enabled, argv = _extract_logs_flag(
        ["run", "--", "echo", "--logs"],
    )
    assert logs_enabled is False
    assert argv == ["run", "--", "echo", "--logs"]


def test_cli_disables_logs_by_default(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_configure_logging(**kwargs: object) -> None:
        seen["logging_kwargs"] = kwargs

    def _fake_serve(argv: list[str] | None = None) -> int:
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(
        "sift_gateway.obs.logging.configure_logging",
        _fake_configure_logging,
    )
    monkeypatch.setattr("sift_gateway.main.serve", _fake_serve)
    monkeypatch.setattr("sys.argv", ["sift-gateway", "--check"])

    with pytest.raises(SystemExit) as exc_info:
        cli()

    assert exc_info.value.code == 0
    assert seen["argv"] == ["--check"]
    assert seen["logging_kwargs"] == {"enabled": False}


def test_cli_enables_logs_with_logs_flag(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_configure_logging(**kwargs: object) -> None:
        seen["logging_kwargs"] = kwargs

    def _fake_serve(argv: list[str] | None = None) -> int:
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(
        "sift_gateway.obs.logging.configure_logging",
        _fake_configure_logging,
    )
    monkeypatch.setattr("sift_gateway.main.serve", _fake_serve)
    monkeypatch.setattr("sys.argv", ["sift-gateway", "--logs", "--check"])

    with pytest.raises(SystemExit) as exc_info:
        cli()

    assert exc_info.value.code == 0
    assert seen["argv"] == ["--check"]
    assert seen["logging_kwargs"] == {"enabled": True}


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
        "sift_gateway.main._parse_args",
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
    monkeypatch.setattr("sys.argv", ["sift-gateway"])
    args = _parse_args()
    assert args.transport == "stdio"


def test_parse_args_version_prints_package_version(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr("sys.argv", ["sift-gateway", "--version"])
    with pytest.raises(SystemExit) as exc_info:
        _parse_args()
    captured = capsys.readouterr()
    assert exc_info.value.code == 0
    assert captured.out.strip() == f"sift-gateway {__version__}"


def test_parse_args_transport_accepts_sse(
    monkeypatch,
) -> None:
    monkeypatch.setattr("sys.argv", ["sift-gateway", "--transport", "sse"])
    args = _parse_args()
    assert args.transport == "sse"


def test_parse_args_transport_accepts_streamable_http(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["sift-gateway", "--transport", "streamable-http"],
    )
    args = _parse_args()
    assert args.transport == "streamable-http"


def test_parse_args_upstream_add_accepts_from_shortcut(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
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
    assert not hasattr(args, "instance_id")


def test_parse_args_upstream_add_accepts_from_with_data_dir(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
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
    assert not hasattr(args, "instance_id")
    assert args.data_dir == "/tmp/custom"


def test_parse_args_upstream_add_flag_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "add",
            "--name",
            "github",
            "--transport",
            "stdio",
            "--command",
            "npx",
            "--arg",
            "-y",
            "--arg",
            "@modelcontextprotocol/server-github",
            "--env",
            "GITHUB_TOKEN=abc",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "add"
    assert args.snippet is None
    assert args.name == "github"
    assert args.transport == "stdio"
    assert args.stdio_command == "npx"
    assert args.command_args == [
        "-y",
        "@modelcontextprotocol/server-github",
    ]
    assert args.env_pairs == ["GITHUB_TOKEN=abc"]


def test_parse_args_upstream_add_accepts_long_option_arg_value(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "add",
            "--name",
            "github",
            "--transport",
            "stdio",
            "--command",
            "npx",
            "--arg",
            "--verbose",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "add"
    assert args.command_args == ["--verbose"]
    assert args.dry_run is False


@pytest.mark.parametrize(
    "arg_value",
    [
        "--dry-run",
        "--data-dir",
        "--from",
        "--help",
    ],
)
def test_parse_args_upstream_add_accepts_reserved_flag_names_as_arg_values(
    monkeypatch,
    arg_value: str,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "add",
            "--name",
            "github",
            "--transport",
            "stdio",
            "--command",
            "npx",
            "--dry-run",
            "--arg",
            arg_value,
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "add"
    assert args.dry_run is True
    assert args.command_args == [arg_value]


def test_parse_args_upstream_auth_set(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "auth",
            "set",
            "--server",
            "api",
            "--header",
            "Authorization=Bearer tok",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "auth"
    assert args.auth_command == "set"
    assert args.server == "api"
    assert args.header_pairs == ["Authorization=Bearer tok"]


def test_parse_args_upstream_auth_check(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "auth",
            "check",
            "--all",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "auth"
    assert args.auth_command == "check"
    assert args.all is True
    assert args.server is None


def test_parse_args_upstream_login(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "login",
            "--server",
            "notion",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "login"
    assert args.server == "notion"
    assert args.dry_run is False
    assert args.headless is False


def test_parse_args_upstream_login_headless(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
            "upstream",
            "login",
            "--server",
            "notion",
            "--headless",
        ],
    )
    args = _parse_args()
    assert args.command == "upstream"
    assert args.upstream_command == "login"
    assert args.server == "notion"
    assert args.headless is True


def test_parse_args_global_data_dir_reaches_install(
    monkeypatch,
) -> None:
    """Global --data-dir before 'install' is visible to the handler."""
    monkeypatch.setattr(
        "sys.argv",
        [
            "sift-gateway",
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
            "sift-gateway",
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
            "sift-gateway",
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
    monkeypatch.setattr("sys.argv", ["sift-gateway"])
    args = _parse_args()
    assert args.host == "127.0.0.1"


def test_parse_args_port_default(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sift-gateway"])
    args = _parse_args()
    assert args.port == 8080


@pytest.mark.parametrize(
    ("attr", "value", "flag"),
    [
        ("transport", "stdio", "--transport"),
        ("stdio_command", "npx", "--command"),
        ("url", "https://example.com/mcp", "--url"),
        ("command_args", ["--dry-run"], "--arg"),
        ("env_pairs", ["TOKEN=abc"], "--env"),
        ("header_pairs", ["Authorization=Bearer tok"], "--header"),
        ("external_user_id", "user-123", "--external-user-id"),
        ("inherit_parent_env", True, "--inherit-parent-env"),
    ],
)
def test_run_upstream_add_rejects_mixed_snippet_and_flag_mode_inputs(
    attr: str,
    value: object,
    flag: str,
) -> None:
    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        transport=None,
        stdio_command=None,
        url=None,
        command_args=None,
        env_pairs=None,
        header_pairs=None,
        external_user_id=None,
        inherit_parent_env=False,
        source=None,
        data_dir=None,
        dry_run=True,
    )
    setattr(args, attr, value)

    with pytest.raises(
        ValueError,
        match="legacy snippet mode cannot be combined with flag-based options",
    ) as exc_info:
        _run_upstream_add(args)

    message = str(exc_info.value)
    assert "legacy snippet mode cannot be combined with flag-based options" in (
        message
    )
    assert flag in message


def test_run_upstream_add_from_source_falls_back_to_gateway_data_dir_arg(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_GATEWAY_INSTANCES_DIR",
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
                        "command": "sift-gateway",
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
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
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


def test_run_upstream_add_from_source_prefers_gateway_data_dir_arg_when_uninitialized(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(
        "SIFT_GATEWAY_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "config.json"
    source_data_dir = tmp_path / "custom-instance-data"
    source.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "artifact-gateway": {
                        "command": "sift-gateway",
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

    from sift_gateway.constants import (
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
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
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
            {"mcpServers": {"artifact-gateway": {"command": "sift-gateway"}}}
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
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
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


def test_run_upstream_add_resolves_sync_redirect_data_dir(
    tmp_path: Path, monkeypatch
) -> None:
    seed_data_dir = tmp_path / "seed"
    runtime_data_dir = tmp_path / "runtime"
    (seed_data_dir / "state").mkdir(parents=True)
    (runtime_data_dir / "state").mkdir(parents=True)
    (seed_data_dir / "state" / "config.json").write_text(
        json.dumps(
            {"_gateway_sync": {"data_dir": str(runtime_data_dir.resolve())}}
        ),
        encoding="utf-8",
    )
    (runtime_data_dir / "state" / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        seen["run_data_dir"] = data_dir
        seen["run_dry_run"] = dry_run
        seen["raw"] = raw
        return {"added": ["github"], "skipped": [], "config_path": "ignored"}

    def _fake_bootstrap_registry_from_config(data_dir: Path) -> int:
        seen["bootstrap_data_dir"] = data_dir
        return 0

    def _fake_merge_missing_registry_from_config(data_dir: Path) -> int:
        seen["merge_data_dir"] = data_dir
        return 0

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        _fake_bootstrap_registry_from_config,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        _fake_merge_missing_registry_from_config,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        lambda **_kw: 0,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.mirror_registry_to_config",
        lambda _dd: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        source=None,
        data_dir=str(seed_data_dir),
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["run_data_dir"] == runtime_data_dir.resolve()
    assert seen["bootstrap_data_dir"] == runtime_data_dir.resolve()
    assert seen["merge_data_dir"] == runtime_data_dir.resolve()


def test_run_upstream_add_flag_mode_builds_servers_dict(
    tmp_path: Path, monkeypatch
) -> None:
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
        return {"added": ["github"], "skipped": [], "config_path": "ignored"}

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet=None,
        name="github",
        transport="stdio",
        stdio_command="npx",
        command_args=["-y", "@modelcontextprotocol/server-github"],
        env_pairs=["GITHUB_TOKEN=abc"],
        header_pairs=None,
        inherit_parent_env=True,
        external_user_id="auto",
        source=None,
        data_dir=str(tmp_path),
        dry_run=True,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["dry_run"] is True
    assert seen["data_dir"] == tmp_path.resolve()
    assert seen["raw"] == {
        "github": {
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-github"],
            "env": {"GITHUB_TOKEN": "abc"},
            "_gateway": {
                "inherit_parent_env": True,
                "external_user_id": "auto",
            },
        }
    }


def test_run_upstream_add_reconciles_added_server_to_registry(
    tmp_path: Path, monkeypatch
) -> None:
    from sift_gateway.config.settings import load_gateway_config
    from sift_gateway.config.upstream_registry import (
        replace_registry_from_mcp_servers,
    )

    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    replace_registry_from_mcp_servers(
        data_dir=tmp_path,
        servers={"github": {"command": "old-gh"}},
        source_kind="manual",
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"new-gh"}}',
        name=None,
        source=None,
        data_dir=str(tmp_path),
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    config = load_gateway_config(data_dir_override=str(tmp_path))
    github = next(item for item in config.upstreams if item.prefix == "github")
    assert github.command == "new-gh"


def test_run_upstream_add_invalid_mirror_keeps_successful_write(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": {"bad": "oops"}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        source=None,
        data_dir=str(tmp_path),
        dry_run=False,
    )

    exit_code = _run_upstream_add(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    config = json.loads((state_dir / "config.json").read_text(encoding="utf-8"))
    assert config["mcpServers"]["github"]["command"] == "gh"
    assert not (state_dir / "gateway.db").exists()
    assert "warning: skipped full registry sync" in captured.err
    assert "warning: skipped registry reconciliation" in captured.err


def test_run_upstream_add_registry_sync_error_is_non_fatal(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: (_ for _ in ()).throw(OSError("db locked")),
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("db locked")),
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        source=None,
        data_dir=str(tmp_path),
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    config = json.loads((state_dir / "config.json").read_text(encoding="utf-8"))
    assert config["mcpServers"]["github"]["command"] == "gh"
    assert "warning: skipped full registry sync" in captured.err
    assert "warning: skipped registry reconciliation" in captured.err


def test_run_upstream_add_reconcile_falls_back_to_raw_snippet(
    tmp_path: Path, monkeypatch
) -> None:
    seen: dict[str, object] = {}

    def _fake_run_upstream_add(
        raw: dict[str, object],
        *,
        data_dir: Path | None = None,
        dry_run: bool = False,
    ) -> dict[str, object]:
        assert raw == {"github": {"command": "gh"}}
        assert data_dir == tmp_path.resolve()
        assert dry_run is False
        return {"added": ["github"], "skipped": [], "config_path": "ignored"}

    def _fake_upsert_registry_from_mcp_servers(
        *,
        data_dir: Path,
        servers: dict[str, dict[str, object]],
        merge_missing: bool,
        source_kind: str,
        source_ref: str | None = None,
    ) -> int:
        seen["data_dir"] = data_dir
        seen["servers"] = servers
        seen["merge_missing"] = merge_missing
        seen["source_kind"] = source_kind
        seen["source_ref"] = source_ref
        return len(servers)

    def _always_raise_extract(_raw: dict[str, object]) -> dict[str, object]:
        raise ValueError("broken mirror")

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.run_upstream_add",
        _fake_run_upstream_add,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: 0,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: 0,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.load_registry_upstream_records",
        lambda *_args, **_kwargs: [{"prefix": "existing"}],
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        _fake_upsert_registry_from_mcp_servers,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.mirror_registry_to_config",
        lambda _data_dir: tmp_path / "state" / "config.json",
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.extract_mcp_servers",
        _always_raise_extract,
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        source=None,
        data_dir=str(tmp_path),
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)

    assert exit_code == 0
    assert seen["data_dir"] == tmp_path.resolve()
    assert seen["servers"] == {"github": {"command": "gh"}}
    assert seen["merge_missing"] is False
    assert seen["source_kind"] == "snippet_add"
    assert seen["source_ref"] is None


def test_run_upstream_add_registry_reconcile_error_is_non_fatal(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sift_gateway.config.upstream_add.print_add_summary",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.bootstrap_registry_from_config",
        lambda _data_dir: 0,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.merge_missing_registry_from_config",
        lambda _data_dir: 0,
    )
    monkeypatch.setattr(
        "sift_gateway.config.upstream_admin.upsert_registry_from_mcp_servers",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("db write failed")),
    )

    args = argparse.Namespace(
        snippet='{"github":{"command":"gh"}}',
        name=None,
        source=None,
        data_dir=str(tmp_path),
        dry_run=False,
    )
    exit_code = _run_upstream_add(args)
    captured = capsys.readouterr()

    assert exit_code == 0
    config = json.loads((state_dir / "config.json").read_text(encoding="utf-8"))
    assert config["mcpServers"]["github"]["command"] == "gh"
    assert "registry reconciliation failed" in captured.err


def test_serve_http_transport_calls_run_with_transport_args(
    tmp_path: Path, monkeypatch
) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    report = CheckResult(fs_ok=True, db_ok=True, upstream_ok=True, details=[])
    pool = _FakePool()
    app = _FakeApp()
    server = _FakeServer(app)

    monkeypatch.setattr(
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sift_gateway.app.build_app",
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.setattr(
        "sift_gateway.app.build_app",
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
        "sift_gateway.mcp.http_auth.validate_http_bind",
        _fake_validate_http_bind,
    )
    monkeypatch.setattr(
        "sift_gateway.mcp.http_auth.bearer_auth_middleware",
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
        "sift_gateway.main._parse_args",
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
        "sift_gateway.main.load_gateway_config",
        lambda **_kwargs: config,
    )
    monkeypatch.setattr(
        "sift_gateway.main.run_startup_check",
        lambda _config: report,
    )
    monkeypatch.delenv("SIFT_GATEWAY_AUTH_TOKEN", raising=False)

    with pytest.raises(SystemExit, match="Security error"):
        serve()
