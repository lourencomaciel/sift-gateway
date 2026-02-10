from __future__ import annotations

from pathlib import Path

from mcp_artifact_gateway.config.settings import GatewayConfig, UpstreamConfig
from mcp_artifact_gateway.lifecycle import _check_migrations, ensure_data_dirs, run_startup_check


class _FakeCursor:
    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, _query: str) -> None:
        if self._fail:
            raise RuntimeError("probe failed")

    def fetchone(self) -> tuple[int]:
        return (1,)


class _FakeConnection:
    def __init__(self, *, fail_probe: bool = False) -> None:
        self._fail_probe = fail_probe
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(fail=self._fail_probe)

    def close(self) -> None:
        self.closed = True


def test_lifecycle_ensure_data_dirs(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    paths = ensure_data_dirs(config)
    assert all(path.exists() for path in paths)


def test_lifecycle_startup_check_invalid_upstream(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("mcp_artifact_gateway.db.conn.connect", lambda _config: _FakeConnection())
    config = GatewayConfig(
        data_dir=tmp_path,
        db_backend="postgres",
        upstreams=[
            UpstreamConfig(prefix="gh", transport="http", url="https://one.example"),
            UpstreamConfig(prefix="gh", transport="http", url="https://two.example"),
        ],
    )
    report = run_startup_check(config)
    assert report.upstream_ok is False
    assert report.ok is False


def test_lifecycle_startup_check_does_not_touch_existing_probe_filename(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("mcp_artifact_gateway.db.conn.connect", lambda _config: _FakeConnection())
    config = GatewayConfig(
        data_dir=tmp_path,
        db_backend="postgres",
        upstreams=[UpstreamConfig(prefix="gh", transport="http", url="https://one.example")],
    )
    ensure_data_dirs(config)
    sentinel = config.state_dir / ".gateway-write-check"
    sentinel.write_text("keep-me", encoding="utf-8")

    report = run_startup_check(config)

    assert report.fs_ok is True
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep-me"


def test_lifecycle_startup_check_reports_db_connect_failure(tmp_path: Path, monkeypatch) -> None:
    def _raise(_config):
        raise RuntimeError("connect failed")

    monkeypatch.setattr("mcp_artifact_gateway.db.conn.connect", _raise)

    config = GatewayConfig(
        data_dir=tmp_path,
        db_backend="postgres",
        upstreams=[UpstreamConfig(prefix="gh", transport="http", url="https://one.example")],
    )
    report = run_startup_check(config)
    assert report.db_ok is False
    assert report.ok is False
    assert any("DB connect failed" in item for item in report.details)


def test_lifecycle_startup_check_reports_db_probe_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "mcp_artifact_gateway.db.conn.connect",
        lambda _config: _FakeConnection(fail_probe=True),
    )

    config = GatewayConfig(
        data_dir=tmp_path,
        db_backend="postgres",
        upstreams=[UpstreamConfig(prefix="gh", transport="http", url="https://one.example")],
    )
    report = run_startup_check(config)
    assert report.db_ok is False
    assert report.ok is False
    assert any("DB probe query failed" in item for item in report.details)


def test_check_migrations_silent_on_failure() -> None:
    """_check_migrations never raises — errors are swallowed."""
    details: list[str] = []
    _check_migrations(None, details)  # type: ignore[arg-type]
    # No crash, no details appended for unhandled connections
    assert not any("pending" in d for d in details)


def test_check_migrations_does_not_affect_db_ok(tmp_path: Path, monkeypatch) -> None:
    """Migration check is informational; db_ok stays True even if migration check fails."""
    monkeypatch.setattr("mcp_artifact_gateway.db.conn.connect", lambda _config: _FakeConnection())
    config = GatewayConfig(
        data_dir=tmp_path,
        db_backend="postgres",
        upstreams=[UpstreamConfig(prefix="gh", transport="http", url="https://one.example")],
    )
    report = run_startup_check(config)
    # _FakeConnection doesn't support fetchall() for migration check,
    # but _check_migrations swallows the error silently
    assert report.db_ok is True
