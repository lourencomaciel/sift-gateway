from __future__ import annotations

from pathlib import Path
import sqlite3

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.lifecycle import (
    _check_db,
    ensure_data_dirs,
    run_startup_check,
)


def test_lifecycle_ensure_data_dirs(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    paths = ensure_data_dirs(config)
    assert all(path.exists() for path in paths)


def test_lifecycle_startup_check_invalid_upstream(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(
        data_dir=tmp_path,
        upstreams=[
            UpstreamConfig(
                prefix="gh", transport="http", url="https://one.example"
            ),
            UpstreamConfig(
                prefix="gh", transport="http", url="https://two.example"
            ),
        ],
    )
    ensure_data_dirs(config)
    report = run_startup_check(config)
    assert report.upstream_ok is False
    assert report.ok is False


def test_lifecycle_startup_check_does_not_touch_existing_probe_filename(
    tmp_path: Path,
) -> None:
    config = GatewayConfig(
        data_dir=tmp_path,
        upstreams=[
            UpstreamConfig(
                prefix="gh", transport="http", url="https://one.example"
            )
        ],
    )
    ensure_data_dirs(config)
    sentinel = config.state_dir / ".gateway-write-check"
    sentinel.write_text("keep-me", encoding="utf-8")

    report = run_startup_check(config)

    assert report.fs_ok is True
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8") == "keep-me"


# ---- SQLite startup check tests ----


def test_lifecycle_startup_check_sqlite_success(tmp_path: Path) -> None:
    """run_startup_check succeeds with SQLite backend (default)."""
    config = GatewayConfig(
        data_dir=tmp_path,
        upstreams=[
            UpstreamConfig(
                prefix="gh", transport="http", url="https://one.example"
            )
        ],
    )
    ensure_data_dirs(config)
    report = run_startup_check(config)
    assert report.db_ok is True
    assert report.ok is True


def test_lifecycle_startup_check_sqlite_creates_db(tmp_path: Path) -> None:
    """SQLite check auto-creates the database file if it doesn't exist."""
    config = GatewayConfig(
        data_dir=tmp_path,
        upstreams=[
            UpstreamConfig(
                prefix="gh", transport="http", url="https://one.example"
            )
        ],
    )
    ensure_data_dirs(config)
    assert not config.sqlite_path.exists()
    report = run_startup_check(config)
    assert report.db_ok is True
    # SQLite auto-creates on connect
    assert config.sqlite_path.exists()


def test_lifecycle_startup_check_sqlite_failure(
    tmp_path: Path, monkeypatch
) -> None:
    """SQLite check reports failure when connect raises."""
    import sqlite3

    config = GatewayConfig(
        data_dir=tmp_path,
        upstreams=[
            UpstreamConfig(
                prefix="gh", transport="http", url="https://one.example"
            )
        ],
    )
    ensure_data_dirs(config)

    def _raise_error(path: str) -> None:
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr("sqlite3.connect", _raise_error)
    report = run_startup_check(config)
    assert report.db_ok is False
    assert report.ok is False
    assert any("SQLite check failed" in item for item in report.details)


def test_check_db_allows_fresh_empty_database(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    ensure_data_dirs(config)

    ok, details = _check_db(config)

    assert ok is True
    assert details == []


def test_check_db_rejects_stale_schema(tmp_path: Path) -> None:
    config = GatewayConfig(data_dir=tmp_path)
    ensure_data_dirs(config)
    with sqlite3.connect(str(config.sqlite_path)) as conn:
        conn.execute(
            "CREATE TABLE artifacts (workspace_id TEXT, artifact_id TEXT)"
        )
        conn.execute(
            "CREATE TABLE payload_blobs (workspace_id TEXT, payload_hash_full TEXT)"
        )
        conn.commit()

    ok, details = _check_db(config)

    assert ok is False
    assert any("outdated" in item.lower() for item in details)
    assert any("gateway.db" in item for item in details)
