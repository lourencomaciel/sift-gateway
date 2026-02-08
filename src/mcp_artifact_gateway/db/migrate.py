"""SQL migration runner."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class CursorLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> object: ...
    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> CursorLike: ...
    def commit(self) -> None: ...


@dataclass(frozen=True)
class Migration:
    name: str
    sql: str
    path: Path


def list_migrations(migrations_dir: Path) -> list[Path]:
    paths = sorted(path for path in migrations_dir.glob("*.sql") if path.is_file())
    if not paths:
        msg = f"no SQL migrations found in {migrations_dir}"
        raise FileNotFoundError(msg)
    _validate_migration_sequence(paths)
    return paths


def load_migrations(migrations_dir: Path) -> list[Migration]:
    migrations = []
    for path in list_migrations(migrations_dir):
        migrations.append(
            Migration(
                name=path.name,
                sql=path.read_text(encoding="utf-8"),
                path=path,
            )
        )
    return migrations


def _ensure_schema_migrations(connection: ConnectionLike) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            migration_name TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _applied_set(connection: ConnectionLike) -> set[str]:
    rows = connection.execute("SELECT migration_name FROM schema_migrations").fetchall()
    return {str(row[0]) for row in rows}


def _migration_version(path: Path) -> int:
    head, _, _ = path.name.partition("_")
    if not head.isdigit():
        msg = f"invalid migration filename (missing numeric prefix): {path.name}"
        raise ValueError(msg)
    return int(head)


def _validate_migration_sequence(paths: list[Path]) -> None:
    versions = [_migration_version(path) for path in paths]
    if len(versions) != len(set(versions)):
        msg = "duplicate migration version detected"
        raise ValueError(msg)

    expected = list(range(1, max(versions) + 1))
    if versions != expected:
        missing = sorted(set(expected) - set(versions))
        msg = f"migration sequence has gaps; missing versions: {missing}"
        raise ValueError(msg)


def apply_migrations(connection: ConnectionLike, migrations_dir: Path) -> list[str]:
    _ensure_schema_migrations(connection)
    applied = _applied_set(connection)
    newly_applied: list[str] = []

    for migration in load_migrations(migrations_dir):
        if migration.name in applied:
            continue
        connection.execute(migration.sql)
        connection.execute(
            "INSERT INTO schema_migrations (migration_name) VALUES (%s)",
            (migration.name,),
        )
        newly_applied.append(migration.name)

    connection.commit()
    return newly_applied
