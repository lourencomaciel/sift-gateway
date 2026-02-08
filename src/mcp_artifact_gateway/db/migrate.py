"""SQL migration runner."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class CursorLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> object: ...
    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> object: ...
    def commit(self) -> None: ...


@dataclass(frozen=True)
class Migration:
    name: str
    sql: str
    path: Path


def list_migrations(migrations_dir: Path) -> list[Path]:
    return sorted(path for path in migrations_dir.glob("*.sql") if path.is_file())


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

