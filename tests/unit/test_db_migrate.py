from __future__ import annotations

from pathlib import Path

from mcp_artifact_gateway.db.migrate import apply_migrations, list_migrations


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self) -> None:
        self.applied: set[str] = set()
        self.queries: list[str] = []
        self.commit_calls = 0

    def execute(self, query: str, params=None):
        self.queries.append(query)
        normalized = " ".join(query.lower().split())
        if normalized.startswith("select migration_name from schema_migrations"):
            return _FakeResult([(name,) for name in sorted(self.applied)])
        if normalized.startswith("insert into schema_migrations"):
            assert params is not None
            self.applied.add(str(params[0]))
            return _FakeResult([])
        return _FakeResult([])

    def commit(self):
        self.commit_calls += 1


def test_list_migrations_includes_sql_files() -> None:
    migration_paths = list_migrations(
        Path("src/mcp_artifact_gateway/db/migrations").resolve()
    )
    names = [path.name for path in migration_paths]
    assert "001_init.sql" in names


def test_apply_migrations_idempotent() -> None:
    connection = _FakeConnection()
    migrations_dir = Path("src/mcp_artifact_gateway/db/migrations").resolve()

    first = apply_migrations(connection, migrations_dir)
    second = apply_migrations(connection, migrations_dir)

    assert "001_init.sql" in first
    assert second == []
    assert connection.commit_calls == 2

