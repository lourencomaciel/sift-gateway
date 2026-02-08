import asyncio

import pytest

from mcp_artifact_gateway.db.migrate import check_migrations, run_migrations


class FakeCursor:
    def __init__(self, conn) -> None:
        self._conn = conn
        self._last_query = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, query, params=None):
        self._last_query = query
        # Handle insertion into schema_migrations
        if isinstance(query, str) and query.startswith("INSERT INTO schema_migrations"):
            filename = params[0]
            self._conn.applied.add(filename)

    async def fetchall(self):
        if "FROM schema_migrations" in str(self._last_query):
            return [{"filename": f} for f in sorted(self._conn.applied)]
        return []

    async def fetchone(self):
        if "to_regclass" in str(self._last_query):
            return {"tbl": "schema_migrations" if self._conn.has_schema else None}
        if "FROM schema_migrations" in str(self._last_query):
            rows = [{"filename": f} for f in sorted(self._conn.applied)]
            return rows[0] if rows else None
        return None


class FakeTransaction:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self) -> None:
        self.applied: set[str] = set()
        self.has_schema = False

    async def execute(self, query, params=None):
        if isinstance(query, str) and query.startswith("CREATE TABLE IF NOT EXISTS schema_migrations"):
            self.has_schema = True
        if isinstance(query, str) and query.startswith("INSERT INTO schema_migrations"):
            filename = params[0]
            self.applied.add(filename)
        # Ignore migration SQL bodies

    async def commit(self):
        return None

    def cursor(self, row_factory=None):
        return FakeCursor(self)

    def transaction(self):
        return FakeTransaction(self)


@pytest.mark.asyncio
async def test_run_migrations_applies_pending() -> None:
    conn = FakeConnection()
    applied = await run_migrations(conn)
    assert "001_init.sql" in applied
    assert conn.has_schema is True


@pytest.mark.asyncio
async def test_check_migrations_missing_table_raises() -> None:
    conn = FakeConnection()
    with pytest.raises(RuntimeError):
        await check_migrations(conn)


@pytest.mark.asyncio
async def test_check_migrations_after_apply() -> None:
    conn = FakeConnection()
    await run_migrations(conn)
    await check_migrations(conn)
