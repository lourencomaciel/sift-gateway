from __future__ import annotations

from pathlib import Path
import re
from typing import ClassVar

import pytest

from sift_mcp.db.migrate import apply_migrations, list_migrations

# ---------------------------------------------------------------------------
# Helpers for schema content auditing
# ---------------------------------------------------------------------------
_MIGRATIONS_DIR = Path("src/sift_mcp/db/migrations_sqlite").resolve()


def _read_sql(filename: str) -> str:
    return (_MIGRATIONS_DIR / filename).read_text(encoding="utf-8")


def _normalize(sql: str) -> str:
    """Collapse whitespace for easier matching."""
    return " ".join(sql.split())


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
        if normalized.startswith(
            "select migration_name from schema_migrations"
        ):
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
        Path("src/sift_mcp/db/migrations_sqlite").resolve()
    )
    names = [path.name for path in migration_paths]
    assert "001_init.sql" in names


def test_apply_migrations_idempotent() -> None:
    connection = _FakeConnection()
    migrations_dir = Path("src/sift_mcp/db/migrations_sqlite").resolve()

    first = apply_migrations(connection, migrations_dir)
    second = apply_migrations(connection, migrations_dir)

    assert "001_init.sql" in first
    assert second == []
    assert connection.commit_calls == 2


def test_list_migrations_fails_when_directory_has_no_sql(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        list_migrations(tmp_path)


def test_list_migrations_fails_on_version_gap(tmp_path: Path) -> None:
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "003_third.sql").write_text("SELECT 1;", encoding="utf-8")

    with pytest.raises(ValueError, match="gaps"):
        list_migrations(tmp_path)


def test_list_migrations_fails_on_missing_numeric_prefix(
    tmp_path: Path,
) -> None:
    (tmp_path / "first.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(ValueError, match="numeric prefix"):
        list_migrations(tmp_path)


# ===========================================================================
# Schema audit tests (G03/G50): verify 001_init.sql satisfies v1.9 spec
# ===========================================================================


class TestSchemaTablesExist:
    """All v1.9 tables are created in 001_init.sql."""

    _EXPECTED_TABLES: ClassVar[list[str]] = [
        "schema_migrations",
        "sessions",
        "binary_blobs",
        "payload_blobs",
        "payload_hash_aliases",
        "payload_binary_refs",
        "artifacts",
        "artifact_refs",
        "artifact_roots",
        "artifact_samples",
    ]

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _normalize(_read_sql("001_init.sql"))

    @pytest.mark.parametrize("table", _EXPECTED_TABLES)
    def test_table_created(self, table: str) -> None:
        pattern = f"CREATE TABLE IF NOT EXISTS {table}"
        assert pattern.lower() in self.sql.lower(), (
            f"missing CREATE TABLE for {table}"
        )


class TestPrimaryKeysIncludeWorkspaceId:
    """Every PK (except schema_migrations) includes workspace_id."""

    _TABLES_WITH_WORKSPACE_PK: ClassVar[list[str]] = [
        "sessions",
        "binary_blobs",
        "payload_blobs",
        "payload_hash_aliases",
        "payload_binary_refs",
        "artifacts",
        "artifact_refs",
        "artifact_roots",
        "artifact_samples",
    ]

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _read_sql("001_init.sql")

    @pytest.mark.parametrize("table", _TABLES_WITH_WORKSPACE_PK)
    def test_pk_includes_workspace_id(self, table: str) -> None:
        # Find the CREATE TABLE block for the table, then find its PRIMARY KEY
        normalized = _normalize(self.sql).lower()
        # Locate the PK clause after the table creation
        table_start = normalized.find(f"create table if not exists {table}")
        assert table_start >= 0, f"table {table} not found"
        # Find the PRIMARY KEY within the next closing paren of the CREATE TABLE
        rest = normalized[table_start:]
        pk_match = re.search(r"primary key\s*\(([^)]+)\)", rest)
        assert pk_match is not None, f"no PRIMARY KEY found for {table}"
        pk_cols = pk_match.group(1)
        assert "workspace_id" in pk_cols, (
            f"workspace_id missing from PK of {table}"
        )


class TestForeignKeysExist:
    """All expected FKs exist in 001_init.sql."""

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _normalize(_read_sql("001_init.sql")).lower()

    def test_payload_hash_aliases_fk_to_payload_blobs(self) -> None:
        assert (
            "references payload_blobs (workspace_id, payload_hash_full)"
            in self.sql
        )

    def test_payload_binary_refs_fk_to_payload_blobs(self) -> None:
        assert (
            "references payload_blobs (workspace_id, payload_hash_full)"
            in self.sql
        )

    def test_payload_binary_refs_fk_to_binary_blobs(self) -> None:
        assert "references binary_blobs (workspace_id, binary_hash)" in self.sql

    def test_artifacts_fk_to_sessions(self) -> None:
        assert "references sessions (workspace_id, session_id)" in self.sql

    def test_artifacts_fk_to_payload_blobs(self) -> None:
        # This FK is in the artifacts table definition
        assert (
            "references payload_blobs (workspace_id, payload_hash_full)"
            in self.sql
        )

    def test_artifacts_self_fk_parent(self) -> None:
        assert "references artifacts (workspace_id, artifact_id)" in self.sql

    def test_artifact_refs_fk_to_sessions(self) -> None:
        assert "references sessions (workspace_id, session_id)" in self.sql

    def test_artifact_refs_fk_to_artifacts(self) -> None:
        assert "references artifacts (workspace_id, artifact_id)" in self.sql

    def test_artifact_roots_fk_to_artifacts(self) -> None:
        assert "references artifacts (workspace_id, artifact_id)" in self.sql

    def test_artifact_samples_fk_to_artifact_roots(self) -> None:
        assert (
            "references artifact_roots (workspace_id, artifact_id, root_key)"
            in self.sql
        )


class TestCreatedSeqEmulation:
    """created_seq is emulated via _created_seq_counter table + trigger."""

    def test_created_seq_uses_counter_table(self) -> None:
        sql = _normalize(_read_sql("001_init.sql")).lower()
        assert "_created_seq_counter" in sql

    def test_created_seq_trigger_exists(self) -> None:
        sql = _normalize(_read_sql("001_init.sql")).lower()
        assert "trg_artifacts_created_seq" in sql


class TestIndexStatusCheck:
    """index_status CHECK constraint allows all 5 lifecycle values."""

    def test_index_status_values(self) -> None:
        sql = _read_sql("001_init.sql").lower()
        for value in ("off", "pending", "ready", "partial", "failed"):
            assert f"'{value}'" in sql, f"index_status missing value: {value}"


class TestArtifactsColumnsComplete:
    """All columns used by the codebase exist in the 001_init.sql artifacts table."""

    _EXPECTED_COLUMNS: ClassVar[list[str]] = [
        "workspace_id",
        "artifact_id",
        "created_seq",
        "session_id",
        "source_tool",
        "upstream_instance_id",
        "request_key",
        "payload_hash_full",
        "canonicalizer_version",
        "payload_json_bytes",
        "payload_binary_bytes_total",
        "payload_total_bytes",
        "created_at",
        "expires_at",
        "deleted_at",
        "last_referenced_at",
        "generation",
        "parent_artifact_id",
        "chain_seq",
        "map_kind",
        "map_status",
        "mapper_version",
        "map_budget_fingerprint",
        "map_backend_id",
        "prng_version",
        "map_error",
        "upstream_tool_schema_hash",
        "request_args_hash",
        "request_args_prefix",
        "mapped_part_index",
        "index_status",
        "error_summary",
    ]

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        raw = _read_sql("001_init.sql")
        # Extract the CREATE TABLE artifacts block
        normalized = raw.lower()
        start = normalized.find("create table if not exists artifacts")
        assert start >= 0
        # Find matching closing paren + semicolon
        rest = raw[start:]
        self.artifacts_block = rest

    @pytest.mark.parametrize("column", _EXPECTED_COLUMNS)
    def test_column_present(self, column: str) -> None:
        assert column in self.artifacts_block.lower(), (
            f"column '{column}' missing from artifacts table in 001_init.sql"
        )


class TestOrderingIndexesExist:
    """Ordering indexes for created_seq, last_seen, request_key exist."""

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        init_sql = _normalize(_read_sql("001_init.sql")).lower()
        idx_sql = _normalize(_read_sql("002_indexes.sql")).lower()
        self.all_sql = init_sql + " " + idx_sql

    def test_idx_created_seq_desc(self) -> None:
        assert "idx_artifacts_created_seq_desc" in self.all_sql

    def test_idx_request_key_created_seq(self) -> None:
        assert "idx_artifacts_request_key_created_seq" in self.all_sql

    def test_idx_sessions_last_seen(self) -> None:
        assert "idx_sessions_last_seen" in self.all_sql

    def test_idx_artifact_refs_last_seen(self) -> None:
        assert "idx_artifact_refs_last_seen" in self.all_sql

    def test_idx_artifacts_last_referenced_at(self) -> None:
        assert "idx_artifacts_last_referenced_at" in self.all_sql

    def test_idx_artifacts_session_id(self) -> None:
        assert "idx_artifacts_session_id" in self.all_sql

    def test_idx_artifacts_payload_hash(self) -> None:
        assert "idx_artifacts_payload_hash" in self.all_sql

    def test_idx_artifacts_request_args_hash(self) -> None:
        assert "idx_artifacts_request_args_hash" in self.all_sql


class TestCheckConstraints:
    """CHECK constraints exist for enum-like columns."""

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _read_sql("001_init.sql").lower()

    def test_map_kind_check(self) -> None:
        for val in ("none", "full", "partial"):
            assert f"'{val}'" in self.sql

    def test_map_status_check(self) -> None:
        for val in ("pending", "ready", "failed", "stale"):
            assert f"'{val}'" in self.sql

    def test_envelope_encoding_check(self) -> None:
        for val in ("zstd", "gzip", "none"):
            assert f"'{val}'" in self.sql

    def test_byte_count_non_negative(self) -> None:
        assert "byte_count >= 0" in self.sql

    def test_generation_positive(self) -> None:
        assert "generation >= 1" in self.sql

    def test_mapped_part_index_non_negative(self) -> None:
        assert "mapped_part_index >= 0" in self.sql


class TestArtifactSamplesTable:
    """artifact_samples (Addendum C) exists with correct structure."""

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _normalize(_read_sql("001_init.sql")).lower()

    def test_table_exists(self) -> None:
        assert "create table if not exists artifact_samples" in self.sql

    def test_pk_includes_workspace_id(self) -> None:
        # Find PK inside artifact_samples block
        start = self.sql.find("create table if not exists artifact_samples")
        rest = self.sql[start:]
        pk_match = re.search(r"primary key\s*\(([^)]+)\)", rest)
        assert pk_match is not None
        pk_cols = pk_match.group(1)
        assert "workspace_id" in pk_cols
        assert "sample_index" in pk_cols

    def test_fk_to_artifact_roots(self) -> None:
        assert (
            "references artifact_roots (workspace_id, artifact_id, root_key)"
            in self.sql
        )

    def test_root_path_index(self) -> None:
        assert "idx_artifact_samples_root_path" in self.sql


class TestArtifactRecordsTable:
    """006_artifact_records.sql creates artifact_records with correct structure."""

    @pytest.fixture(autouse=True)
    def _load_sql(self) -> None:
        self.sql = _normalize(_read_sql("006_artifact_records.sql")).lower()

    def test_table_exists(self) -> None:
        assert "create table if not exists artifact_records" in self.sql

    def test_pk_includes_workspace_id(self) -> None:
        pk_match = re.search(r"primary key\s*\(([^)]+)\)", self.sql)
        assert pk_match is not None
        pk_cols = pk_match.group(1)
        assert "workspace_id" in pk_cols
        assert "artifact_id" in pk_cols
        assert "root_path" in pk_cols
        assert "idx" in pk_cols

    def test_fk_to_artifacts(self) -> None:
        assert (
            "references artifacts (workspace_id, artifact_id)" in self.sql
        )

    def test_on_delete_cascade(self) -> None:
        assert "on delete cascade" in self.sql

    def test_idx_non_negative_check(self) -> None:
        assert "idx >= 0" in self.sql

    def test_record_column_is_json(self) -> None:
        assert "record json not null" in self.sql

    def test_root_path_index(self) -> None:
        assert "idx_artifact_records_root_path" in self.sql


class TestNoAlterTableIn002ForColumnsNowIn001:
    """002_indexes.sql no longer uses ALTER TABLE to add columns that are now in 001."""

    def test_no_alter_table_add_column(self) -> None:
        sql = _read_sql("002_indexes.sql").lower()
        assert "alter table" not in sql, (
            "002_indexes.sql should not contain ALTER TABLE statements "
            "now that columns are in 001_init.sql"
        )
