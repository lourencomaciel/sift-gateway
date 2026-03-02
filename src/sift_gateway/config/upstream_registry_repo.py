"""SQLite data access layer for the upstream registry."""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
from pathlib import Path
from typing import Any

from sift_gateway.config.upstream_registry_convert import (
    registry_rows_to_records,
)
from sift_gateway.config.upstream_secrets import validate_prefix
from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.db.backend import SqliteBackend
from sift_gateway.db.migrate import apply_migrations

_UPSTREAM_REGISTRY_COLUMNS = (
    "prefix",
    "transport",
    "command",
    "args_json",
    "url",
    "pagination_json",
    "auto_paginate_max_pages",
    "auto_paginate_max_records",
    "auto_paginate_timeout_seconds",
    "passthrough_allowed",
    "semantic_salt_env_keys_json",
    "semantic_salt_headers_json",
    "inherit_parent_env",
    "external_user_id",
    "secret_ref",
    "enabled",
    "source_kind",
    "source_ref",
)


def _migrations_dir() -> Path:
    """Return the SQLite migrations directory."""
    return Path(__file__).resolve().parents[1] / "db" / "migrations_sqlite"


def _db_path(data_dir: Path) -> Path:
    """Return the SQLite DB path for a data dir."""
    return data_dir / "state" / "gateway.db"


@contextlib.contextmanager
def connect_migrated(data_dir: Path) -> Iterator[Any]:
    """Yield migrated SQLite connection for registry operations.

    Each call opens a fresh connection and re-checks migrations.
    This is acceptable for CLI admin commands but would benefit
    from a shared connection if used in hot paths.
    """
    db_path = _db_path(data_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    backend = SqliteBackend(db_path=db_path)
    try:
        with backend.connection() as conn:
            apply_migrations(conn, _migrations_dir())
            yield conn
    finally:
        backend.close()


def upsert_payload(conn: Any, payload: dict[str, Any]) -> None:
    """Upsert one registry payload row."""
    conn.execute(
        """
        INSERT INTO upstream_registry (
            workspace_id,
            prefix,
            transport,
            command,
            args_json,
            url,
            pagination_json,
            auto_paginate_max_pages,
            auto_paginate_max_records,
            auto_paginate_timeout_seconds,
            passthrough_allowed,
            semantic_salt_env_keys_json,
            semantic_salt_headers_json,
            inherit_parent_env,
            external_user_id,
            secret_ref,
            enabled,
            source_kind,
            source_ref
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (workspace_id, prefix)
        DO UPDATE SET
            transport = EXCLUDED.transport,
            command = EXCLUDED.command,
            args_json = EXCLUDED.args_json,
            url = EXCLUDED.url,
            pagination_json = EXCLUDED.pagination_json,
            auto_paginate_max_pages =
                EXCLUDED.auto_paginate_max_pages,
            auto_paginate_max_records =
                EXCLUDED.auto_paginate_max_records,
            auto_paginate_timeout_seconds =
                EXCLUDED.auto_paginate_timeout_seconds,
            passthrough_allowed = EXCLUDED.passthrough_allowed,
            semantic_salt_env_keys_json =
                EXCLUDED.semantic_salt_env_keys_json,
            semantic_salt_headers_json =
                EXCLUDED.semantic_salt_headers_json,
            inherit_parent_env = EXCLUDED.inherit_parent_env,
            external_user_id = EXCLUDED.external_user_id,
            secret_ref = EXCLUDED.secret_ref,
            enabled = EXCLUDED.enabled,
            source_kind = EXCLUDED.source_kind,
            source_ref = EXCLUDED.source_ref,
            updated_at = datetime('now')
        """,
        tuple(
            payload[column]
            for column in (
                "workspace_id",
                *_UPSTREAM_REGISTRY_COLUMNS,
            )
        ),
    )


def load_registry_upstream_records(
    data_dir: Path,
    *,
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    """Load registry rows as normalized record dicts."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return []

    with connect_migrated(data_dir) as conn:
        sql = (
            "SELECT "
            + ", ".join(_UPSTREAM_REGISTRY_COLUMNS)
            + " FROM upstream_registry WHERE workspace_id = %s"
        )
        params: tuple[Any, ...] = (WORKSPACE_ID,)
        if not include_disabled:
            sql += " AND enabled = 1"
        # Preserve insertion order so index-based env overrides
        # (e.g. SIFT_GATEWAY_UPSTREAMS__0__...) remain stable.
        sql += " ORDER BY rowid"
        rows = conn.execute(sql, params).fetchall()
    return registry_rows_to_records(rows)


def get_registry_upstream_record(
    *,
    data_dir: Path,
    prefix: str,
) -> dict[str, Any] | None:
    """Load one registry row by prefix."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return None

    with connect_migrated(data_dir) as conn:
        rows = conn.execute(
            (
                "SELECT "
                + ", ".join(_UPSTREAM_REGISTRY_COLUMNS)
                + " FROM upstream_registry "
                + "WHERE workspace_id = %s AND prefix = %s"
            ),
            (WORKSPACE_ID, prefix),
        ).fetchall()
    if not rows:
        return None
    return registry_rows_to_records(rows)[0]


def remove_registry_upstream(
    *,
    data_dir: Path,
    prefix: str,
) -> bool:
    """Remove one registry row by prefix."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "DELETE FROM upstream_registry "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (WORKSPACE_ID, prefix),
        )
        conn.commit()
    return getattr(cursor, "rowcount", 0) > 0


def set_registry_upstream_enabled(
    *,
    data_dir: Path,
    prefix: str,
    enabled: bool,
) -> bool:
    """Set ``enabled`` state for one registry row."""
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "UPDATE upstream_registry "
                "SET enabled = %s, updated_at = datetime('now') "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (int(enabled), WORKSPACE_ID, prefix),
        )
        conn.commit()
    return getattr(cursor, "rowcount", 0) > 0


def set_registry_upstream_secret_ref(
    *,
    data_dir: Path,
    prefix: str,
    secret_ref: str,
) -> bool:
    """Set ``secret_ref`` for one registry row."""
    validate_prefix(secret_ref)
    db_path = _db_path(data_dir)
    if not db_path.exists():
        return False
    with connect_migrated(data_dir) as conn:
        cursor = conn.execute(
            (
                "UPDATE upstream_registry "
                "SET secret_ref = %s, updated_at = datetime('now') "
                "WHERE workspace_id = %s AND prefix = %s"
            ),
            (secret_ref, WORKSPACE_ID, prefix),
        )
        conn.commit()
    return getattr(cursor, "rowcount", 0) > 0
