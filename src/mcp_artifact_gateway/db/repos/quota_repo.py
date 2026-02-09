"""Quota repository: storage usage queries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.constants import WORKSPACE_ID


@dataclass(frozen=True)
class StorageUsage:
    """Current storage usage snapshot."""
    total_payload_bytes: int
    total_binary_bytes: int
    artifact_count: int

    @property
    def total_bytes(self) -> int:
        return self.total_payload_bytes


STORAGE_USAGE_SQL = """
SELECT
    COALESCE(SUM(payload_total_bytes), 0) AS total_payload_bytes,
    COALESCE(SUM(payload_binary_bytes_total), 0) AS total_binary_bytes,
    COUNT(*) AS artifact_count
FROM artifacts
WHERE workspace_id = %s
  AND deleted_at IS NULL
"""


def storage_usage_params() -> tuple[str]:
    return (WORKSPACE_ID,)


def parse_storage_usage(row: tuple[object, ...] | None) -> StorageUsage:
    """Parse a storage usage query result row."""
    if row is None or len(row) < 3:
        return StorageUsage(total_payload_bytes=0, total_binary_bytes=0, artifact_count=0)
    total_payload = int(row[0]) if isinstance(row[0], (int, float)) else 0
    total_binary = int(row[1]) if isinstance(row[1], (int, float)) else 0
    count = int(row[2]) if isinstance(row[2], (int, float)) else 0
    return StorageUsage(
        total_payload_bytes=total_payload,
        total_binary_bytes=total_binary,
        artifact_count=count,
    )


def query_storage_usage(connection: Any) -> StorageUsage:
    """Query current storage usage from the database."""
    row = connection.execute(STORAGE_USAGE_SQL, storage_usage_params()).fetchone()
    return parse_storage_usage(row)
