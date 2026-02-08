"""Repository functions for mapping-related tables:
``artifacts`` (map columns), ``artifact_roots``, and ``artifact_samples``.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from mcp_artifact_gateway.canon.decimal_json import dumps_safe
from mcp_artifact_gateway.constants import WORKSPACE_ID

# ---------------------------------------------------------------------------
# Mapping status update on artifacts
# ---------------------------------------------------------------------------

_UPDATE_MAPPING_STATUS = """\
UPDATE artifacts
   SET map_kind              = %s,
       map_status            = %s,
       mapped_part_index     = %s,
       map_budget_fingerprint = %s,
       map_backend_id        = %s,
       prng_version          = %s,
       map_error             = %s,
       generation            = generation + 1
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND deleted_at IS NULL
   AND map_status IN ('pending', 'stale')
   AND generation   = %s
RETURNING *;
"""


async def update_mapping_status(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
    generation: int,
    *,
    map_kind: str,
    map_status: str,
    mapped_part_index: int | None = None,
    map_budget_fingerprint: str | None = None,
    map_backend_id: str | None = None,
    prng_version: str | None = None,
    map_error: Any | None = None,
) -> dict[str, Any] | None:
    """Conditionally update mapping columns on an artifact.

    The update only succeeds when the artifact is not deleted, its current
    ``map_status`` is ``pending`` or ``stale``, and the supplied
    ``generation`` matches.  Returns the updated row, or ``None`` on
    optimistic-lock failure.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UPDATE_MAPPING_STATUS,
            (
                map_kind,
                map_status,
                mapped_part_index,
                map_budget_fingerprint,
                map_backend_id,
                prng_version,
                psycopg.types.json.Jsonb(map_error) if map_error is not None else None,
                WORKSPACE_ID,
                artifact_id,
                generation,
            ),
        )
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# artifact_roots
# ---------------------------------------------------------------------------

_UPSERT_ARTIFACT_ROOT = """\
INSERT INTO artifact_roots (
    workspace_id,
    artifact_id,
    root_key,
    root_path,
    count_estimate,
    inventory_coverage,
    root_summary,
    root_score,
    root_shape,
    fields_top,
    examples,
    recipes,
    sample_indices
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (workspace_id, artifact_id, root_key) DO UPDATE SET
    root_path          = EXCLUDED.root_path,
    count_estimate     = EXCLUDED.count_estimate,
    inventory_coverage = EXCLUDED.inventory_coverage,
    root_summary       = EXCLUDED.root_summary,
    root_score         = EXCLUDED.root_score,
    root_shape         = EXCLUDED.root_shape,
    fields_top         = EXCLUDED.fields_top,
    examples           = EXCLUDED.examples,
    recipes            = EXCLUDED.recipes,
    sample_indices     = EXCLUDED.sample_indices
RETURNING *;
"""

_GET_ARTIFACT_ROOTS = """\
SELECT *
  FROM artifact_roots
 WHERE workspace_id = %s
   AND artifact_id  = %s
 ORDER BY root_key;
"""


async def upsert_artifact_root(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
    root_key: str,
    *,
    root_path: str,
    count_estimate: int | None,
    inventory_coverage: float,
    root_summary: str,
    root_score: float,
    root_shape: Any,
    fields_top: Any,
    examples: Any,
    recipes: Any,
    sample_indices: Any,
) -> dict[str, Any]:
    """Insert or update an artifact root."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _UPSERT_ARTIFACT_ROOT,
            (
                WORKSPACE_ID,
                artifact_id,
                root_key,
                root_path,
                count_estimate,
                inventory_coverage,
                root_summary,
                root_score,
                psycopg.types.json.Jsonb(root_shape, dumps=dumps_safe),
                psycopg.types.json.Jsonb(fields_top, dumps=dumps_safe),
                psycopg.types.json.Jsonb(examples, dumps=dumps_safe),
                psycopg.types.json.Jsonb(recipes, dumps=dumps_safe),
                psycopg.types.json.Jsonb(sample_indices, dumps=dumps_safe),
            ),
        )
        row = await cur.fetchone()
    assert row is not None
    return row


async def get_artifact_roots(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
) -> list[dict[str, Any]]:
    """Return all roots for an artifact, ordered by ``root_key``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_ARTIFACT_ROOTS, (WORKSPACE_ID, artifact_id))
        return await cur.fetchall()


# ---------------------------------------------------------------------------
# artifact_samples
# ---------------------------------------------------------------------------

_DELETE_SAMPLES_FOR_ROOT = """\
DELETE FROM artifact_samples
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND root_key     = %s;
"""

_INSERT_SAMPLE = """\
INSERT INTO artifact_samples (
    workspace_id,
    artifact_id,
    root_key,
    root_path,
    sample_index,
    record,
    record_bytes,
    record_hash
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

_GET_ARTIFACT_SAMPLES = """\
SELECT *
  FROM artifact_samples
 WHERE workspace_id = %s
   AND artifact_id  = %s
   AND root_key     = %s
 ORDER BY sample_index;
"""


async def replace_artifact_samples(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
    root_key: str,
    samples: list[dict[str, Any]],
) -> None:
    """Atomically replace all samples for a given root.

    Deletes existing samples and inserts the new batch.  The caller should
    ensure this runs inside a transaction.

    Each dict in *samples* must contain: ``root_path``, ``sample_index``,
    ``record`` (JSON-serializable), ``record_bytes``, ``record_hash``.
    """
    await conn.execute(
        _DELETE_SAMPLES_FOR_ROOT,
        (WORKSPACE_ID, artifact_id, root_key),
    )

    if not samples:
        return

    async with conn.cursor() as cur:
        await cur.executemany(
            _INSERT_SAMPLE,
            [
                (
                    WORKSPACE_ID,
                    artifact_id,
                    root_key,
                    s["root_path"],
                    s["sample_index"],
                    psycopg.types.json.Jsonb(s["record"], dumps=dumps_safe),
                    s["record_bytes"],
                    s["record_hash"],
                )
                for s in samples
            ],
        )


async def get_artifact_samples(
    conn: psycopg.AsyncConnection[dict[str, Any]],
    artifact_id: str,
    root_key: str,
) -> list[dict[str, Any]]:
    """Return samples for an artifact root, ordered by ``sample_index``."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _GET_ARTIFACT_SAMPLES,
            (WORKSPACE_ID, artifact_id, root_key),
        )
        return await cur.fetchall()
