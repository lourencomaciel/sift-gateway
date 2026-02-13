"""artifact.find handler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from sift_mcp.constants import WORKSPACE_ID
from sift_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
)
from sift_mcp.cursor.payload import CursorStaleError
from sift_mcp.envelope.responses import gateway_error
from sift_mcp.mcp.handlers.common import (
    ARTIFACT_META_COLUMNS,
    ENVELOPE_COLUMNS,
    FETCH_ARTIFACT_META_SQL,
    ROOT_COLUMNS,
    extract_json_target,
    row_to_dict,
    rows_to_dicts,
)
from sift_mcp.query.where_dsl import WhereDslError, evaluate_where
from sift_mcp.query.where_hash import where_hash
from sift_mcp.retrieval.response import apply_output_budgets

if TYPE_CHECKING:
    from sift_mcp.mcp.server import GatewayServer

# Batch query: fetch all samples for multiple root_keys in one round-trip.
_FETCH_SAMPLES_BATCH_SQL = """
SELECT root_key, sample_index, record, record_bytes, record_hash
FROM artifact_samples
WHERE workspace_id = %s AND artifact_id = %s AND root_key = ANY(%s)
ORDER BY root_key, sample_index ASC
"""
_BATCH_SAMPLE_COLUMNS = [
    "root_key",
    "sample_index",
    "record",
    "record_bytes",
    "record_hash",
]


def _find_full_mapping_items(
    ctx: GatewayServer,
    connection: Any,
    artifact_id: str,
    roots: list[dict[str, Any]],
    where_expr: Mapping[str, Any] | str | None,
    items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Populate *items* from a fully-mapped envelope.

    Reconstructs the envelope, extracts the JSON target, then
    evaluates each root's JSONPath to collect matching records.

    Args:
        ctx: Gateway server instance.
        connection: Active database connection.
        artifact_id: Artifact being queried.
        roots: Discovered root dicts with ``root_path``.
        where_expr: Optional where filter expression.
        items: Mutable list to append matching records into.

    Returns:
        A gateway error dict on failure, or ``None`` on success.
    """
    from sift_mcp.canon.rfc8785 import canonical_bytes as _canon
    from sift_mcp.query.jsonpath import (
        JsonPathError,
        evaluate_jsonpath,
    )
    from sift_mcp.storage.payload_store import reconstruct_envelope
    from sift_mcp.tools.artifact_get import FETCH_ARTIFACT_SQL
    from sift_mcp.util.hashing import sha256_trunc

    artifact_row = row_to_dict(
        connection.execute(
            FETCH_ARTIFACT_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchone(),
        ENVELOPE_COLUMNS,
    )
    if artifact_row is None:
        return gateway_error("NOT_FOUND", "artifact not found")

    envelope_value = artifact_row.get("envelope")
    canonical_bytes_raw = artifact_row.get("envelope_canonical_bytes")
    if isinstance(envelope_value, dict) and "content" in envelope_value:
        envelope = envelope_value
    elif canonical_bytes_raw is None:
        return gateway_error(
            "INTERNAL_ERROR",
            "missing canonical bytes for artifact",
        )
    else:
        try:
            envelope = reconstruct_envelope(
                compressed_bytes=bytes(canonical_bytes_raw),
                encoding=str(
                    artifact_row.get("envelope_canonical_encoding", "none")
                ),
                expected_hash=str(artifact_row.get("payload_hash_full", "")),
            )
        except ValueError as exc:
            return gateway_error(
                "INTERNAL_ERROR",
                f"envelope reconstruction failed: {exc}",
            )

    json_target = extract_json_target(
        envelope, artifact_row.get("mapped_part_index")
    )

    for root in roots:
        rp = root.get("root_path", "$")
        try:
            root_values = evaluate_jsonpath(
                json_target,
                rp,
                max_length=ctx.config.max_jsonpath_length,
                max_segments=ctx.config.max_path_segments,
                max_wildcard_expansion_total=(
                    ctx.config.max_wildcard_expansion_total
                ),
            )
        except JsonPathError as exc:
            return gateway_error("INVALID_ARGUMENT", str(exc))

        records: list[Any]
        if len(root_values) == 1 and isinstance(root_values[0], list):
            records = list(root_values[0])
        else:
            records = list(root_values)

        for index, record in enumerate(records):
            if where_expr is not None:
                try:
                    matches = evaluate_where(
                        record,
                        where_expr,
                        max_compute_steps=ctx.config.max_compute_steps,
                        max_wildcard_expansion=ctx.config.max_wildcards,
                    )
                except WhereDslError as exc:
                    return gateway_error("INVALID_ARGUMENT", str(exc))
                if not matches:
                    continue

            record_hash: str | None = None
            if isinstance(record, dict):
                try:
                    record_hash = sha256_trunc(_canon(record), 32)
                except (TypeError, ValueError):
                    pass

            items.append(
                {
                    "root_path": rp,
                    "index": index,
                    "record_hash": record_hash,
                }
            )

    return None


async def handle_artifact_find(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle the ``artifact.find`` tool call.

    Args:
        ctx: Gateway server instance providing DB and cursor helpers.
        arguments: Tool arguments including ``artifact_id``, optional
            ``root_path``, ``where``, ``cursor``, and ``limit``.

    Returns:
        Paginated find response with matching records, or a gateway
        error.
    """
    from sift_mcp.tools.artifact_describe import FETCH_ROOTS_SQL
    from sift_mcp.tools.artifact_find import (
        build_find_response,
        validate_find_args,
    )

    err = validate_find_args(arguments)
    if err is not None:
        return gateway_error(str(err["code"]), str(err["message"]))
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.find")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    artifact_id = str(arguments["artifact_id"])
    root_path_filter = arguments.get("root_path")
    if root_path_filter is not None and not isinstance(root_path_filter, str):
        return gateway_error("INVALID_ARGUMENT", "root_path must be a string")
    where_expr = arguments.get("where")
    if where_expr is not None and not isinstance(where_expr, (Mapping, str)):
        return gateway_error(
            "INVALID_ARGUMENT", "where must be an object or string"
        )
    if where_expr is None:
        where_binding_hash = "__none__"
    else:
        try:
            where_binding_hash = where_hash(
                where_expr,
                mode=ctx.config.where_canonicalization_mode.value,
            )
        except ValueError as exc:
            return gateway_error(
                "INVALID_ARGUMENT",
                f"invalid where expression: {exc}",
            )
    root_path_binding = (
        root_path_filter if isinstance(root_path_filter, str) else "__any__"
    )

    offset = 0
    cursor_payload: dict[str, Any] | None = None
    cursor_token = arguments.get("cursor")
    if isinstance(cursor_token, str) and cursor_token:
        try:
            cursor_payload = ctx._verify_cursor_payload(
                token=cursor_token,
                tool="artifact.find",
                artifact_id=artifact_id,
            )
            position = ctx._cursor_position(cursor_payload)
        except (CursorTokenError, CursorExpiredError, CursorStaleError) as exc:
            return ctx._cursor_error(exc)
        raw_offset = position.get("offset", 0)
        if not isinstance(raw_offset, int) or raw_offset < 0:
            return gateway_error("INVALID_ARGUMENT", "invalid cursor offset")
        offset = raw_offset

    with ctx.db_pool.connection() as connection:
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        ):
            return gateway_error("NOT_FOUND", "artifact not found")

        artifact_meta = row_to_dict(
            connection.execute(
                FETCH_ARTIFACT_META_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchone(),
            ARTIFACT_META_COLUMNS,
        )
        if artifact_meta is None:
            return gateway_error("NOT_FOUND", "artifact not found")
        if artifact_meta.get("deleted_at") is not None:
            ctx._safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            commit = getattr(connection, "commit", None)
            if callable(commit):
                commit()
            return gateway_error("GONE", "artifact has been deleted")
        if artifact_meta.get("map_status") != "ready":
            return gateway_error(
                "INVALID_ARGUMENT",
                "artifact mapping is not ready",
            )
        map_budget_fingerprint = (
            str(artifact_meta.get("map_budget_fingerprint"))
            if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
            else ""
        )
        if cursor_payload is not None:
            try:
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="root_path_filter",
                    expected=root_path_binding,
                )
                ctx._assert_cursor_field(
                    cursor_payload,
                    field="where_hash",
                    expected=where_binding_hash,
                )
                generation = artifact_meta.get("generation")
                if isinstance(generation, int):
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="artifact_generation",
                        expected=generation,
                    )
                if str(artifact_meta.get("map_kind", "none")) == "partial":
                    ctx._assert_cursor_field(
                        cursor_payload,
                        field="map_budget_fingerprint",
                        expected=map_budget_fingerprint,
                    )
            except CursorStaleError as exc:
                return ctx._cursor_error(exc)

        roots = rows_to_dicts(
            connection.execute(
                FETCH_ROOTS_SQL,
                (WORKSPACE_ID, artifact_id),
            ).fetchall(),
            ROOT_COLUMNS,
        )
        if root_path_filter is not None:
            roots = [
                root
                for root in roots
                if root.get("root_path") == root_path_filter
            ]

        root_keys = [root["root_key"] for root in roots]
        root_key_to_path = {
            root["root_key"]: root.get("root_path") for root in roots
        }

        items: list[dict[str, Any]] = []
        map_kind = str(artifact_meta.get("map_kind", "none"))

        if map_kind == "partial" and root_keys:
            all_sample_rows = rows_to_dicts(
                connection.execute(
                    _FETCH_SAMPLES_BATCH_SQL,
                    (WORKSPACE_ID, artifact_id, root_keys),
                ).fetchall(),
                _BATCH_SAMPLE_COLUMNS,
            )
            # Per-root corruption check
            samples_by_root: dict[str, list[dict[str, Any]]] = {}
            for sample in all_sample_rows:
                rk = sample.get("root_key", "")
                samples_by_root.setdefault(rk, []).append(sample)
            for root in roots:
                rk = root["root_key"]
                corruption = ctx._check_sample_corruption(
                    root,
                    samples_by_root.get(rk, []),
                )
                if corruption is not None:
                    return corruption
            for sample in all_sample_rows:
                record = sample.get("record")
                if where_expr is not None:
                    try:
                        matches = evaluate_where(
                            record,
                            where_expr,
                            max_compute_steps=(ctx.config.max_compute_steps),
                            max_wildcard_expansion=(ctx.config.max_wildcards),
                        )
                    except WhereDslError as exc:
                        return gateway_error("INVALID_ARGUMENT", str(exc))
                    if not matches:
                        continue
                items.append(
                    {
                        "root_path": root_key_to_path.get(
                            sample.get("root_key")
                        ),
                        "sample_index": sample.get("sample_index"),
                        "record_hash": sample.get("record_hash"),
                    }
                )
        elif root_keys:
            # Full mapping: reconstruct envelope, evaluate roots.
            full_err = _find_full_mapping_items(
                ctx,
                connection,
                artifact_id,
                roots,
                where_expr,
                items,
            )
            if full_err is not None:
                return full_err

        ctx._safe_touch_for_retrieval(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

        index_status = str(artifact_meta.get("index_status", "off"))

    max_items = ctx._bounded_limit(arguments.get("limit"))
    selected, truncated, _omitted, _used_bytes = apply_output_budgets(
        items[offset:],
        max_items=max_items,
        max_bytes_out=ctx.config.max_bytes_out,
    )
    next_cursor: str | None = None
    if truncated:
        extra: dict[str, Any] = {
            "root_path_filter": root_path_binding,
            "where_hash": where_binding_hash,
        }
        generation = artifact_meta.get("generation")
        if isinstance(generation, int):
            extra["artifact_generation"] = generation
        if str(artifact_meta.get("map_kind", "none")) == "partial":
            map_budget_fingerprint = (
                str(artifact_meta.get("map_budget_fingerprint"))
                if isinstance(artifact_meta.get("map_budget_fingerprint"), str)
                else ""
            )
            extra["map_budget_fingerprint"] = map_budget_fingerprint
        next_cursor = ctx._issue_cursor(
            tool="artifact.find",
            artifact_id=artifact_id,
            position_state={"offset": offset + len(selected)},
            extra=extra,
        )
    determinism: dict[str, str] | None = None
    if map_kind == "partial" and map_budget_fingerprint:
        from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION

        determinism = {
            "traversal_contract_version": TRAVERSAL_CONTRACT_VERSION,
            "map_budget_fingerprint": map_budget_fingerprint,
        }
    return build_find_response(
        items=selected,
        truncated=truncated,
        cursor=next_cursor,
        sampled_only=map_kind == "partial",
        index_status=index_status,
        determinism=determinism,
        matched_count=len(items),
    )
