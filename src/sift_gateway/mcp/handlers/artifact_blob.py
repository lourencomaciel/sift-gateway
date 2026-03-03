"""Handlers for blob-oriented actions in the consolidated artifact tool."""

from __future__ import annotations

from collections.abc import Sequence
import csv
import importlib
import json
import mimetypes
import os
from pathlib import Path
import re
import shutil
import time
from typing import TYPE_CHECKING, Any

from sift_gateway.constants import WORKSPACE_ID
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.fs.blob_store import normalize_mime
from sift_gateway.mcp.lineage import resolve_related_artifacts

if TYPE_CHECKING:
    from sift_gateway.mcp.server import GatewayServer

_QUERY_SCOPES = frozenset({"all_related", "single"})
_BLOB_LIST_DEFAULT_LIMIT = 100
_BLOB_LIST_MAX_LIMIT = 1_000
_BLOB_CLEANUP_DEFAULT_LIMIT = 1_000
_BLOB_CLEANUP_MAX_LIMIT = 10_000
_MATERIALIZED_BLOB_SUBDIR = "materialized_blobs"
_MANIFEST_SUBDIR = "blob_manifests"
_EXTENSION_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,15}$")

_BLOB_LIST_COLUMNS = [
    "artifact_id",
    "source_tool",
    "blob_id",
    "binary_hash",
    "mime",
    "byte_count",
]
_BLOB_LOOKUP_COLUMNS = [
    "blob_id",
    "binary_hash",
    "mime",
    "byte_count",
    "fs_path",
]

_LIST_BLOBS_FOR_ARTIFACTS_SQL = """
SELECT a.artifact_id,
       a.source_tool,
       bb.blob_id,
       bb.binary_hash,
       bb.mime,
       bb.byte_count
FROM artifacts a
JOIN payload_binary_refs pbr
  ON pbr.workspace_id = a.workspace_id
 AND pbr.payload_hash_full = a.payload_hash_full
JOIN binary_blobs bb
  ON bb.workspace_id = pbr.workspace_id
 AND bb.binary_hash = pbr.binary_hash
WHERE a.workspace_id = %s
  AND a.deleted_at IS NULL
  AND a.artifact_id = ANY(%s)
ORDER BY a.created_seq DESC, a.artifact_id ASC, bb.binary_hash ASC
"""

_FETCH_BLOB_BY_ID_SQL = """
SELECT blob_id, binary_hash, mime, byte_count, fs_path
FROM binary_blobs
WHERE workspace_id = %s
  AND blob_id = %s
LIMIT 1
"""

_FETCH_BLOB_BY_HASH_SQL = """
SELECT blob_id, binary_hash, mime, byte_count, fs_path
FROM binary_blobs
WHERE workspace_id = %s
  AND binary_hash = %s
LIMIT 1
"""

_MIME_EXTENSION_OVERRIDES = {
    "application/gzip": ".gz",
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
    "audio/mpeg": ".mp3",
    "audio/wav": ".wav",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "text/csv": ".csv",
    "text/plain": ".txt",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
}


def _blob_uri(blob_id: str) -> str:
    """Build stable internal URI for a blob id."""
    return f"sift://blob/{blob_id}"


def _row_to_dict(
    row: tuple[object, ...] | dict[str, Any] | None,
    columns: list[str],
) -> dict[str, Any] | None:
    """Convert a DB row into a dict keyed by column name."""
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return {
        column: row[index] if index < len(row) else None
        for index, column in enumerate(columns)
    }


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _normalize_artifact_ids(
    raw_artifact_ids: object,
) -> tuple[list[str] | None, dict[str, Any] | None]:
    """Validate and normalize user-provided artifact ids."""
    if raw_artifact_ids is None:
        return None, None
    if not isinstance(raw_artifact_ids, list) or not raw_artifact_ids:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "artifact_ids must be a non-empty list of strings",
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for value in raw_artifact_ids:
        if not _is_non_empty_string(value):
            return None, gateway_error(
                "INVALID_ARGUMENT",
                "artifact_ids must contain only non-empty strings",
            )
        artifact_id = str(value).strip()
        if artifact_id in seen:
            continue
        seen.add(artifact_id)
        deduped.append(artifact_id)
    if not deduped:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "artifact_ids must contain only non-empty strings",
        )
    return deduped, None


def _resolve_scope(raw_scope: object) -> tuple[str, dict[str, Any] | None]:
    """Normalize optional scope argument."""
    if raw_scope is None:
        return "single", None
    if isinstance(raw_scope, str) and raw_scope in _QUERY_SCOPES:
        return raw_scope, None
    return "", gateway_error(
        "INVALID_ARGUMENT",
        "scope must be one of: all_related, single",
    )


def _resolve_blob_list_limit(
    raw_limit: object,
) -> tuple[int, dict[str, Any] | None]:
    """Normalize and bounds-check blob_list result limit."""
    if raw_limit is None:
        return _BLOB_LIST_DEFAULT_LIMIT, None
    if not isinstance(raw_limit, int):
        return 0, gateway_error("INVALID_ARGUMENT", "limit must be an integer")
    if raw_limit < 1 or raw_limit > _BLOB_LIST_MAX_LIMIT:
        return 0, gateway_error(
            "INVALID_ARGUMENT",
            (
                "limit must be between 1 and "
                f"{_BLOB_LIST_MAX_LIMIT}"
            ),
        )
    return raw_limit, None


def _resolve_blob_cleanup_limit(
    raw_limit: object,
) -> tuple[int, dict[str, Any] | None]:
    """Normalize and bounds-check blob_cleanup candidate limit."""
    if raw_limit is None:
        return _BLOB_CLEANUP_DEFAULT_LIMIT, None
    if not isinstance(raw_limit, int):
        return 0, gateway_error("INVALID_ARGUMENT", "limit must be an integer")
    if raw_limit < 1 or raw_limit > _BLOB_CLEANUP_MAX_LIMIT:
        return 0, gateway_error(
            "INVALID_ARGUMENT",
            (
                "limit must be between 1 and "
                f"{_BLOB_CLEANUP_MAX_LIMIT}"
            ),
        )
    return raw_limit, None


def _check_artifact_visibility(
    ctx: GatewayServer,
    connection: Any,
    *,
    session_id: str,
    artifact_ids: Sequence[str],
) -> dict[str, Any] | None:
    """Ensure all requested artifacts are visible to the session."""
    if not session_id:
        return None
    missing = [
        artifact_id
        for artifact_id in artifact_ids
        if not ctx._artifact_visible(
            connection,
            session_id=session_id,
            artifact_id=artifact_id,
        )
    ]
    if not missing:
        return None
    return gateway_error(
        "NOT_FOUND",
        "artifact not found",
        details={"artifact_ids": missing},
    )


def _resolve_blob_list_artifacts(
    ctx: GatewayServer,
    connection: Any,
    arguments: dict[str, Any],
    *,
    session_id: str,
) -> tuple[list[str] | None, str, dict[str, Any] | None]:
    """Resolve anchor artifact ids for blob listing."""
    raw_artifact_id = arguments.get("artifact_id")
    artifact_id = (
        str(raw_artifact_id).strip()
        if _is_non_empty_string(raw_artifact_id)
        else None
    )
    artifact_ids, artifact_ids_err = _normalize_artifact_ids(
        arguments.get("artifact_ids")
    )
    if artifact_ids_err is not None:
        return None, "", artifact_ids_err

    if artifact_id is not None and artifact_ids is not None:
        return None, "", gateway_error(
            "INVALID_ARGUMENT",
            "Provide either artifact_id or artifact_ids, not both",
        )
    if artifact_id is None and artifact_ids is None:
        return None, "", gateway_error(
            "INVALID_ARGUMENT",
            "artifact_id or artifact_ids is required for action=blob_list",
        )

    scope, scope_err = _resolve_scope(arguments.get("scope"))
    if scope_err is not None:
        return None, "", scope_err

    if artifact_ids is not None:
        if scope == "all_related":
            return None, "", gateway_error(
                "INVALID_ARGUMENT",
                "scope=all_related requires artifact_id anchor",
            )
        visibility_err = _check_artifact_visibility(
            ctx,
            connection,
            session_id=session_id,
            artifact_ids=artifact_ids,
        )
        if visibility_err is not None:
            return None, "", visibility_err
        return artifact_ids, "single", None

    assert artifact_id is not None
    if scope == "single":
        visibility_err = _check_artifact_visibility(
            ctx,
            connection,
            session_id=session_id,
            artifact_ids=[artifact_id],
        )
        if visibility_err is not None:
            return None, "", visibility_err
        return [artifact_id], scope, None

    related_rows = resolve_related_artifacts(
        connection,
        session_id=session_id,
        anchor_artifact_id=artifact_id,
    )
    related_ids = [
        related_id
        for row in related_rows
        if isinstance((related_id := row.get("artifact_id")), str)
    ]
    if not related_ids:
        return None, "", gateway_error("NOT_FOUND", "artifact not found")
    return related_ids, scope, None


def _build_blob_list_entries(
    rows: list[dict[str, Any]],
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Build deduplicated blob rows from artifact/blob join rows."""
    by_hash: dict[str, dict[str, Any]] = {}
    for row in rows:
        binary_hash = row.get("binary_hash")
        blob_id = row.get("blob_id")
        if not isinstance(binary_hash, str) or not isinstance(blob_id, str):
            continue
        artifact_id = row.get("artifact_id")
        source_tool = row.get("source_tool")
        entry = by_hash.setdefault(
            binary_hash,
            {
                "blob_id": blob_id,
                "binary_hash": binary_hash,
                "mime": row.get("mime"),
                "byte_count": row.get("byte_count"),
                "uri": _blob_uri(blob_id),
                "artifact_ids": [],
                "source_artifact_id": artifact_id,
                "source_tool": source_tool,
            },
        )
        if isinstance(artifact_id, str) and artifact_id not in entry["artifact_ids"]:
            entry["artifact_ids"].append(artifact_id)
    entries = list(by_hash.values())
    for entry in entries:
        entry["artifact_count"] = len(entry["artifact_ids"])
    total = len(entries)
    truncated = total > limit
    return entries[:limit], total, truncated


def _parse_extension(
    raw_extension: object,
) -> tuple[str | None, dict[str, Any] | None]:
    """Validate an optional extension override."""
    if raw_extension is None:
        return None, None
    if not isinstance(raw_extension, str) or not raw_extension.strip():
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "extension must be a non-empty string when provided",
        )
    token = raw_extension.strip().lower()
    if token.startswith("."):
        token = token[1:]
    if not _EXTENSION_TOKEN.fullmatch(token):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "extension must be alphanumeric (plus ._-), max 16 chars",
        )
    return f".{token}", None


def _parse_filename(
    raw_filename: object,
) -> tuple[str | None, dict[str, Any] | None]:
    """Validate optional output filename."""
    if raw_filename is None:
        return None, None
    if not isinstance(raw_filename, str) or not raw_filename.strip():
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "filename must be a non-empty string when provided",
        )
    filename = raw_filename.strip()
    candidate = Path(filename)
    if (
        candidate.is_absolute()
        or candidate.name != filename
        or filename in {".", ".."}
    ):
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "filename must be a leaf filename without path separators",
        )
    return filename, None


def _path_is_within(path: Path, root: Path) -> bool:
    """Return whether *path* is equal to or contained by *root*."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _allowed_materialization_roots(ctx: GatewayServer) -> list[Path]:
    """Return allowed destination roots for blob materialization."""
    roots = [ctx.config.tmp_dir.resolve(), (Path.cwd() / ".tmp").resolve()]
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        token = str(root)
        if token in seen:
            continue
        seen.add(token)
        unique.append(root)
    return unique


def _resolve_destination_dir(
    ctx: GatewayServer,
    raw_dest_dir: object,
) -> tuple[Path | None, dict[str, Any] | None]:
    """Resolve destination directory and enforce allowlist roots."""
    if raw_dest_dir is None:
        destination = (ctx.config.tmp_dir / _MATERIALIZED_BLOB_SUBDIR).resolve()
    elif isinstance(raw_dest_dir, str) and raw_dest_dir.strip():
        candidate = Path(raw_dest_dir.strip()).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        destination = candidate.resolve()
    else:
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "destination_dir must be a non-empty string when provided",
        )

    allowed_roots = _allowed_materialization_roots(ctx)
    if any(_path_is_within(destination, root) for root in allowed_roots):
        return destination, None
    return None, gateway_error(
        "INVALID_ARGUMENT",
        "destination_dir must be inside an allowed staging root",
        details={"allowed_roots": [str(root) for root in allowed_roots]},
    )


def _resolve_one_allowed_path(
    ctx: GatewayServer,
    raw_path: object,
) -> tuple[Path | None, dict[str, Any] | None]:
    """Resolve one path and enforce allowlist roots."""
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None, gateway_error(
            "INVALID_ARGUMENT",
            "path entries must be non-empty strings",
        )
    candidate = Path(raw_path.strip()).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    resolved = candidate.resolve(strict=False)
    allowed_roots = _allowed_materialization_roots(ctx)
    if any(_path_is_within(resolved, root) for root in allowed_roots):
        return resolved, None
    return None, gateway_error(
        "INVALID_ARGUMENT",
        "path must be inside an allowed staging root",
        details={"allowed_roots": [str(root) for root in allowed_roots]},
    )


def _resolve_cleanup_paths(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> tuple[list[Path], dict[str, Any] | None]:
    """Normalize optional explicit cleanup paths."""
    raw_path = arguments.get("path")
    raw_paths = arguments.get("paths")
    if raw_paths is not None and not isinstance(raw_paths, list):
        return [], gateway_error(
            "INVALID_ARGUMENT",
            "paths must be a list of strings when provided",
        )

    collected_raw: list[object] = []
    if raw_path is not None:
        collected_raw.append(raw_path)
    if isinstance(raw_paths, list):
        collected_raw.extend(raw_paths)
    if not collected_raw:
        return [], None

    resolved: list[Path] = []
    seen: set[str] = set()
    for item in collected_raw:
        path, path_err = _resolve_one_allowed_path(ctx, item)
        if path_err is not None:
            return [], path_err
        assert path is not None
        token = str(path)
        if token in seen:
            continue
        seen.add(token)
        resolved.append(path)
    return resolved, None


def _resolve_materialize_mode(
    raw_mode: object,
) -> tuple[str, dict[str, Any] | None]:
    """Validate materialization mode selection."""
    if raw_mode is None:
        return "copy", None
    if not isinstance(raw_mode, str):
        return "", gateway_error(
            "INVALID_ARGUMENT",
            "materialize_mode must be one of: copy, hardlink, auto",
        )
    mode = raw_mode.strip().lower()
    if mode not in {"copy", "hardlink", "auto"}:
        return "", gateway_error(
            "INVALID_ARGUMENT",
            "materialize_mode must be one of: copy, hardlink, auto",
        )
    return mode, None


def _resolve_manifest_format(
    raw_format: object,
) -> tuple[str, dict[str, Any] | None]:
    """Validate manifest serialization format."""
    if raw_format is None:
        return "csv", None
    if not isinstance(raw_format, str):
        return "", gateway_error(
            "INVALID_ARGUMENT",
            "format must be one of: csv, json",
        )
    manifest_format = raw_format.strip().lower()
    if manifest_format not in {"csv", "json"}:
        return "", gateway_error(
            "INVALID_ARGUMENT",
            "format must be one of: csv, json",
        )
    return manifest_format, None


def _resolve_manifest_destination_dir(
    ctx: GatewayServer,
    raw_dest_dir: object,
) -> tuple[Path | None, dict[str, Any] | None]:
    """Resolve destination for manifest export files."""
    destination: Path | None
    if raw_dest_dir is None:
        destination = (ctx.config.tmp_dir / _MANIFEST_SUBDIR).resolve()
    else:
        destination, err = _resolve_destination_dir(ctx, raw_dest_dir)
        if err is not None:
            return None, err
        assert destination is not None
    return destination, None


def _resolve_manifest_filename(
    *,
    filename: str | None,
    manifest_format: str,
) -> str:
    """Resolve output filename for manifest file."""
    extension = f".{manifest_format}"
    if filename is None:
        timestamp = int(time.time())
        return f"blob_manifest_{timestamp}{extension}"
    suffix = Path(filename).suffix.lower()
    if not suffix:
        return f"{filename}{extension}"
    if suffix == extension:
        return filename
    stem = Path(filename).stem
    return f"{stem}{extension}"


def _materialize_manifest_csv(path: Path, blobs: Sequence[dict[str, Any]]) -> None:
    """Write manifest rows as CSV."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "blob_id",
                "binary_hash",
                "mime",
                "byte_count",
                "uri",
                "source_artifact_id",
                "source_tool",
                "artifact_count",
                "artifact_ids",
            ],
        )
        writer.writeheader()
        for blob in blobs:
            artifact_ids = blob.get("artifact_ids")
            writer.writerow(
                {
                    "blob_id": blob.get("blob_id"),
                    "binary_hash": blob.get("binary_hash"),
                    "mime": blob.get("mime"),
                    "byte_count": blob.get("byte_count"),
                    "uri": blob.get("uri"),
                    "source_artifact_id": blob.get("source_artifact_id"),
                    "source_tool": blob.get("source_tool"),
                    "artifact_count": blob.get("artifact_count"),
                    "artifact_ids": ";".join(artifact_ids)
                    if isinstance(artifact_ids, list)
                    else "",
                }
            )


def _materialize_manifest_json(
    path: Path,
    payload: dict[str, Any],
) -> None:
    """Write manifest payload as JSON."""
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _collect_cleanup_candidates(
    *,
    root: Path,
    older_than_seconds: int,
    limit: int,
    now_ts: float,
) -> list[Path]:
    """Collect file candidates under root for sweep-mode cleanup."""
    if not root.exists():
        return []
    candidates: list[Path] = []
    for candidate in root.rglob("*"):
        if len(candidates) >= limit:
            break
        if not candidate.is_file():
            continue
        if older_than_seconds > 0:
            age_seconds = max(0.0, now_ts - candidate.stat().st_mtime)
            if age_seconds < older_than_seconds:
                continue
        candidates.append(candidate.resolve(strict=False))
    return candidates


def _cleanup_paths(
    *,
    paths: Sequence[Path],
    dry_run: bool,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Delete or simulate deleting file paths."""
    deleted: list[str] = []
    skipped: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            skipped.append({"path": str(path), "reason": "not_found"})
            continue
        if not path.is_file():
            skipped.append({"path": str(path), "reason": "not_a_file"})
            continue
        if dry_run:
            deleted.append(str(path))
            continue
        try:
            path.unlink()
            deleted.append(str(path))
        except OSError as exc:
            skipped.append({"path": str(path), "reason": str(exc)})
    return deleted, skipped


def _lookup_blob_row(
    connection: Any,
    *,
    blob_id: str | None,
    binary_hash: str | None,
) -> dict[str, Any] | None:
    """Lookup one blob row by id or hash."""
    if blob_id is not None:
        row = connection.execute(
            _FETCH_BLOB_BY_ID_SQL,
            (WORKSPACE_ID, blob_id),
        ).fetchone()
        mapped = _row_to_dict(row, _BLOB_LOOKUP_COLUMNS)
        if (
            mapped is not None
            and binary_hash is not None
            and mapped.get("binary_hash") != binary_hash
        ):
            return None
        return mapped

    assert binary_hash is not None
    row = connection.execute(
        _FETCH_BLOB_BY_HASH_SQL,
        (WORKSPACE_ID, binary_hash),
    ).fetchone()
    return _row_to_dict(row, _BLOB_LOOKUP_COLUMNS)


def _detect_mime_with_python_magic(path: Path) -> str | None:
    """Detect MIME type using python-magic when available."""
    try:
        magic_module = importlib.import_module("magic")
    except Exception:
        return None

    detected_raw: object | None = None
    from_file = getattr(magic_module, "from_file", None)
    if callable(from_file):
        try:
            detected_raw = from_file(str(path), mime=True)
        except Exception:
            detected_raw = None
    if detected_raw is None:
        magic_cls = getattr(magic_module, "Magic", None)
        if callable(magic_cls):
            try:
                detector = magic_cls(mime=True)
                detected_raw = detector.from_file(str(path))
            except Exception:
                detected_raw = None
    if not isinstance(detected_raw, str) or not detected_raw.strip():
        return None
    detected = normalize_mime(detected_raw)
    if detected == "application/octet-stream":
        return None
    return detected


def _extension_from_mime(mime: str | None) -> str | None:
    """Resolve extension from MIME with stable overrides first."""
    if not isinstance(mime, str) or not mime.strip():
        return None
    normalized = normalize_mime(mime)
    if normalized == "application/octet-stream":
        return None
    override = _MIME_EXTENSION_OVERRIDES.get(normalized)
    if override is not None:
        return override
    guessed = mimetypes.guess_extension(normalized, strict=False)
    if guessed == ".jpe":
        return ".jpg"
    return guessed


def _resolve_output_extension(
    *,
    explicit_extension: str | None,
    filename: str | None,
    source_path: Path,
    stored_mime: str | None,
) -> tuple[str, str, str | None]:
    """Resolve target extension and provenance."""
    if explicit_extension is not None:
        return explicit_extension, "explicit_extension", None

    if filename is not None:
        suffix = Path(filename).suffix.strip().lower()
        if suffix:
            return suffix, "filename", None

    detected_mime = _detect_mime_with_python_magic(source_path)
    detected_extension = _extension_from_mime(detected_mime)
    if detected_extension is not None:
        return detected_extension, "python_magic", detected_mime

    mime_extension = _extension_from_mime(stored_mime)
    if mime_extension is not None:
        return mime_extension, "mime", detected_mime

    return ".bin", "default_bin", detected_mime


async def handle_artifact_blob_list(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle ``artifact(action="blob_list")`` requests."""
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.blob_list")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    limit, limit_err = _resolve_blob_list_limit(arguments.get("limit"))
    if limit_err is not None:
        return limit_err

    with ctx.db_pool.connection() as connection:
        artifact_ids, scope, resolve_err = _resolve_blob_list_artifacts(
            ctx,
            connection,
            arguments,
            session_id=session_id,
        )
        if resolve_err is not None:
            return resolve_err
        assert artifact_ids is not None

        rows = connection.execute(
            _LIST_BLOBS_FOR_ARTIFACTS_SQL,
            (WORKSPACE_ID, artifact_ids),
        ).fetchall()
        mapped_rows = [
            mapped
            for row in rows
            if (mapped := _row_to_dict(row, _BLOB_LIST_COLUMNS)) is not None
        ]
        blobs, total, truncated = _build_blob_list_entries(
            mapped_rows,
            limit=limit,
        )
        if session_id:
            touched = ctx._safe_touch_for_retrieval_many(
                connection,
                session_id=session_id,
                artifact_ids=artifact_ids,
            )
            if touched:
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()

    return {
        "action": "blob_list",
        "scope": scope,
        "artifact_ids": artifact_ids,
        "limit": limit,
        "blob_count": len(blobs),
        "blob_count_total": total,
        "truncated": truncated,
        "blobs": blobs,
    }


async def handle_artifact_blob_materialize(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle ``artifact(action="blob_materialize")`` requests."""
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.blob_materialize")

    blob_id = (
        str(arguments["blob_id"]).strip()
        if _is_non_empty_string(arguments.get("blob_id"))
        else None
    )
    binary_hash = (
        str(arguments["binary_hash"]).strip()
        if _is_non_empty_string(arguments.get("binary_hash"))
        else None
    )
    if blob_id is None and binary_hash is None:
        return gateway_error(
            "INVALID_ARGUMENT",
            "blob_id or binary_hash is required for action=blob_materialize",
        )

    overwrite = arguments.get("overwrite", False)
    if not isinstance(overwrite, bool):
        return gateway_error(
            "INVALID_ARGUMENT",
            "overwrite must be a boolean when provided",
        )

    if_exists = arguments.get("if_exists", "reuse")
    if not isinstance(if_exists, str) or if_exists not in {
        "fail",
        "overwrite",
        "reuse",
    }:
        return gateway_error(
            "INVALID_ARGUMENT",
            "if_exists must be one of: reuse, overwrite, fail",
        )
    if overwrite:
        if_exists = "overwrite"

    materialize_mode, mode_err = _resolve_materialize_mode(
        arguments.get("materialize_mode")
    )
    if mode_err is not None:
        return mode_err

    max_bytes = arguments.get("max_bytes")
    if max_bytes is not None and (
        not isinstance(max_bytes, int) or max_bytes < 1
    ):
        return gateway_error(
            "INVALID_ARGUMENT",
            "max_bytes must be a positive integer when provided",
        )

    filename, filename_err = _parse_filename(arguments.get("filename"))
    if filename_err is not None:
        return filename_err
    explicit_extension, extension_err = _parse_extension(
        arguments.get("extension")
    )
    if extension_err is not None:
        return extension_err
    if (
        filename is not None
        and explicit_extension is not None
        and Path(filename).suffix
        and Path(filename).suffix.lower() != explicit_extension
    ):
        return gateway_error(
            "INVALID_ARGUMENT",
            "filename suffix and extension do not match",
        )

    destination_dir, destination_err = _resolve_destination_dir(
        ctx, arguments.get("destination_dir")
    )
    if destination_err is not None:
        return destination_err
    assert destination_dir is not None

    with ctx.db_pool.connection() as connection:
        blob_row = _lookup_blob_row(
            connection,
            blob_id=blob_id,
            binary_hash=binary_hash,
        )
    if blob_row is None:
        return gateway_error("NOT_FOUND", "blob not found")

    resolved_blob_id = blob_row.get("blob_id")
    resolved_binary_hash = blob_row.get("binary_hash")
    byte_count = blob_row.get("byte_count")
    if not isinstance(resolved_blob_id, str) or not isinstance(
        resolved_binary_hash, str
    ):
        return gateway_error("INTERNAL", "blob metadata is corrupted")
    if not isinstance(byte_count, int) or byte_count < 0:
        return gateway_error("INTERNAL", "blob byte_count is invalid")
    if max_bytes is not None and byte_count > max_bytes:
        return gateway_error(
            "RESOURCE_EXHAUSTED",
            "blob exceeds max_bytes limit",
            details={
                "blob_id": resolved_blob_id,
                "byte_count": byte_count,
                "max_bytes": max_bytes,
            },
        )

    source_path: Path | None = None
    raw_fs_path = blob_row.get("fs_path")
    if isinstance(raw_fs_path, str) and raw_fs_path:
        candidate = Path(raw_fs_path).expanduser()
        if candidate.exists():
            source_path = candidate
    if source_path is None and ctx.blob_store is not None:
        candidate = ctx.blob_store.path_for_hash(resolved_binary_hash)
        if candidate.exists():
            source_path = candidate
    if source_path is None:
        return gateway_error(
            "NOT_FOUND",
            "blob bytes not found on disk",
            details={"blob_id": resolved_blob_id},
        )

    extension, resolved_from, detected_mime = _resolve_output_extension(
        explicit_extension=explicit_extension,
        filename=filename,
        source_path=source_path,
        stored_mime=blob_row.get("mime")
        if isinstance(blob_row.get("mime"), str)
        else None,
    )

    if filename is None:
        output_filename = f"{resolved_blob_id}{extension}"
    elif Path(filename).suffix:
        output_filename = filename
    else:
        output_filename = f"{filename}{extension}"

    destination_dir.mkdir(parents=True, exist_ok=True)
    target_path = (destination_dir / output_filename).resolve(strict=False)
    if not _path_is_within(target_path, destination_dir):
        return gateway_error(
            "INVALID_ARGUMENT",
            "resolved output path escaped destination_dir",
        )

    source_resolved = source_path.resolve()
    target_exists = target_path.exists()
    materialized = False
    materialize_mode_used = "reuse"
    if source_resolved != target_path:
        if target_exists and if_exists == "fail":
            return gateway_error(
                "INVALID_ARGUMENT",
                "target path already exists and if_exists=fail",
                details={"path": str(target_path)},
            )
        if if_exists == "overwrite" and target_exists:
            target_path.unlink()
            target_exists = False
        if not target_exists:
            if materialize_mode == "copy":
                shutil.copyfile(source_path, target_path)
                materialize_mode_used = "copy"
            elif materialize_mode == "hardlink":
                try:
                    os.link(source_path, target_path)
                except OSError as exc:
                    return gateway_error(
                        "INVALID_ARGUMENT",
                        "failed to create hardlink during materialization",
                        details={"error": str(exc)},
                    )
                materialize_mode_used = "hardlink"
            else:
                try:
                    os.link(source_path, target_path)
                    materialize_mode_used = "hardlink"
                except OSError:
                    shutil.copyfile(source_path, target_path)
                    materialize_mode_used = "copy"
            materialized = True
    elif target_exists and if_exists == "fail":
        return gateway_error(
            "INVALID_ARGUMENT",
            "target path already exists and if_exists=fail",
            details={"path": str(target_path)},
        )
    elif source_resolved == target_path:
        materialize_mode_used = "source_path"

    return {
        "action": "blob_materialize",
        "blob_id": resolved_blob_id,
        "binary_hash": resolved_binary_hash,
        "sha256": resolved_binary_hash,
        "mime": blob_row.get("mime"),
        "byte_count": byte_count,
        "path": str(target_path),
        "uri": _blob_uri(resolved_blob_id),
        "if_exists": if_exists,
        "materialize_mode": materialize_mode,
        "materialize_mode_used": materialize_mode_used,
        "materialized": materialized,
        "resolved_extension": extension,
        "resolved_from": resolved_from,
        "detected_mime": detected_mime,
    }


async def handle_artifact_blob_cleanup(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle ``artifact(action="blob_cleanup")`` requests."""
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.blob_cleanup")

    dry_run = arguments.get("dry_run", False)
    if not isinstance(dry_run, bool):
        return gateway_error(
            "INVALID_ARGUMENT",
            "dry_run must be a boolean when provided",
        )

    older_than_seconds_raw = arguments.get("older_than_seconds", 0)
    if not isinstance(older_than_seconds_raw, int) or older_than_seconds_raw < 0:
        return gateway_error(
            "INVALID_ARGUMENT",
            "older_than_seconds must be a non-negative integer",
        )
    older_than_seconds = older_than_seconds_raw

    limit, limit_err = _resolve_blob_cleanup_limit(arguments.get("limit"))
    if limit_err is not None:
        return limit_err

    explicit_paths, explicit_err = _resolve_cleanup_paths(ctx, arguments)
    if explicit_err is not None:
        return explicit_err

    now_ts = time.time()
    mode: str
    root: Path | None = None
    candidates: list[Path]
    if explicit_paths:
        mode = "paths"
        candidates = explicit_paths[:limit]
    else:
        destination_dir, destination_err = _resolve_destination_dir(
            ctx, arguments.get("destination_dir")
        )
        if destination_err is not None:
            return destination_err
        assert destination_dir is not None
        mode = "sweep"
        root = destination_dir
        candidates = _collect_cleanup_candidates(
            root=destination_dir,
            older_than_seconds=older_than_seconds,
            limit=limit,
            now_ts=now_ts,
        )

    deleted_paths, skipped = _cleanup_paths(paths=candidates, dry_run=dry_run)
    return {
        "action": "blob_cleanup",
        "mode": mode,
        "dry_run": dry_run,
        "older_than_seconds": older_than_seconds,
        "limit": limit,
        "destination_dir": str(root) if root is not None else None,
        "matched_count": len(candidates),
        "deleted_count": 0 if dry_run else len(deleted_paths),
        "deleted_paths": [] if dry_run else deleted_paths,
        "would_delete_count": len(deleted_paths) if dry_run else 0,
        "would_delete_paths": deleted_paths if dry_run else [],
        "skipped_count": len(skipped),
        "skipped": skipped,
    }


async def handle_artifact_blob_manifest(
    ctx: GatewayServer,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Handle ``artifact(action="blob_manifest")`` requests."""
    if ctx.db_pool is None:
        return ctx._not_implemented("artifact.blob_manifest")

    raw_ctx = arguments.get("_gateway_context")
    session_id = str(raw_ctx["session_id"]) if isinstance(raw_ctx, dict) else ""
    limit, limit_err = _resolve_blob_list_limit(arguments.get("limit"))
    if limit_err is not None:
        return limit_err

    filename, filename_err = _parse_filename(arguments.get("filename"))
    if filename_err is not None:
        return filename_err
    manifest_format, format_err = _resolve_manifest_format(
        arguments.get("format")
    )
    if format_err is not None:
        return format_err
    if_exists = arguments.get("if_exists", "overwrite")
    if not isinstance(if_exists, str) or if_exists not in {
        "fail",
        "overwrite",
        "reuse",
    }:
        return gateway_error(
            "INVALID_ARGUMENT",
            "if_exists must be one of: reuse, overwrite, fail",
        )
    destination_dir, destination_err = _resolve_manifest_destination_dir(
        ctx, arguments.get("destination_dir")
    )
    if destination_err is not None:
        return destination_err
    assert destination_dir is not None

    with ctx.db_pool.connection() as connection:
        artifact_ids, scope, resolve_err = _resolve_blob_list_artifacts(
            ctx,
            connection,
            arguments,
            session_id=session_id,
        )
        if resolve_err is not None:
            return resolve_err
        assert artifact_ids is not None

        rows = connection.execute(
            _LIST_BLOBS_FOR_ARTIFACTS_SQL,
            (WORKSPACE_ID, artifact_ids),
        ).fetchall()
        mapped_rows = [
            mapped
            for row in rows
            if (mapped := _row_to_dict(row, _BLOB_LIST_COLUMNS)) is not None
        ]
        blobs, total, truncated = _build_blob_list_entries(
            mapped_rows,
            limit=limit,
        )
        if session_id:
            touched = ctx._safe_touch_for_retrieval_many(
                connection,
                session_id=session_id,
                artifact_ids=artifact_ids,
            )
            if touched:
                commit = getattr(connection, "commit", None)
                if callable(commit):
                    commit()

    output_filename = _resolve_manifest_filename(
        filename=filename,
        manifest_format=manifest_format,
    )
    destination_dir.mkdir(parents=True, exist_ok=True)
    target_path = (destination_dir / output_filename).resolve(strict=False)
    if not _path_is_within(target_path, destination_dir):
        return gateway_error(
            "INVALID_ARGUMENT",
            "resolved output path escaped destination_dir",
        )
    if target_path.exists() and if_exists == "fail":
        return gateway_error(
            "INVALID_ARGUMENT",
            "target path already exists and if_exists=fail",
            details={"path": str(target_path)},
        )

    written = False
    if target_path.exists() and if_exists == "reuse":
        written = False
    else:
        if manifest_format == "csv":
            _materialize_manifest_csv(target_path, blobs)
        else:
            _materialize_manifest_json(
                target_path,
                {
                    "action": "blob_manifest",
                    "scope": scope,
                    "artifact_ids": artifact_ids,
                    "blob_count": len(blobs),
                    "blob_count_total": total,
                    "truncated": truncated,
                    "blobs": blobs,
                },
            )
        written = True

    return {
        "action": "blob_manifest",
        "scope": scope,
        "artifact_ids": artifact_ids,
        "format": manifest_format,
        "if_exists": if_exists,
        "written": written,
        "path": str(target_path),
        "blob_count": len(blobs),
        "blob_count_total": total,
        "truncated": truncated,
    }


__all__ = [
    "handle_artifact_blob_cleanup",
    "handle_artifact_blob_list",
    "handle_artifact_blob_manifest",
    "handle_artifact_blob_materialize",
]
