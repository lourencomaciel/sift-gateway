"""Quota enforcement: check storage usage and trigger prune when over cap."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.db.protocols import increment_metric
from mcp_artifact_gateway.db.repos.quota_repo import query_storage_usage
from mcp_artifact_gateway.jobs.hard_delete import HardDeleteResult, run_hard_delete_batch
from mcp_artifact_gateway.jobs.soft_delete import SoftDeleteResult, run_soft_delete_unreferenced
from mcp_artifact_gateway.obs.logging import LogEvents, get_logger


@dataclass(frozen=True)
class QuotaEnforcementResult:
    """Result of a quota enforcement check.

    ``usage_bytes_before`` is the storage total measured *before* any
    enforcement deletions ran.  Subtract ``bytes_reclaimed`` to estimate
    post-enforcement usage.
    """
    over_quota: bool
    usage_bytes_before: int
    cap_bytes: int
    soft_deleted: int
    hard_deleted: int
    bytes_reclaimed: int


_DEFAULT_UNREFERENCED_DAYS = 7
_DEFAULT_HARD_DELETE_GRACE_DAYS = 1
_DEFAULT_SOFT_BATCH = 100
_DEFAULT_HARD_BATCH = 50


def check_and_enforce_quota(
    connection: Any,
    *,
    max_total_storage_bytes: int,
    metrics: Any | None = None,
    logger: Any | None = None,
    unreferenced_days: int = _DEFAULT_UNREFERENCED_DAYS,
    hard_delete_grace_days: int = _DEFAULT_HARD_DELETE_GRACE_DAYS,
    soft_batch_size: int = _DEFAULT_SOFT_BATCH,
    hard_batch_size: int = _DEFAULT_HARD_BATCH,
) -> QuotaEnforcementResult:
    """Check current storage usage and prune if over quota.

    This is a best-effort operation: failures are logged but not raised.
    Callers should wrap in try/except for safety.

    Steps:
    1. Query total storage usage.
    2. If under cap, return immediately.
    3. If over cap, soft-delete oldest unreferenced artifacts.
    4. Hard-delete artifacts past grace period.
    5. Return enforcement result.
    """
    log = logger or get_logger(component="jobs.quota")

    usage = query_storage_usage(connection)
    if usage.total_bytes <= max_total_storage_bytes:
        return QuotaEnforcementResult(
            over_quota=False,
            usage_bytes_before=usage.total_bytes,
            cap_bytes=max_total_storage_bytes,
            soft_deleted=0,
            hard_deleted=0,
            bytes_reclaimed=0,
        )

    increment_metric(metrics, "quota_breaches")
    log.warning(
        LogEvents.QUOTA_BREACH,
        usage_bytes=usage.total_bytes,
        cap_bytes=max_total_storage_bytes,
        artifact_count=usage.artifact_count,
    )

    # Phase 1: Soft-delete unreferenced artifacts
    now = dt.datetime.now(dt.timezone.utc)
    threshold = now - dt.timedelta(days=unreferenced_days)
    threshold_str = threshold.isoformat()

    soft_result: SoftDeleteResult | None = None
    try:
        soft_result = run_soft_delete_unreferenced(
            connection,
            threshold_timestamp=threshold_str,
            batch_size=soft_batch_size,
            metrics=metrics,
            logger=log,
        )
    except Exception:
        log.warning("quota soft-delete failed", exc_info=True)

    # Phase 2: Hard-delete artifacts past grace period
    grace_threshold = now - dt.timedelta(days=hard_delete_grace_days)
    grace_str = grace_threshold.isoformat()

    hard_result: HardDeleteResult | None = None
    try:
        hard_result = run_hard_delete_batch(
            connection,
            grace_period_timestamp=grace_str,
            batch_size=hard_batch_size,
            remove_fs_blobs=True,
            metrics=metrics,
            logger=log,
        )
    except Exception:
        log.warning("quota hard-delete failed", exc_info=True)

    soft_deleted = soft_result.deleted_count if soft_result else 0
    hard_deleted = hard_result.artifacts_deleted if hard_result else 0
    bytes_reclaimed = hard_result.bytes_reclaimed if hard_result else 0

    increment_metric(metrics, "quota_enforcements")
    log.info(
        LogEvents.QUOTA_ENFORCED,
        usage_bytes_before=usage.total_bytes,
        cap_bytes=max_total_storage_bytes,
        soft_deleted=soft_deleted,
        hard_deleted=hard_deleted,
        bytes_reclaimed=bytes_reclaimed,
    )

    return QuotaEnforcementResult(
        over_quota=True,
        usage_bytes_before=usage.total_bytes,
        cap_bytes=max_total_storage_bytes,
        soft_deleted=soft_deleted,
        hard_deleted=hard_deleted,
        bytes_reclaimed=bytes_reclaimed,
    )
