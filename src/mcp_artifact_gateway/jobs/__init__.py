"""Background jobs for pruning and cleanup."""

from mcp_artifact_gateway.jobs.hard_delete import HardDeleteResult, run_hard_delete_batch
from mcp_artifact_gateway.jobs.quota import (
    QuotaBreaches,
    QuotaEnforcementResult,
    StorageUsage,
    enforce_quota,
    query_storage_usage,
)
from mcp_artifact_gateway.jobs.reconcile_fs import ReconcileResult, run_reconcile
from mcp_artifact_gateway.jobs.soft_delete import (
    SoftDeleteResult,
    run_soft_delete_expired,
    run_soft_delete_unreferenced,
)

__all__ = [
    "HardDeleteResult",
    "QuotaBreaches",
    "QuotaEnforcementResult",
    "ReconcileResult",
    "SoftDeleteResult",
    "StorageUsage",
    "enforce_quota",
    "query_storage_usage",
    "run_hard_delete_batch",
    "run_reconcile",
    "run_soft_delete_expired",
    "run_soft_delete_unreferenced",
]
