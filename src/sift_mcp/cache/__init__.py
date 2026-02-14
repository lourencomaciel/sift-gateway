"""Re-export cache reuse and advisory lock helpers."""

from sift_mcp.cache.reuse import (
    ReuseResult,
    check_reuse_candidate,
)
from sift_mcp.util.hashing import advisory_lock_keys

__all__ = ["ReuseResult", "advisory_lock_keys", "check_reuse_candidate"]
