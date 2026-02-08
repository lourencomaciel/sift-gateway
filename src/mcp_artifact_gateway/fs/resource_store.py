"""Resource store for internal copies of resource_ref content.

Resources are stored under a content-addressed directory structure derived from
both the URI and the SHA-256 hash of the content.  This layout provides
predictable paths while avoiding collisions between resources that share a URI
but differ in content.

All writes follow the same atomic pattern used throughout the filesystem layer:
temporary file, fsync, rename.

File I/O is synchronous because the store targets a local filesystem where
system-call latency is negligible.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# URI sanitisation helpers
# ---------------------------------------------------------------------------
_UNSAFE_CHARS = re.compile(r"[^a-zA-Z0-9._\-/]")
_CONSECUTIVE_SLASHES = re.compile(r"/+")
_MAX_SEGMENT_LEN = 128


def _sanitise_uri_to_path(uri: str) -> str:
    """Convert an arbitrary URI string into a safe relative filesystem path.

    The strategy:
    1. Strip the scheme (``https://``, ``file://``, etc.).
    2. Replace unsafe characters with ``_``.
    3. Collapse consecutive slashes.
    4. Truncate individual path segments to avoid filesystem limits.
    5. Strip leading/trailing slashes.
    """
    # Remove scheme
    if "://" in uri:
        uri = uri.split("://", 1)[1]

    safe = _UNSAFE_CHARS.sub("_", uri)
    safe = _CONSECUTIVE_SLASHES.sub("/", safe)
    safe = safe.strip("/")

    # Truncate long segments and reject dot segments to prevent path traversal.
    parts = safe.split("/")
    parts = [p[:_MAX_SEGMENT_LEN] for p in parts if p and p not in (".", "..")]

    if not parts:
        return "_empty"

    return "/".join(parts)


# ---------------------------------------------------------------------------
# ResourceStore
# ---------------------------------------------------------------------------
class ResourceStore:
    """Store for internal copies of resource_ref content.

    Files are organised under *resources_dir* using a combination of the
    content hash (for deduplication) and a sanitised form of the source URI
    (for human readability)::

        <resources_dir>/<content_hash[0:2]>/<content_hash[2:4]>/<sanitised_uri_tail>

    Parameters
    ----------
    resources_dir:
        Root directory under which resource files are persisted.
    """

    def __init__(self, resources_dir: Path) -> None:
        self._resources_dir = resources_dir

    # -- public API ---------------------------------------------------------

    async def store_internal(
        self, uri: str, content: bytes, mime: str | None = None
    ) -> tuple[str, str]:
        """Persist *content* as an internal copy of the resource at *uri*.

        Returns
        -------
        tuple[str, str]
            ``(fs_path_relative, content_hash)`` where *fs_path_relative* is
            the path relative to *resources_dir* and *content_hash* is the
            hex-encoded SHA-256 digest of *content*.
        """
        content_hash = hashlib.sha256(content).hexdigest()

        # Build a human-friendly but collision-safe relative path.
        sanitised = _sanitise_uri_to_path(uri)
        # Use the last component of the sanitised path as a recognisable
        # filename, prefixed by hash-based subdirectories.
        parts = sanitised.rsplit("/", 1)
        if len(parts) == 2:
            filename = parts[1]
        else:
            filename = parts[0]

        relative = (
            Path(content_hash[0:2])
            / content_hash[2:4]
            / f"{content_hash[:16]}_{filename}"
        )
        relative_str = str(relative)

        full_path = self._resources_dir / relative
        # Belt-and-suspenders: reject if resolved path escapes resources_dir.
        if not full_path.resolve().is_relative_to(self._resources_dir.resolve()):
            raise ValueError(
                f"URI sanitisation produced a path outside resources_dir: {uri!r}"
            )
        if full_path.exists():
            # Already stored — nothing to do.
            return relative_str, content_hash

        # Atomic write: temp file -> fsync -> rename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: str | None = None
        try:
            tmp_fd = tempfile.NamedTemporaryFile(
                dir=full_path.parent,
                delete=False,
                prefix=".res_",
                suffix=".tmp",
            )
            tmp_path = tmp_fd.name
            tmp_fd.write(content)
            tmp_fd.flush()
            os.fsync(tmp_fd.fileno())
            tmp_fd.close()
            os.rename(tmp_path, full_path)
            tmp_path = None
        except BaseException:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise

        return relative_str, content_hash

    def resolve_path(self, relative_path: str) -> Path:
        """Return the absolute path for a previously stored resource.

        Parameters
        ----------
        relative_path:
            The relative path as returned by :meth:`store_internal`.
        """
        resolved = (self._resources_dir / relative_path).resolve()
        if not resolved.is_relative_to(self._resources_dir.resolve()):
            raise ValueError(
                f"Relative path escapes resources_dir: {relative_path!r}"
            )
        return resolved
