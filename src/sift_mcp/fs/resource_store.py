"""Store resource payloads with internal or external durability.

Manages content-addressed file storage for MCP resource
payloads and tracks metadata via the ``ResourceRef`` frozen
dataclass.  Supports both ``internal`` durability (files
persisted locally) and ``external_ref`` durability (URI-only
references to externally-hosted content).
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import os
from pathlib import Path
import tempfile

from sift_mcp.fs.blob_store import normalize_mime
from sift_mcp.util.hashing import sha256_hex


@dataclass(frozen=True)
class ResourceRef:
    """Metadata reference for a stored or externally-hosted resource.

    Attributes:
        uri: Canonical URI for the resource (file URI for
            internal, original URL for external_ref).
        mime: Normalized MIME type.
        name: Optional human-readable name.
        durability: Storage mode (``internal`` or
            ``external_ref``).
        content_hash: SHA-256 prefixed with ``sha256:``, or
            None if unavailable.
        byte_count: Size of the resource payload in bytes.
        fs_path: Local filesystem path for internal resources,
            or None for external references.
    """

    uri: str
    mime: str
    name: str | None
    durability: str
    content_hash: str | None
    byte_count: int
    fs_path: str | None


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Write payload to path atomically via temp file and rename.

    Args:
        path: Destination file path.
        payload: Bytes to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=".tmp-resource-",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file by streaming chunks.

    Args:
        path: Path to the file to hash.

    Returns:
        Hex-encoded SHA-256 digest string.
    """
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1_048_576)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class ResourceStore:
    """Content-addressed store for MCP resource payloads.

    Writes files to a two-level hex prefix directory tree and
    verifies integrity on deduplication, similar to BlobStore.

    Attributes:
        resources_dir: Root directory for resource storage.
    """

    def __init__(self, resources_dir: Path) -> None:
        """Initialize the resource store.

        Args:
            resources_dir: Root directory for resource storage.
        """
        self.resources_dir = resources_dir

    def _path_for_hash(self, content_hash: str) -> Path:
        """Compute the filesystem path for a content hash.

        Args:
            content_hash: SHA-256 hex digest of the resource.

        Returns:
            Path under the two-level hex prefix directory tree.
        """
        return (
            self.resources_dir
            / content_hash[:2]
            / content_hash[2:4]
            / content_hash
        )

    def put_bytes(
        self,
        payload: bytes,
        *,
        mime: str | None = None,
        name: str | None = None,
        durability: str = "internal",
        source_uri: str | None = None,
    ) -> ResourceRef:
        """Store a resource payload with the given durability mode.

        Args:
            payload: Raw bytes to store.
            mime: Optional MIME type (normalized internally).
            name: Optional human-readable resource name.
            durability: Storage mode, either ``internal`` or
                ``external_ref``.
            source_uri: Required URI when durability is
                ``external_ref``.

        Returns:
            A ResourceRef describing the stored or referenced
            resource.

        Raises:
            ValueError: If durability is unsupported, external_ref
                is missing source_uri, or integrity check fails.
        """
        if durability not in {"internal", "external_ref"}:
            msg = f"unsupported durability: {durability}"
            raise ValueError(msg)

        normalized_mime = normalize_mime(mime)
        if durability == "external_ref":
            if not isinstance(source_uri, str) or not source_uri.strip():
                msg = "external_ref durability requires non-empty source_uri"
                raise ValueError(msg)
            content_hash = sha256_hex(payload)
            return ResourceRef(
                uri=source_uri.strip(),
                mime=normalized_mime,
                name=name,
                durability=durability,
                content_hash=f"sha256:{content_hash}",
                byte_count=len(payload),
                fs_path=None,
            )

        content_hash = sha256_hex(payload)
        path = self._path_for_hash(content_hash)
        if path.exists():
            actual_size = path.stat().st_size
            if actual_size != len(payload):
                msg = f"existing resource size mismatch for {content_hash}"
                raise ValueError(msg)
            existing_hash = _sha256_file(path)
            if existing_hash != content_hash:
                msg = (
                    "existing resource content hash"
                    f" mismatch for {content_hash}"
                )
                raise ValueError(msg)
        else:
            _atomic_write_bytes(path, payload)

        return ResourceRef(
            uri=path.resolve().as_uri(),
            mime=normalized_mime,
            name=name,
            durability=durability,
            content_hash=f"sha256:{content_hash}",
            byte_count=len(payload),
            fs_path=str(path),
        )
