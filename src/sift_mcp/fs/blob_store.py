"""Provide a content-addressed binary blob store on the filesystem.

Implements atomic writes, SHA-256-based deduplication, and probe
hashing for head/tail integrity checks.  Exports ``BlobStore``
for read/write access and the ``BinaryRef`` frozen dataclass
that records metadata about a stored blob.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import tempfile
from typing import BinaryIO

from sift_mcp.constants import BLOB_ID_PREFIX
from sift_mcp.util.hashing import sha256_hex

_MIME_ALIASES = {
    "image/jpg": "image/jpeg",
    "application/x-json": "application/json",
}


def normalize_mime(mime: str | None) -> str:
    """Normalize MIME type by lowercasing and dropping params.

    Args:
        mime: Raw MIME type string, or None.

    Returns:
        Normalized MIME type with parameters stripped and
        common aliases resolved.
    """
    if not mime:
        return "application/octet-stream"
    base = mime.split(";", 1)[0].strip().lower()
    return _MIME_ALIASES.get(base, base or "application/octet-stream")


@dataclass(frozen=True)
class BinaryRef:
    """Metadata reference for a stored binary blob.

    Returned by ``BlobStore.put_bytes`` after a blob is written
    (or deduplicated).  Contains hashes, size, and optional
    probe hashes for integrity spot-checks.

    Attributes:
        blob_id: Short identifier (``bin_`` prefix + 32 hex).
        binary_hash: Full SHA-256 hex digest of the payload.
        mime: Normalized MIME type of the payload.
        byte_count: Size of the payload in bytes.
        fs_path: Absolute path to the blob file on disk.
        probe_head_hash: SHA-256 of the first *probe_bytes*
            bytes, or None if probing is disabled.
        probe_tail_hash: SHA-256 of the last *probe_bytes*
            bytes, or None if probing is disabled.
        probe_bytes: Number of bytes used for head/tail probes.
    """

    blob_id: str
    binary_hash: str
    mime: str
    byte_count: int
    fs_path: str
    probe_head_hash: str | None
    probe_tail_hash: str | None
    probe_bytes: int


def _probe_hashes(
    data: bytes, probe_bytes: int
) -> tuple[str | None, str | None]:
    """Compute SHA-256 hashes of the head and tail probe regions.

    Args:
        data: Full payload bytes.
        probe_bytes: Number of bytes to hash from each end.

    Returns:
        Tuple of (head_hash, tail_hash), both None if probing
        is disabled or data is empty.
    """
    if probe_bytes <= 0 or not data:
        return None, None
    head = data[:probe_bytes]
    tail = data[-probe_bytes:]
    return sha256_hex(head), sha256_hex(tail)


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
            prefix=".tmp-blob-",
            delete=False,
        ) as handle:
            tmp_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file by streaming chunks.

    Args:
        path: Path to the file to hash.

    Returns:
        Hex-encoded SHA-256 digest string.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1_048_576)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class BlobStore:
    """Filesystem-backed content-addressed store for binary blobs.

    Organises files in a two-level hex prefix directory tree
    (``ab/cd/<hash>``) and verifies integrity on deduplication.

    Attributes:
        blobs_bin_dir: Root directory for blob storage.
        probe_bytes: Number of bytes used for head/tail probes.
    """

    def __init__(self, blobs_bin_dir: Path, probe_bytes: int = 65_536) -> None:
        """Initialize the blob store.

        Args:
            blobs_bin_dir: Root directory for blob file storage.
            probe_bytes: Bytes to use for head/tail integrity
                probes.
        """
        self.blobs_bin_dir = blobs_bin_dir
        self.probe_bytes = probe_bytes

    def path_for_hash(self, binary_hash: str) -> Path:
        """Compute the filesystem path for a given hash.

        Args:
            binary_hash: SHA-256 hex digest of the blob.

        Returns:
            Path under the two-level hex prefix directory tree.
        """
        return (
            self.blobs_bin_dir
            / binary_hash[:2]
            / binary_hash[2:4]
            / binary_hash
        )

    def put_bytes(self, payload: bytes, mime: str | None = None) -> BinaryRef:
        """Store a binary payload, deduplicating by content hash.

        Args:
            payload: Raw bytes to store.
            mime: Optional MIME type (normalized internally).

        Returns:
            A BinaryRef describing the stored blob.

        Raises:
            ValueError: If an existing blob has a size or hash
                mismatch (corruption detected).
        """
        binary_hash = sha256_hex(payload)
        path = self.path_for_hash(binary_hash)

        if path.exists():
            actual_size = path.stat().st_size
            if actual_size != len(payload):
                msg = (
                    f"existing blob size mismatch for {binary_hash}: "
                    f"{actual_size} != {len(payload)}"
                )
                raise ValueError(msg)
            existing_hash = _sha256_file(path)
            if existing_hash != binary_hash:
                msg = (
                    f"existing blob content hash mismatch"
                    f" for {binary_hash}: found"
                    f" {existing_hash}"
                )
                raise ValueError(msg)
        else:
            _atomic_write_bytes(path, payload)

        probe_head_hash, probe_tail_hash = _probe_hashes(
            payload, self.probe_bytes
        )
        return BinaryRef(
            blob_id=f"{BLOB_ID_PREFIX}{binary_hash[:32]}",
            binary_hash=binary_hash,
            mime=normalize_mime(mime),
            byte_count=len(payload),
            fs_path=str(path),
            probe_head_hash=probe_head_hash,
            probe_tail_hash=probe_tail_hash,
            probe_bytes=self.probe_bytes,
        )

    def open_stream(self, binary_hash: str) -> BinaryIO:
        """Open a read-only binary stream for a stored blob.

        Args:
            binary_hash: SHA-256 hex digest identifying the blob.

        Returns:
            A binary file object opened for reading.
        """
        return self.path_for_hash(binary_hash).open("rb")
