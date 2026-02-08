"""Content-addressed binary blob store (spec §6).

All writes use an atomic pattern: write to a temporary file in the same
directory, fsync the file descriptor, then ``os.rename`` to the final path.
This guarantees that the blob file either exists in its entirety or not at all,
even on unexpected process termination.

File I/O is synchronous because the store targets a local filesystem where
system-call latency is negligible and the added complexity of
``run_in_executor`` is not warranted.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from mcp_artifact_gateway.constants import BLOB_ID_PREFIX

# ---------------------------------------------------------------------------
# Default MIME alias map — normalises common legacy / vendor types to their
# canonical equivalents.
# ---------------------------------------------------------------------------
_DEFAULT_MIME_ALIASES: dict[str, str] = {
    "application/x-javascript": "application/javascript",
    "text/x-json": "application/json",
    "text/json": "application/json",
    "image/x-png": "image/png",
    "image/x-ms-bmp": "image/bmp",
    "text/x-markdown": "text/markdown",
}


# ---------------------------------------------------------------------------
# BinaryRef — immutable reference to a stored blob
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class BinaryRef:
    """Immutable descriptor returned after a blob is persisted."""

    binary_hash: str
    blob_id: str
    byte_count: int
    mime: str | None
    fs_path: Path
    probe_head_hash: str | None = field(default=None)
    probe_tail_hash: str | None = field(default=None)


# ---------------------------------------------------------------------------
# BlobStore
# ---------------------------------------------------------------------------
class BlobStore:
    """Content-addressed binary blob store.

    Blobs are laid out on disk using a two-level fan-out derived from the
    leading hex characters of the SHA-256 hash::

        <blobs_bin_dir>/<hash[0:2]>/<hash[2:4]>/<hash>

    Parameters
    ----------
    blobs_bin_dir:
        Root directory under which blob shards are stored.
    probe_bytes:
        Number of bytes at the head/tail used for probe hashes (§6.3).
    mime_aliases:
        Optional mapping of non-canonical MIME types to their canonical form.
        Merged on top of the built-in default alias table.
    """

    def __init__(
        self,
        blobs_bin_dir: Path,
        probe_bytes: int = 65_536,
        mime_aliases: dict[str, str] | None = None,
    ) -> None:
        self._blobs_bin_dir = blobs_bin_dir
        self._probe_bytes = probe_bytes
        self._mime_aliases: dict[str, str] = {**_DEFAULT_MIME_ALIASES}
        if mime_aliases:
            self._mime_aliases.update(mime_aliases)

    # -- path helpers -------------------------------------------------------

    def blob_path(self, binary_hash: str) -> Path:
        """Return the on-disk path for a blob identified by *binary_hash*."""
        return (
            self._blobs_bin_dir
            / binary_hash[0:2]
            / binary_hash[2:4]
            / binary_hash
        )

    # -- MIME normalisation -------------------------------------------------

    def _normalise_mime(self, mime: str | None) -> str | None:
        """Lowercase, strip parameters after ``';'``, and apply alias map."""
        if mime is None:
            return None
        # Lowercase and strip parameters (e.g. "; charset=utf-8")
        base = mime.split(";", 1)[0].strip().lower()
        return self._mime_aliases.get(base, base)

    # -- probe hashes -------------------------------------------------------

    def _compute_probe_hashes(
        self, data: bytes
    ) -> tuple[str | None, str | None]:
        """Return ``(head_hash, tail_hash)`` probe hashes for *data*.

        * If the data is shorter than *probe_bytes*, the head hash covers the
          entire content and the tail hash is ``None``.
        * Otherwise, the head hash covers ``data[:probe_bytes]`` and the tail
          hash covers ``data[-probe_bytes:]``.
        """
        if not data:
            return None, None

        if len(data) < self._probe_bytes:
            head_hash = hashlib.sha256(data).hexdigest()
            return head_hash, None

        head_hash = hashlib.sha256(data[: self._probe_bytes]).hexdigest()
        tail_hash = hashlib.sha256(data[-self._probe_bytes :]).hexdigest()
        return head_hash, tail_hash

    # -- main write entry point ---------------------------------------------

    async def put_bytes(
        self, raw_bytes: bytes, mime: str | None = None
    ) -> BinaryRef:
        """Persist *raw_bytes* and return a :class:`BinaryRef`.

        If a blob with the same content hash already exists the file is **not**
        rewritten; instead the existing file's size is verified and the
        reference is returned directly.

        All disk writes are atomic (temp-file + fsync + rename).
        """
        binary_hash = hashlib.sha256(raw_bytes).hexdigest()
        blob_id = BLOB_ID_PREFIX + binary_hash[:32]
        final_path = self.blob_path(binary_hash)
        normalised_mime = self._normalise_mime(mime)
        byte_count = len(raw_bytes)

        if final_path.exists():
            # De-duplication: verify the existing blob is consistent.
            existing_size = final_path.stat().st_size
            if existing_size != byte_count:
                raise ValueError(
                    f"Blob size mismatch for hash {binary_hash}: "
                    f"existing file is {existing_size} bytes but new content "
                    f"is {byte_count} bytes"
                )
            head_hash, tail_hash = self._compute_probe_hashes(raw_bytes)
            return BinaryRef(
                binary_hash=binary_hash,
                blob_id=blob_id,
                byte_count=byte_count,
                mime=normalised_mime,
                fs_path=final_path,
                probe_head_hash=head_hash,
                probe_tail_hash=tail_hash,
            )

        # --- First write: atomic temp + fsync + rename ---------------------
        final_path.parent.mkdir(parents=True, exist_ok=True)

        # Create the temp file in the *same* directory so that os.rename is
        # guaranteed to be atomic (same filesystem).
        fd = -1
        tmp_path: str | None = None
        try:
            tmp_fd = tempfile.NamedTemporaryFile(
                dir=final_path.parent,
                delete=False,
                prefix=".blob_",
                suffix=".tmp",
            )
            tmp_path = tmp_fd.name
            fd = tmp_fd.fileno()
            tmp_fd.write(raw_bytes)
            tmp_fd.flush()
            os.fsync(fd)
            tmp_fd.close()
            fd = -1  # closed by the context above
            os.rename(tmp_path, final_path)
            tmp_path = None  # rename succeeded; nothing to clean up
        except BaseException:
            # Best-effort cleanup of the temp file on failure.
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise

        head_hash, tail_hash = self._compute_probe_hashes(raw_bytes)
        return BinaryRef(
            binary_hash=binary_hash,
            blob_id=blob_id,
            byte_count=byte_count,
            mime=normalised_mime,
            fs_path=final_path,
            probe_head_hash=head_hash,
            probe_tail_hash=tail_hash,
        )

    # -- read helpers -------------------------------------------------------

    def open_stream(self, binary_hash: str) -> IO[bytes]:
        """Return an open binary file handle for reading the blob.

        Raises :class:`FileNotFoundError` if the blob does not exist on disk.
        """
        path = self.blob_path(binary_hash)
        if not path.exists():
            raise FileNotFoundError(
                f"Blob not found for hash {binary_hash}: expected at {path}"
            )
        return open(path, "rb")  # noqa: SIM115 — caller manages the handle

    # -- verification -------------------------------------------------------

    async def verify_blob(
        self, binary_hash: str, expected_byte_count: int
    ) -> bool:
        """Return ``True`` if the blob exists and its size matches."""
        path = self.blob_path(binary_hash)
        if not path.exists():
            return False
        return path.stat().st_size == expected_byte_count
