"""Content-addressed binary blob store."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from mcp_artifact_gateway.constants import BLOB_ID_PREFIX
from mcp_artifact_gateway.util.hashing import sha256_hex

_MIME_ALIASES = {
    "image/jpg": "image/jpeg",
    "application/x-json": "application/json",
}


def normalize_mime(mime: str | None) -> str:
    """Normalize MIME type by lowercasing and dropping params."""
    if not mime:
        return "application/octet-stream"
    base = mime.split(";", 1)[0].strip().lower()
    return _MIME_ALIASES.get(base, base or "application/octet-stream")


@dataclass(frozen=True)
class BinaryRef:
    blob_id: str
    binary_hash: str
    mime: str
    byte_count: int
    fs_path: str
    probe_head_hash: str | None
    probe_tail_hash: str | None
    probe_bytes: int


def _probe_hashes(data: bytes, probe_bytes: int) -> tuple[str | None, str | None]:
    if probe_bytes <= 0 or not data:
        return None, None
    head = data[:probe_bytes]
    tail = data[-probe_bytes:]
    return sha256_hex(head), sha256_hex(tail)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
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
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1_048_576)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class BlobStore:
    """Filesystem-backed content-addressed store for binary payloads."""

    def __init__(self, blobs_bin_dir: Path, probe_bytes: int = 65_536) -> None:
        self.blobs_bin_dir = blobs_bin_dir
        self.probe_bytes = probe_bytes

    def path_for_hash(self, binary_hash: str) -> Path:
        return self.blobs_bin_dir / binary_hash[:2] / binary_hash[2:4] / binary_hash

    def put_bytes(self, payload: bytes, mime: str | None = None) -> BinaryRef:
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
                    f"existing blob content hash mismatch for {binary_hash}: "
                    f"found {existing_hash}"
                )
                raise ValueError(msg)
        else:
            _atomic_write_bytes(path, payload)

        probe_head_hash, probe_tail_hash = _probe_hashes(payload, self.probe_bytes)
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
        return self.path_for_hash(binary_hash).open("rb")
