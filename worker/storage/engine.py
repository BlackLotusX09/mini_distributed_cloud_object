"""Crash-safe, transport-agnostic chunk store.

Knows only opaque chunk IDs and raw bytes. No HTTP, no objects, no replication.

On-disk layout (all under DATA_DIR/chunks/<first-2-chars-of-id>/):
    <id>.chunk      payload
    <id>.sha256     hex SHA-256 of the payload (the commit marker)
    .<id>.tmp       staging file for the payload
    .<id>.sha256.tmp staging file for the checksum

A chunk exists only if BOTH <id>.chunk and a valid <id>.sha256 exist. The
checksum is written last, so a crash at any point leaves either nothing
visible or a payload without a checksum (treated as missing).
"""
import hashlib
import hmac
import os
import re
import threading
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

MAX_CHUNK_SIZE = 1_048_576  # 1 MiB

_ID_RE = re.compile(r"[A-Za-z0-9_\-]{1,128}")
_HEX64_RE = re.compile(r"[0-9a-f]{64}")
_LOCK_STRIPES = 64


# ---------------------------------------------------------------- errors
class StorageError(Exception):
    """Base class for storage errors."""


class InvalidChunkIDException(StorageError):
    pass


class NotFoundError(StorageError):
    pass


class ChunkCorruptionError(StorageError):
    pass


class ChecksumMismatchError(StorageError):
    pass


class ChunkTooLargeError(StorageError):
    pass


class ChunkConflictError(StorageError):
    """A chunk with this ID already exists with different content."""


# ----------------------------------------------------------------- types
class WriteStatus(str, Enum):
    CREATED = "CREATED"
    ALREADY_EXISTS = "ALREADY_EXISTS"


@dataclass(frozen=True)
class WriteResult:
    status: WriteStatus
    chunk_id: str
    size: int
    checksum: str


@dataclass(frozen=True)
class ChunkMeta:
    chunk_id: str
    size: int
    checksum: str


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Storage:
    def __init__(self, data_dir: str, max_chunk_size: int = MAX_CHUNK_SIZE):
        self.data_dir = data_dir
        self.root = os.path.join(data_dir, "chunks")
        self.max_chunk_size = max_chunk_size
        self._locks = [threading.Lock() for _ in range(_LOCK_STRIPES)]
        os.makedirs(self.root, exist_ok=True)
        self.recovered_tmp_files = self.recover()

    # ------------------------------------------------------------ helpers
    @staticmethod
    def validate_id(chunk_id) -> None:
        if not isinstance(chunk_id, str) or not _ID_RE.fullmatch(chunk_id):
            raise InvalidChunkIDException(
                "chunk_id must match [A-Za-z0-9_-]{1,128}"
            )

    def _lock(self, chunk_id: str) -> threading.Lock:
        return self._locks[hash(chunk_id) % _LOCK_STRIPES]

    def _paths(self, chunk_id: str):
        d = os.path.join(self.root, chunk_id[:2])
        return d, os.path.join(d, chunk_id + ".chunk"), os.path.join(d, chunk_id + ".sha256")

    @staticmethod
    def _read_sidecar(path: str) -> Optional[str]:
        """Return the recorded checksum, or None if missing/malformed."""
        try:
            with open(path, "r", encoding="ascii") as f:
                value = f.read().strip()
        except (FileNotFoundError, UnicodeDecodeError):
            return None
        return value if _HEX64_RE.fullmatch(value) else None

    @staticmethod
    def _atomic_write(directory: str, name: str, payload: bytes) -> None:
        """tmp file (same dir => same filesystem) -> flush -> fsync -> rename."""
        tmp = os.path.join(directory, "." + name + ".tmp")
        final = os.path.join(directory, name)
        try:
            with open(tmp, "wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, final)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ----------------------------------------------------------- recovery
    def recover(self) -> int:
        """Unlink orphan staging files left by a crash. Returns count removed."""
        removed = 0
        for dirpath, _dirs, files in os.walk(self.data_dir):
            for name in files:
                if name.endswith(".tmp"):
                    try:
                        os.unlink(os.path.join(dirpath, name))
                        removed += 1
                    except FileNotFoundError:
                        pass
        return removed

    # --------------------------------------------------------------- ops
    def write_chunk(self, chunk_id: str, data: bytes, expected_checksum: Optional[str] = None) -> WriteResult:
        self.validate_id(chunk_id)
        if len(data) > self.max_chunk_size:
            raise ChunkTooLargeError(f"chunk exceeds {self.max_chunk_size} bytes")
        digest = sha256_hex(data)
        if expected_checksum is not None:
            if not hmac.compare_digest(digest, expected_checksum.strip().lower()):
                raise ChecksumMismatchError("payload does not match expected checksum")

        directory, chunk_path, sum_path = self._paths(chunk_id)
        with self._lock(chunk_id):
            recorded = self._read_sidecar(sum_path)
            if recorded is not None and os.path.exists(chunk_path):
                if recorded != digest:
                    raise ChunkConflictError("chunk exists with different content")
                if self._data_matches(chunk_path, len(data), digest):
                    return WriteResult(WriteStatus.ALREADY_EXISTS, chunk_id, len(data), digest)
                # same ID/content but the stored copy is damaged: rewrite it below.
            # else: nothing, or an aborted write (payload without valid checksum).

            if not os.path.isdir(directory):
                os.makedirs(directory, exist_ok=True)
                _fsync_dir(self.root)
            self._atomic_write(directory, chunk_id + ".chunk", data)
            self._atomic_write(directory, chunk_id + ".sha256", digest.encode("ascii"))
            _fsync_dir(directory)
        return WriteResult(WriteStatus.CREATED, chunk_id, len(data), digest)

    @staticmethod
    def _data_matches(path: str, size: int, digest: str) -> bool:
        try:
            if os.path.getsize(path) != size:
                return False
            with open(path, "rb") as f:
                return hmac.compare_digest(sha256_hex(f.read()), digest)
        except OSError:
            return False

    def read_chunk(self, chunk_id: str) -> bytes:
        self.validate_id(chunk_id)
        _directory, chunk_path, sum_path = self._paths(chunk_id)
        with self._lock(chunk_id):
            try:
                with open(sum_path, "r", encoding="ascii") as f:
                    recorded = f.read().strip()
                with open(chunk_path, "rb") as f:
                    data = f.read()
            except FileNotFoundError:
                raise NotFoundError(f"chunk {chunk_id} not found") from None
            except UnicodeDecodeError:
                raise ChunkCorruptionError("checksum file unreadable") from None
        if not _HEX64_RE.fullmatch(recorded):
            raise ChunkCorruptionError("checksum file malformed")
        if not hmac.compare_digest(sha256_hex(data), recorded):
            raise ChunkCorruptionError(f"chunk {chunk_id} failed checksum verification")
        return data

    def delete_chunk(self, chunk_id: str) -> None:
        self.validate_id(chunk_id)
        directory, chunk_path, sum_path = self._paths(chunk_id)
        with self._lock(chunk_id):
            # Checksum first: a crash in between leaves a payload with no
            # checksum, which reads as "missing" (never as valid data).
            for p in (sum_path, chunk_path):
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass
            if os.path.isdir(directory):
                _fsync_dir(directory)

    def list_chunks(self) -> List[ChunkMeta]:
        out: List[ChunkMeta] = []
        try:
            shards = list(os.scandir(self.root))
        except FileNotFoundError:
            return out
        for shard in shards:
            if not shard.is_dir():
                continue
            for entry in os.scandir(shard.path):
                name = entry.name
                if name.startswith(".") or not name.endswith(".chunk"):
                    continue
                cid = name[: -len(".chunk")]
                if not _ID_RE.fullmatch(cid):
                    continue
                checksum = self._read_sidecar(os.path.join(shard.path, cid + ".sha256"))
                if checksum is None:
                    continue  # aborted write: not a chunk
                try:
                    size = entry.stat().st_size
                except FileNotFoundError:
                    continue
                out.append(ChunkMeta(cid, size, checksum))
        out.sort(key=lambda m: m.chunk_id)
        return out

    def chunk_count(self) -> int:
        return len(self.list_chunks())

