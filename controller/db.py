"""Controller metadata layer: pure Python + sqlite3, no HTTP/FastAPI dependencies.

Every call uses its own fresh connection (connections are never shared across
threads). Multi-step writes go through `transaction()` (BEGIN IMMEDIATE).
Timestamps are REAL epoch seconds supplied by the caller (the controller's own
clock), never by workers.
"""
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional

_DB_PATH = os.environ.get("DB_PATH", "/data/controller.db")
_path_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id      TEXT PRIMARY KEY,
    address        TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('ALIVE','DEAD')),
    last_heartbeat REAL NOT NULL,
    dead_since     REAL,
    registered_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    object_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    content_type TEXT,
    checksum     TEXT,
    chunk_count  INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('PENDING','COMMITTED','DELETING')),
    created_at   REAL NOT NULL,
    modified_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL,
    size      INTEGER NOT NULL,
    checksum  TEXT
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id  TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(worker_id),
    state     TEXT NOT NULL CHECK (state IN ('OK','CORRUPT','PENDING_DELETE')),
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS custom_metadata (
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    key       TEXT NOT NULL,
    value     TEXT,
    PRIMARY KEY (object_id, key)
);

CREATE TABLE IF NOT EXISTS scrub_status (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    last_scrub_at  REAL,
    chunks_checked INTEGER NOT NULL DEFAULT 0,
    corrupt_found  INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chunks_object_idx ON chunks(object_id, idx);
CREATE INDEX IF NOT EXISTS idx_replicas_worker       ON replicas(worker_id);
CREATE INDEX IF NOT EXISTS idx_replicas_state        ON replicas(state);
CREATE INDEX IF NOT EXISTS idx_replicas_state_worker ON replicas(state, worker_id);
CREATE INDEX IF NOT EXISTS idx_workers_status        ON workers(status);
CREATE INDEX IF NOT EXISTS idx_objects_status        ON objects(status);
"""


# ------------------------------------------------------------ connections
def connect(path: Optional[str] = None) -> sqlite3.Connection:
    """Open a NEW connection with the required pragmas."""
    path = path or _DB_PATH
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    # isolation_level=None: autocommit; we issue BEGIN/COMMIT ourselves.
    conn = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def connection(path: Optional[str] = None):
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE; COMMIT on clean exit, ROLLBACK on any exception."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def _writer(conn: Optional[sqlite3.Connection]):
    """Use the caller's connection/transaction, or a fresh one in its own transaction."""
    if conn is not None:
        yield conn
        return
    with connection() as c, transaction(c):
        yield c


@contextmanager
def _reader(conn: Optional[sqlite3.Connection]):
    if conn is not None:
        yield conn
        return
    with connection() as c:
        yield c


def init_db(path: Optional[str] = None) -> None:
    """Set the database path (if given) and create tables/indexes if missing."""
    global _DB_PATH
    if path:
        with _path_lock:
            _DB_PATH = path
    with connection() as conn:
        conn.executescript(SCHEMA)


def current_path() -> str:
    return _DB_PATH


# ---------------------------------------------------------------- workers
def upsert_worker(worker_id: str, address: str, now: float, conn=None) -> str:
    """Insert as ALIVE, or refresh an existing worker. Returns 'new' or 'returning'."""
    with _writer(conn) as c:
        row = c.execute("SELECT 1 FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        if row is None:
            c.execute(
                "INSERT INTO workers (worker_id, address, status, last_heartbeat, dead_since, registered_at)"
                " VALUES (?, ?, 'ALIVE', ?, NULL, ?)",
                (worker_id, address, now, now),
            )
            return "new"
        c.execute(
            "UPDATE workers SET address=?, status='ALIVE', dead_since=NULL, last_heartbeat=? WHERE worker_id=?",
            (address, now, worker_id),
        )
        return "returning"


def record_heartbeat(worker_id: str, now: float, conn=None) -> Optional[str]:
    """Update last_heartbeat. Returns the PREVIOUS status, or None if the worker is unknown.

    A DEAD worker becomes ALIVE (dead_since cleared).
    """
    with _writer(conn) as c:
        row = c.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        if row is None:
            return None
        c.execute(
            "UPDATE workers SET last_heartbeat=?, status='ALIVE', dead_since=NULL WHERE worker_id=?",
            (now, worker_id),
        )
        return row["status"]


def get_worker(worker_id: str, conn=None) -> Optional[dict]:
    with _reader(conn) as c:
        row = c.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        return dict(row) if row else None


def list_workers(status: Optional[str] = None, conn=None) -> List[dict]:
    with _reader(conn) as c:
        if status is None:
            rows = c.execute("SELECT * FROM workers ORDER BY worker_id").fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM workers WHERE status=? ORDER BY worker_id", (status,)
            ).fetchall()
        return [dict(r) for r in rows]


def mark_dead(worker_id: str, now: float, stale_before: Optional[float] = None, conn=None) -> bool:
    """ALIVE -> DEAD. Returns True if a transition happened.

    If `stale_before` is given, only transition when last_heartbeat < stale_before
    (guards against a heartbeat that landed after the caller read the row).
    Replicas are deliberately left untouched.
    """
    with _writer(conn) as c:
        cur = c.execute(
            "UPDATE workers SET status='DEAD', dead_since=? "
            "WHERE worker_id=? AND status='ALIVE' AND (? IS NULL OR last_heartbeat < ?)",
            (now, worker_id, stale_before, stale_before),
        )
        return cur.rowcount == 1


def mark_alive(worker_id: str, now: float, conn=None) -> bool:
    """DEAD -> ALIVE. Returns True if a transition happened."""
    with _writer(conn) as c:
        cur = c.execute(
            "UPDATE workers SET status='ALIVE', dead_since=NULL, last_heartbeat=? "
            "WHERE worker_id=? AND status='DEAD'",
            (now, worker_id),
        )
        return cur.rowcount == 1


# ------------------------------------------------- objects / chunks / replicas
def create_object_pending(
    object_id: str,
    name: str,
    size: int,
    chunk_count: int,
    now: float,
    content_type: Optional[str] = None,
    checksum: Optional[str] = None,
    conn=None,
) -> None:
    with _writer(conn) as c:
        c.execute(
            "INSERT INTO objects (object_id, name, size, content_type, checksum, chunk_count,"
            " status, created_at, modified_at) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)",
            (object_id, name, size, content_type, checksum, chunk_count, now, now),
        )


def add_chunk(chunk_id: str, object_id: str, idx: int, size: int,
              checksum: Optional[str] = None, conn=None) -> None:
    with _writer(conn) as c:
        c.execute(
            "INSERT INTO chunks (chunk_id, object_id, idx, size, checksum) VALUES (?, ?, ?, ?, ?)",
            (chunk_id, object_id, idx, size, checksum),
        )


def add_replica(chunk_id: str, worker_id: str, state: str = "OK", conn=None) -> None:
    with _writer(conn) as c:
        c.execute(
            "INSERT INTO replicas (chunk_id, worker_id, state) VALUES (?, ?, ?)",
            (chunk_id, worker_id, state),
        )


def commit_object(object_id: str, now: float, checksum: Optional[str] = None, conn=None) -> bool:
    """PENDING -> COMMITTED. Returns False if the object isn't PENDING."""
    with _writer(conn) as c:
        cur = c.execute(
            "UPDATE objects SET status='COMMITTED', modified_at=?, checksum=COALESCE(?, checksum) "
            "WHERE object_id=? AND status='PENDING'",
            (now, checksum, object_id),
        )
        return cur.rowcount == 1


def get_object(object_id: str, conn=None) -> Optional[dict]:
    """Object row plus ordered `chunks`, each with its `replicas` list."""
    with _reader(conn) as c:
        obj = c.execute("SELECT * FROM objects WHERE object_id=?", (object_id,)).fetchone()
        if obj is None:
            return None
        result = dict(obj)
        chunks = [dict(r) for r in c.execute(
            "SELECT * FROM chunks WHERE object_id=? ORDER BY idx", (object_id,))]
        replicas: Dict[str, list] = {}
        for r in c.execute(
            "SELECT r.chunk_id, r.worker_id, r.state FROM replicas r "
            "JOIN chunks k ON k.chunk_id = r.chunk_id WHERE k.object_id=? "
            "ORDER BY r.worker_id", (object_id,)):
            replicas.setdefault(r["chunk_id"], []).append(
                {"worker_id": r["worker_id"], "state": r["state"]})
        for ch in chunks:
            ch["replicas"] = replicas.get(ch["chunk_id"], [])
        result["chunks"] = chunks
        return result


def delete_object(object_id: str, conn=None) -> bool:
    """Delete the object; chunks, replicas and custom_metadata cascade."""
    with _writer(conn) as c:
        return c.execute("DELETE FROM objects WHERE object_id=?", (object_id,)).rowcount == 1


def count_chunks_per_worker(conn=None) -> Dict[str, int]:
    """{worker_id: number of 'OK' replicas}. Workers with none are absent."""
    with _reader(conn) as c:
        rows = c.execute(
            "SELECT worker_id, COUNT(*) AS n FROM replicas WHERE state='OK' GROUP BY worker_id"
        ).fetchall()
        return {r["worker_id"]: r["n"] for r in rows}



# ====================================================== Day 4/5: data-plane queries
class ObjectNotFoundError(Exception):
    """Object missing, or not in the state the operation requires."""


class MetadataTooLargeError(Exception):
    pass


def create_object_with_metadata(
    object_id: str, name: str, content_type: Optional[str], now: float,
    custom: Optional[dict] = None, conn=None,
) -> None:
    """Step A of upload: PENDING object (+ any custom metadata) in ONE transaction."""
    with _writer(conn) as c:
        create_object_pending(object_id, name, 0, 0, now, content_type, None, conn=c)
        for k, v in (custom or {}).items():
            c.execute("INSERT INTO custom_metadata (object_id, key, value) VALUES (?, ?, ?)",
                      (object_id, k, v))


def persist_chunk(chunk_id: str, object_id: str, idx: int, size: int, checksum: str,
                  worker_ids: List[str], conn=None) -> None:
    """One chunk row + its replica rows, atomically."""
    with _writer(conn) as c:
        add_chunk(chunk_id, object_id, idx, size, checksum, conn=c)
        for wid in worker_ids:
            add_replica(chunk_id, wid, "OK", conn=c)


def finalize_object(object_id: str, size: int, checksum: str, chunk_count: int,
                    now: float, conn=None) -> bool:
    """Step C of upload: PENDING -> COMMITTED with final size/checksum/chunk_count."""
    with _writer(conn) as c:
        cur = c.execute(
            "UPDATE objects SET size=?, checksum=?, chunk_count=?, status='COMMITTED', modified_at=? "
            "WHERE object_id=? AND status='PENDING'",
            (size, checksum, chunk_count, now, object_id),
        )
        return cur.rowcount == 1


def set_object_status(object_id: str, status: str, now: float, conn=None) -> bool:
    with _writer(conn) as c:
        return c.execute("UPDATE objects SET status=?, modified_at=? WHERE object_id=?",
                         (status, now, object_id)).rowcount == 1


def get_object_row(object_id: str, conn=None) -> Optional[dict]:
    with _reader(conn) as c:
        row = c.execute("SELECT * FROM objects WHERE object_id=?", (object_id,)).fetchone()
        return dict(row) if row else None


def get_custom_metadata(object_id: str, conn=None) -> Dict[str, str]:
    with _reader(conn) as c:
        return {r["key"]: r["value"] for r in c.execute(
            "SELECT key, value FROM custom_metadata WHERE object_id=? ORDER BY key", (object_id,))}


def list_alive_with_counts(conn=None) -> List[dict]:
    """ALIVE workers with their current number of OK replicas (for placement)."""
    with _reader(conn) as c:
        rows = c.execute(
            "SELECT w.worker_id, w.address, w.status, COALESCE(k.n, 0) AS chunk_count "
            "FROM workers w LEFT JOIN ("
            "  SELECT worker_id, COUNT(*) AS n FROM replicas WHERE state='OK' GROUP BY worker_id"
            ") k ON k.worker_id = w.worker_id WHERE w.status='ALIVE' ORDER BY w.worker_id"
        ).fetchall()
        return [dict(r) for r in rows]


def get_object_replicas(object_id: str, conn=None) -> List[dict]:
    """Every replica row of an object, with the worker address."""
    with _reader(conn) as c:
        rows = c.execute(
            "SELECT r.chunk_id, r.worker_id, r.state, w.address FROM replicas r "
            "JOIN chunks k ON k.chunk_id = r.chunk_id "
            "JOIN workers w ON w.worker_id = r.worker_id "
            "WHERE k.object_id=? ORDER BY k.idx, r.worker_id", (object_id,)).fetchall()
        return [dict(r) for r in rows]


def count_object_replicas(object_id: str, conn=None) -> int:
    with _reader(conn) as c:
        return c.execute(
            "SELECT COUNT(*) FROM replicas r JOIN chunks k ON k.chunk_id = r.chunk_id "
            "WHERE k.object_id=?", (object_id,)).fetchone()[0]


def set_replica_state(chunk_id: str, worker_id: str, state: str, conn=None) -> bool:
    with _writer(conn) as c:
        return c.execute("UPDATE replicas SET state=? WHERE chunk_id=? AND worker_id=?",
                         (state, chunk_id, worker_id)).rowcount == 1


def apply_replica_cleanup(deleted: List[tuple], failed: List[tuple], conn=None) -> None:
    """After best-effort worker deletes: drop confirmed rows, mark the rest PENDING_DELETE."""
    with _writer(conn) as c:
        for chunk_id, worker_id in deleted:
            c.execute("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?", (chunk_id, worker_id))
        for chunk_id, worker_id in failed:
            c.execute("UPDATE replicas SET state='PENDING_DELETE' WHERE chunk_id=? AND worker_id=?",
                      (chunk_id, worker_id))


def begin_delete(object_id: str, now: float, conn=None):
    """Logical delete in ONE transaction.

    Returns ("not_found"|"busy"|"ok", replicas). On "ok" the object is DELETING and all its
    replica rows are PENDING_DELETE (hidden from list/metadata/download immediately).
    """
    with _writer(conn) as c:
        row = c.execute("SELECT status FROM objects WHERE object_id=?", (object_id,)).fetchone()
        if row is None or row["status"] == "DELETING":
            return "not_found", []
        if row["status"] == "PENDING":
            return "busy", []
        c.execute("UPDATE objects SET status='DELETING', modified_at=? WHERE object_id=?",
                  (now, object_id))
        c.execute(
            "UPDATE replicas SET state='PENDING_DELETE' WHERE chunk_id IN "
            "(SELECT chunk_id FROM chunks WHERE object_id=?)", (object_id,))
        replicas = [dict(r) for r in c.execute(
            "SELECT r.chunk_id, r.worker_id, r.state, w.address FROM replicas r "
            "JOIN chunks k ON k.chunk_id = r.chunk_id JOIN workers w ON w.worker_id = r.worker_id "
            "WHERE k.object_id=? ORDER BY k.idx, r.worker_id", (object_id,))]
        return "ok", replicas


def finish_delete_if_empty(object_id: str, conn=None) -> bool:
    """Remove chunks/metadata/object rows iff no replica rows remain."""
    with _writer(conn) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM replicas r JOIN chunks k ON k.chunk_id = r.chunk_id "
            "WHERE k.object_id=?", (object_id,)).fetchone()[0]
        if n:
            return False
        c.execute("DELETE FROM objects WHERE object_id=?", (object_id,))  # cascades
        return True


def finalize_partial_purge(object_id: str, now: float, conn=None) -> bool:
    """Complete rollback cleanup after best-effort worker deletes.

    If no replica rows remain, deletes the entire object (cascades).
    Otherwise deletes custom_metadata and any chunk rows with zero remaining
    replicas, and marks the object DELETING so it stays invisible until the
    Day-6 reconciler finishes clearing the PENDING_DELETE replicas.
    """
    with _writer(conn) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM replicas r JOIN chunks k ON k.chunk_id = r.chunk_id "
            "WHERE k.object_id=?", (object_id,)).fetchone()[0]
        if n == 0:
            c.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
            return True
        c.execute("DELETE FROM custom_metadata WHERE object_id=?", (object_id,))
        c.execute(
            "DELETE FROM chunks WHERE object_id=? AND chunk_id NOT IN (SELECT chunk_id FROM replicas)",
            (object_id,),
        )
        c.execute("UPDATE objects SET status='DELETING', modified_at=? WHERE object_id=?",
                  (now, object_id))
        return False


# ====================================================== Day 6/7: repair, reconciliation, scrub


def get_chunk_replica_status(conn=None, replication_factor: int = 2) -> dict:
    """Compute replication health of every chunk belonging to COMMITTED objects.

    Returns {
        "healthy": [...], "under_replicated": [...], "unavailable": [...],
        "unrecoverable": [...], "over_replicated": [...]
    } where each list item has chunk_id, object_id, valid_count, total_count,
    checksum, dead_ok_count, corrupt_count, dead_since_min.

    A replica is 'valid' iff state='OK' AND its host worker is 'ALIVE'.
    """
    with _reader(conn) as c:
        rows = c.execute("""
            SELECT c.chunk_id, c.object_id, c.checksum, c.size, c.idx,
                   COUNT(CASE WHEN r.state = 'OK' AND w.status = 'ALIVE' THEN 1 END) AS valid,
                   COUNT(CASE WHEN r.state = 'OK' AND w.status = 'DEAD' THEN 1 END) AS dead_ok,
                   COUNT(CASE WHEN r.state = 'CORRUPT' THEN 1 END) AS corrupt_cnt,
                   COUNT(r.chunk_id) AS total,
                   COUNT(CASE WHEN r.state = 'OK' THEN 1 END) AS ok_any,
                   MIN(CASE WHEN r.state = 'OK' AND w.status = 'DEAD' THEN w.dead_since END) AS dead_since_min
            FROM chunks c
            JOIN objects o ON o.object_id = c.object_id
            LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
            LEFT JOIN workers w ON w.worker_id = r.worker_id
            WHERE o.status = 'COMMITTED'
            GROUP BY c.chunk_id
            ORDER BY c.chunk_id
        """).fetchall()
        result: Dict[str, list] = {
            "healthy": [], "under_replicated": [], "unavailable": [],
            "unrecoverable": [], "over_replicated": [],
        }
        for r in rows:
            item = {
                "chunk_id": r["chunk_id"],
                "object_id": r["object_id"],
                "checksum": r["checksum"],
                "size": r["size"],
                "idx": r["idx"],
                "valid_count": r["valid"],
                "total_count": r["total"],
                "dead_ok_count": r["dead_ok"],
                "corrupt_count": r["corrupt_cnt"],
                "dead_since_min": r["dead_since_min"],
            }
            v = r["valid"]
            ok_any = r["ok_any"]
            if v == replication_factor:
                result["healthy"].append(item)
            elif v > replication_factor:
                result["over_replicated"].append(item)
            elif 0 < v < replication_factor:
                result["under_replicated"].append(item)
            elif ok_any > 0:
                # valid_count == 0, at least one OK replica exists on a DEAD worker
                result["unavailable"].append(item)
            else:
                # valid_count == 0 and no OK replicas anywhere (all CORRUPT / missing)
                result["unrecoverable"].append(item)
        return result


def get_under_replicated_chunks(replication_factor: int = 2, conn=None) -> List[dict]:
    """Chunks of COMMITTED objects with fewer than R valid replicas.

    Returns [{chunk_id, object_id, checksum, valid_count, dead_since_min}].
    dead_since_min is the earliest dead_since among the workers hosting OK replicas
    that are currently DEAD (used for grace period).
    """
    with _reader(conn) as c:
        rows = c.execute("""
            SELECT c.chunk_id, c.object_id, c.checksum,
                   COUNT(CASE WHEN r.state = 'OK' AND w.status = 'ALIVE' THEN 1 END) AS valid_count,
                   MIN(CASE WHEN r.state = 'OK' AND w.status = 'DEAD' THEN w.dead_since END) AS dead_since_min
            FROM chunks c
            JOIN objects o ON o.object_id = c.object_id
            LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
            LEFT JOIN workers w ON w.worker_id = r.worker_id
            WHERE o.status = 'COMMITTED'
            GROUP BY c.chunk_id
            HAVING valid_count < ?
            ORDER BY valid_count ASC, c.chunk_id ASC
        """, (replication_factor,)).fetchall()
        return [dict(r) for r in rows]


def get_over_replicated_chunks(replication_factor: int = 2, conn=None) -> List[dict]:
    """Chunks with more than R valid replicas. Returns [{chunk_id, object_id, valid_count}]."""
    with _reader(conn) as c:
        rows = c.execute("""
            SELECT c.chunk_id, c.object_id,
                   COUNT(CASE WHEN r.state = 'OK' AND w.status = 'ALIVE' THEN 1 END) AS valid_count
            FROM chunks c
            JOIN objects o ON o.object_id = c.object_id
            LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
            LEFT JOIN workers w ON w.worker_id = r.worker_id
            WHERE o.status = 'COMMITTED'
            GROUP BY c.chunk_id
            HAVING valid_count > ?
            ORDER BY c.chunk_id
        """, (replication_factor,)).fetchall()
        return [dict(r) for r in rows]


def get_chunk_replicas_detail(chunk_id: str, conn=None) -> List[dict]:
    """All replicas for a chunk with worker address and status."""
    with _reader(conn) as c:
        rows = c.execute("""
            SELECT r.chunk_id, r.worker_id, r.state, w.address, w.status AS worker_status
            FROM replicas r
            JOIN workers w ON w.worker_id = r.worker_id
            WHERE r.chunk_id = ?
            ORDER BY r.worker_id
        """, (chunk_id,)).fetchall()
        return [dict(r) for r in rows]


def get_chunk_checksum(chunk_id: str, conn=None) -> Optional[str]:
    """Return the authoritative DB checksum for a chunk."""
    with _reader(conn) as c:
        row = c.execute("SELECT checksum FROM chunks WHERE chunk_id = ?", (chunk_id,)).fetchone()
        return row["checksum"] if row else None


def insert_replica_if_absent(chunk_id: str, worker_id: str, state: str = "OK", conn=None) -> bool:
    """Insert a replica row only if it doesn't already exist. Returns True if inserted."""
    with _writer(conn) as c:
        try:
            c.execute("INSERT INTO replicas (chunk_id, worker_id, state) VALUES (?, ?, ?)",
                      (chunk_id, worker_id, state))
            return True
        except sqlite3.IntegrityError:
            return False


def delete_replica(chunk_id: str, worker_id: str, conn=None) -> bool:
    """Delete a specific replica row. Returns True if deleted."""
    with _writer(conn) as c:
        return c.execute("DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?",
                         (chunk_id, worker_id)).rowcount == 1


def get_replicas_for_worker(worker_id: str, state: Optional[str] = None, conn=None) -> List[dict]:
    """All replica rows for a given worker, optionally filtered by state."""
    with _reader(conn) as c:
        if state:
            rows = c.execute(
                "SELECT r.chunk_id, r.worker_id, r.state, c.checksum AS db_checksum "
                "FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
                "WHERE r.worker_id = ? AND r.state = ? ORDER BY r.chunk_id",
                (worker_id, state)).fetchall()
        else:
            rows = c.execute(
                "SELECT r.chunk_id, r.worker_id, r.state, c.checksum AS db_checksum "
                "FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
                "WHERE r.worker_id = ? ORDER BY r.chunk_id",
                (worker_id,)).fetchall()
        return [dict(r) for r in rows]


def get_object_for_chunk(chunk_id: str, conn=None) -> Optional[dict]:
    """Return the object row for a given chunk_id, or None."""
    with _reader(conn) as c:
        row = c.execute(
            "SELECT o.* FROM objects o JOIN chunks c ON c.object_id = o.object_id "
            "WHERE c.chunk_id = ?", (chunk_id,)).fetchone()
        return dict(row) if row else None


def get_chunks_for_object_ordered(object_id: str, conn=None) -> List[dict]:
    """Chunks for an object ordered by idx, each with replicas and worker info."""
    with _reader(conn) as c:
        chunks = [dict(r) for r in c.execute(
            "SELECT * FROM chunks WHERE object_id = ? ORDER BY idx", (object_id,))]
        for ch in chunks:
            reps = c.execute(
                "SELECT r.chunk_id, r.worker_id, r.state, w.address, w.status AS worker_status "
                "FROM replicas r JOIN workers w ON w.worker_id = r.worker_id "
                "WHERE r.chunk_id = ? ORDER BY r.worker_id", (ch["chunk_id"],)).fetchall()
            ch["replicas"] = [dict(r) for r in reps]
        return chunks


def get_chunk_info(chunk_id: str, conn=None) -> Optional[dict]:
    """Return chunk row joined with parent object_status and object_created_at."""
    with _reader(conn) as c:
        row = c.execute("""
            SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum,
                   o.status AS object_status, o.created_at AS object_created_at
            FROM chunks c
            JOIN objects o ON o.object_id = c.object_id
            WHERE c.chunk_id = ?
        """, (chunk_id,)).fetchone()
        return dict(row) if row else None


def upsert_replica_ok(chunk_id: str, worker_id: str, conn=None) -> None:
    """Atomically insert or update a replica row to state='OK'."""
    with _writer(conn) as c:
        c.execute("""
            INSERT INTO replicas (chunk_id, worker_id, state)
            VALUES (?, ?, 'OK')
            ON CONFLICT(chunk_id, worker_id) DO UPDATE SET state = 'OK'
        """, (chunk_id, worker_id))


def get_replicas_for_worker_with_object(worker_id: str, state: Optional[str] = None, conn=None) -> List[dict]:
    """Replica rows for a worker including parent object_id, object_status, and chunk checksum."""
    with _reader(conn) as c:
        sql = """
            SELECT r.chunk_id, r.worker_id, r.state,
                   c.object_id, c.checksum AS db_checksum, o.status AS object_status
            FROM replicas r
            JOIN chunks c ON c.chunk_id = r.chunk_id
            JOIN objects o ON o.object_id = c.object_id
            WHERE r.worker_id = ?
        """
        params: list = [worker_id]
        if state is not None:
            sql += " AND r.state = ?"
            params.append(state)
        sql += " ORDER BY r.chunk_id"
        return [dict(r) for r in c.execute(sql, params).fetchall()]


def mark_chunk_replica_corrupt_if_exists(chunk_id: str, worker_id: str, conn=None) -> bool:
    """Mark an existing replica row as CORRUPT (ignores unknown chunks/workers)."""
    with _writer(conn) as c:
        cur = c.execute(
            "UPDATE replicas SET state = 'CORRUPT' WHERE chunk_id = ? AND worker_id = ?",
            (chunk_id, worker_id),
        )
        return cur.rowcount == 1


def get_worker_replica_summary(conn=None) -> List[dict]:
    """Per-worker summary of replica counts for admin observability."""
    with _reader(conn) as c:
        rows = c.execute("""
            SELECT w.worker_id, w.address, w.status,
                   COUNT(CASE WHEN r.state = 'OK' AND w.status = 'ALIVE' THEN 1 END) AS valid_replicas,
                   COUNT(CASE WHEN r.state = 'CORRUPT' THEN 1 END) AS corrupt_replicas,
                   COUNT(CASE WHEN r.state = 'PENDING_DELETE' THEN 1 END) AS pending_delete_replicas
            FROM workers w
            LEFT JOIN replicas r ON r.worker_id = w.worker_id
            GROUP BY w.worker_id
            ORDER BY w.worker_id
        """).fetchall()
        return [dict(r) for r in rows]


def save_scrub_status(last_scrub_at: float, chunks_checked: int, corrupt_found: int, conn=None) -> None:
    """Persist the latest cluster scrub summary in scrub_status singleton row."""
    with _writer(conn) as c:
        c.execute("""
            INSERT INTO scrub_status (id, last_scrub_at, chunks_checked, corrupt_found)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_scrub_at = excluded.last_scrub_at,
                chunks_checked = excluded.chunks_checked,
                corrupt_found = excluded.corrupt_found
        """, (last_scrub_at, chunks_checked, corrupt_found))


def get_scrub_status(conn=None) -> dict:
    """Return latest scrub summary or default zeroed structure."""
    with _reader(conn) as c:
        row = c.execute(
            "SELECT last_scrub_at, chunks_checked, corrupt_found FROM scrub_status WHERE id = 1"
        ).fetchone()
        if row is None:
            return {"last_scrub_at": None, "chunks_checked": 0, "corrupt_found": 0}
        return dict(row)


def list_stale_pending(cutoff: float, conn=None) -> List[str]:
    with _reader(conn) as c:
        return [r[0] for r in c.execute(
            "SELECT object_id FROM objects WHERE status='PENDING' AND created_at < ? "
            "ORDER BY created_at", (cutoff,))]


def list_committed(limit: int, offset: int, conn=None):
    """(rows, total) of COMMITTED objects: newest first, object_id ASC as tie-break."""
    with _reader(conn) as c:
        rows = c.execute(
            "SELECT object_id, name, size, content_type, checksum, chunk_count, created_at, modified_at "
            "FROM objects WHERE status='COMMITTED' "
            "ORDER BY created_at DESC, object_id ASC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        total = c.execute("SELECT COUNT(*) FROM objects WHERE status='COMMITTED'").fetchone()[0]
        return [dict(r) for r in rows], total


def patch_metadata(object_id: str, patch: Dict[str, Optional[str]], now: float,
                   max_bytes: int, conn=None) -> Dict[str, str]:
    """Merge-patch custom metadata under BEGIN IMMEDIATE (no lost updates).

    None deletes a key; a string inserts/updates it. Raises ObjectNotFoundError (missing or not
    COMMITTED) or MetadataTooLargeError (merged result over `max_bytes`); both roll back.
    """
    import json

    with _writer(conn) as c:
        row = c.execute("SELECT status FROM objects WHERE object_id=?", (object_id,)).fetchone()
        if row is None or row["status"] != "COMMITTED":
            raise ObjectNotFoundError(object_id)
        current = {r["key"]: r["value"] for r in c.execute(
            "SELECT key, value FROM custom_metadata WHERE object_id=?", (object_id,))}
        for k, v in patch.items():
            if v is None:
                current.pop(k, None)
            else:
                current[k] = v
        from settings import MAX_METADATA_KEYS
        if len(current) > MAX_METADATA_KEYS:
            raise MetadataTooLargeError(f"{len(current)} keys > {MAX_METADATA_KEYS}")
        size = len(json.dumps(current, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        if size > max_bytes:
            raise MetadataTooLargeError(f"{size} > {max_bytes} bytes")
        c.execute("DELETE FROM custom_metadata WHERE object_id=?", (object_id,))
        for k, v in current.items():
            c.execute("INSERT INTO custom_metadata (object_id, key, value) VALUES (?, ?, ?)",
                      (object_id, k, v))
        c.execute("UPDATE objects SET modified_at=? WHERE object_id=?", (now, object_id))
        return dict(sorted(current.items()))
