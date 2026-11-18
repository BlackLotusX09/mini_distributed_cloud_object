#!/usr/bin/env python3
"""Cluster invariant checker. Exit 0 only if every invariant holds, else print violations and exit 1.

Invariants
  1. Every chunk of every COMMITTED object has 2 valid replicas (state OK, worker ALIVE) on 2 distinct
     workers (checked when >= 2 workers are ALIVE).
  2. Every OK replica row on an ALIVE worker has a chunk file on that worker's disk whose SHA-256
     matches the DB checksum.
  3. Every chunk file on an ALIVE worker has a DB replica row (PENDING_DELETE rows count; files that
     belong to a live PENDING upload, or that are younger than the TTL, are exempt).
  4. No PENDING object is older than PENDING_OBJECT_TTL_SECONDS.

Sources (auto-detected):
  docker : `docker exec` into controller/worker1..4 (default when docker is usable)
  local  : --db PATH --data-root DIR   (worker data at DIR/<worker_id>/chunks, as used by the
           local-process cluster in tests/integration)
"""
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time

TTL_DEFAULT = 600.0

_WORKER_SCAN = r"""
import hashlib, json, os
root = "/data/chunks"
out = {}
for dp, _, fs in os.walk(root):
    for f in fs:
        if f.endswith(".chunk") and not f.startswith("."):
            p = os.path.join(dp, f)
            with open(p, "rb") as fh:
                h = hashlib.sha256()
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(blk)
            out[f[:-6]] = {"sha": h.hexdigest(), "mtime": os.path.getmtime(p)}
print(json.dumps(out))
"""

_DB_QUERY = r"""
import json, sqlite3, sys
c = sqlite3.connect("file:/data/controller.db?mode=ro", uri=True)
c.row_factory = sqlite3.Row
print(json.dumps([dict(r) for r in c.execute(sys.argv[1], json.loads(sys.argv[2]))]))
"""


def scan_dir(chunks_root):
    import hashlib
    out = {}
    for dp, _, fs in os.walk(chunks_root):
        for f in fs:
            if f.endswith(".chunk") and not f.startswith("."):
                p = os.path.join(dp, f)
                h = hashlib.sha256()
                with open(p, "rb") as fh:
                    for blk in iter(lambda: fh.read(1 << 20), b""):
                        h.update(blk)
                out[f[:-6]] = {"sha": h.hexdigest(), "mtime": os.path.getmtime(p)}
    return out


class LocalSource:
    def __init__(self, db_path, data_root):
        self.db_path, self.data_root = db_path, data_root

    def query(self, sql, params=()):
        c = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10)
        c.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in c.execute(sql, params)]
        finally:
            c.close()

    def worker_files(self, worker_id):
        return scan_dir(os.path.join(self.data_root, worker_id, "chunks"))


class DockerSource:
    def _exec(self, container, *args, stdin=None):
        r = subprocess.run(["docker", "exec", "-i", container, *args], input=stdin,
                           capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            raise RuntimeError(f"docker exec {container} failed: {r.stderr.strip()}")
        return r.stdout

    def query(self, sql, params=()):
        return json.loads(self._exec("controller", "python", "-c", _DB_QUERY, sql, json.dumps(list(params))))

    def worker_files(self, worker_id):
        return json.loads(self._exec(worker_id, "python", "-c", _WORKER_SCAN))


def check(src, ttl=TTL_DEFAULT, now=None, replication_factor=2):
    now = time.time() if now is None else now
    violations = []
    workers = {w["worker_id"]: w for w in src.query("SELECT * FROM workers")}
    alive = sorted(w for w, r in workers.items() if r["status"] == "ALIVE")
    reps = src.query("SELECT r.chunk_id, r.worker_id, r.state, c.checksum, c.object_id, o.status AS ostatus "
                     "FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
                     "JOIN objects o ON o.object_id = c.object_id")
    chunks = src.query("SELECT c.chunk_id, c.object_id, c.checksum FROM chunks c "
                       "JOIN objects o ON o.object_id = c.object_id WHERE o.status = 'COMMITTED'")
    chunk_row = {c["chunk_id"]: c for c in src.query(
        "SELECT c.chunk_id, c.object_id, o.status AS ostatus, o.created_at FROM chunks c "
        "JOIN objects o ON o.object_id = c.object_id")}
    objects = {o["object_id"]: o for o in src.query("SELECT object_id, status, created_at FROM objects")}

    # 1. replication
    if len(alive) >= replication_factor:
        by_chunk = {}
        for r in reps:
            if r["state"] == "OK" and workers.get(r["worker_id"], {}).get("status") == "ALIVE":
                by_chunk.setdefault(r["chunk_id"], []).append(r["worker_id"])
        for c in chunks:
            ws = by_chunk.get(c["chunk_id"], [])
            if len(ws) != replication_factor or len(set(ws)) != len(ws):
                violations.append(f"[1] chunk {c['chunk_id']}: valid replicas on {ws} "
                                  f"(want {replication_factor} distinct ALIVE workers)")

    # disk scans of ALIVE workers
    files = {}
    for w in alive:
        try:
            files[w] = src.worker_files(w)
        except Exception as e:  # unreachable container/dir counts as a violation of inspectability
            violations.append(f"[2] cannot inspect disk of {w}: {e}")

    # 2. OK rows <-> files with matching sha
    for r in reps:
        w = r["worker_id"]
        if r["state"] != "OK" or w not in files:
            continue
        f = files[w].get(r["chunk_id"])
        if f is None:
            violations.append(f"[2] {w}: OK replica {r['chunk_id']} has no chunk file on disk")
        elif f["sha"] != r["checksum"]:
            violations.append(f"[2] {w}: chunk {r['chunk_id']} sha {f['sha'][:12]} != DB {str(r['checksum'])[:12]}")

    # 3. files <-> rows
    row_keys = {(r["chunk_id"], r["worker_id"]) for r in reps}
    for w, fs in files.items():
        for cid, f in fs.items():
            if (cid, w) in row_keys:
                continue
            info = chunk_row.get(cid)
            if info is not None and info["ostatus"] == "PENDING" and now - info["created_at"] < ttl:
                continue  # active upload
            if info is None:
                parent = objects.get(cid.rsplit("_", 1)[0])
                if parent is not None and parent["status"] == "PENDING" and now - parent["created_at"] < ttl:
                    continue
                if now - f["mtime"] < ttl:
                    continue  # in-flight / just-rolled-back chunk inside the safety window
            violations.append(f"[3] {w}: chunk file {cid} has no DB replica row")

    # 4. stale PENDING
    for o in objects.values():
        if o["status"] == "PENDING" and now - o["created_at"] >= ttl:
            violations.append(f"[4] object {o['object_id']} PENDING for {int(now - o['created_at'])}s (ttl {int(ttl)}s)")
    return violations


def docker_usable():
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except Exception:
        return False


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", help="controller SQLite path (local mode)")
    ap.add_argument("--data-root", help="dir containing <worker_id>/chunks (local mode)")
    ap.add_argument("--ttl", type=float, default=float(os.environ.get("PENDING_OBJECT_TTL_SECONDS", TTL_DEFAULT)))
    ap.add_argument("--replication-factor", type=int, default=2)
    a = ap.parse_args(argv)
    if a.db and a.data_root:
        src = LocalSource(a.db, a.data_root)
    elif docker_usable():
        src = DockerSource()
    else:
        print("ERROR: Docker is unavailable; pass --db and --data-root for a local-process cluster", file=sys.stderr)
        return 2
    try:
        violations = check(src, ttl=a.ttl, replication_factor=a.replication_factor)
    except Exception as e:
        print(f"ERROR: could not inspect cluster: {e}", file=sys.stderr)
        return 2
    if violations:
        print(f"INVARIANT VIOLATIONS ({len(violations)}):")
        for v in violations:
            print("  -", v)
        return 1
    print("OK: all cluster invariants hold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
