-- SQLite schema for the controller (metadata authority).
-- Applied on startup by controller/app.py (idempotent).

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS objects (
    object_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    checksum     TEXT NOT NULL,                 -- full-file SHA-256
    chunk_count  INTEGER NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id    TEXT PRIMARY KEY,
    object_id   TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,               -- zero-based order
    size        INTEGER NOT NULL,               -- <= 1 MiB
    checksum    TEXT NOT NULL,                  -- chunk SHA-256
    UNIQUE (object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,            -- e.g. worker1
    address        TEXT NOT NULL,               -- e.g. http://worker1:8001
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status         TEXT NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'DEAD'))
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id  TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    state     TEXT NOT NULL DEFAULT 'ACTIVE'
              CHECK (state IN ('ACTIVE', 'CORRUPT', 'REPAIRING')),
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS custom_metadata (
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    key       TEXT NOT NULL,
    value     TEXT NOT NULL,
    PRIMARY KEY (object_id, key)
);

CREATE INDEX IF NOT EXISTS idx_chunks_object   ON chunks(object_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);

