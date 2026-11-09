# Design Doc: Mini Distributed Object Store (Day 1)

Condensed from `Distributed Object Store Architecture Design.md`.

## Components
| Component | Role |
|---|---|
| **Client** | Plain HTTP client. It talks only to the controller (`:8000`). |
| **Controller** (1) | REST gateway, splits objects into 1 MiB chunks, SHA-256 hashing, SQLite metadata (WAL), replica placement, heartbeat tracking, repair loop. |
| **Workers** `worker1`–`worker4` | Store raw chunks on their own named volume (`/data/chunks/{chunk_id}`). They verify checksums, heartbeat every 3 s, and expose `/health`. |

Replication factor R = 2, on distinct workers.

## Data model
`objects` (object_id, name, size, content_type, checksum, chunk_count, timestamps) → `chunks` (chunk_id, object_id, chunk_index, size, checksum) → `replicas` (chunk_id, worker_id, state) ← `workers` (id, address, last_heartbeat, status). `custom_metadata` (object_id, key, value) holds user key-values.
The full DDL is in `controller/schema.sql`.

## Upload flow
1. The client sends `POST /api/v1/objects` (multipart).
2. The controller creates an `object_id` and splits the stream into 1 MiB chunks.
3. For each chunk it computes the SHA-256 and a `chunk_id`.
4. It picks 2 distinct ACTIVE workers (heartbeat < 10 s old) with the fewest chunks.
5. It sends `PUT /internal/chunks/{id}` to both workers in parallel. Each worker verifies the checksum, writes the chunk and returns 200.
6. If every chunk reached both workers, the controller commits `objects`, `chunks`, `replicas` and `custom_metadata` in one transaction. If not, it deletes the partial chunks and returns 503.
7. The controller returns 201 with the metadata JSON.

## Download flow
1. The controller reads the chunks in order (`ORDER BY chunk_index`). It returns 404 if the object is missing.
2. For each chunk it tries the first ACTIVE replica and verifies the SHA-256.
3. On a timeout, an error or a checksum mismatch it fails over to the other replica. It marks the bad replica `CORRUPT` for repair.
4. It streams the verified bytes to the client.

## Repair flow
This loop runs every 5 s on the controller.
1. Mark workers with no heartbeat for more than 10 s as `DEAD`.
2. Find chunks with fewer than 2 ACTIVE replicas, including replicas in the `CORRUPT` state.
3. Copy each chunk from a healthy worker to an ACTIVE worker that doesn't hold it, then insert the new `replicas` row.

## Failure model
| Failure | Handling |
|---|---|
| Worker stops (`docker stop worker2`) | Detected in about 10 s. Reads fail over to the other replica. Repair restores R = 2. |
| Worker restarts | Its volume keeps the chunks. It re-registers on the next heartbeat. |
| Silent corruption | Caught by the checksum on read. The bad replica is replaced from the good one. |
| Controller crash | Out of scope (single point of failure). State is recovered from SQLite on restart. |
| Malicious workers | Out of scope. |

