# Architecture Design: Mini Distributed Cloud Object Store

## 1\. Executive Summary & System Objectives

This document details the architectural design for a fault-tolerant, rack-local **Mini Distributed Cloud Object Store**. The system is optimized for containerized environments running on Docker and Docker Compose.

### Key System Parameters

- **Controller Nodes:** 1 (Single coordinator, REST Gateway, metadata authority)  
- **Worker/Storage Nodes:** 4 (`worker-1`, `worker-2`, `worker-3`, `worker-4`)  
- **Clients:** 1 or more external consumers (via standard HTTP/REST)  
- **Default Chunk Size:** 1 MB ($1,048,576$ bytes)  
- **Replication Factor ($R$):** 2 (stored on distinct physical worker instances)  
- **Persistence Layer:** Host-mounted Docker named volumes per worker node

---

## 2\. High-Level Architecture & Separation of Concerns

```
                     +---------------------------------------+
                     |         Client (CLI / curl)           |
                     +---------------------------------------+
                                          |
                         POST/GET/PATCH/DELETE /api/v1/objects
                                          v
                     +---------------------------------------+
                     |         Controller (Master Node)       |
                     |  - REST Public Gateway                |
                     |  - Chunking Engine (1 MB fixed)       |
                     |  - Metadata Store (SQLite / WAL)      |
                     |  - Heartbeat & Health Tracker         |
                     |  - Re-replication / Self-Heal Loop    |
                     +---------------------------------------+
                               /      |       |      \
         Internal HTTP Calls  /       |       |       \  (PUT / GET / DELETE)
                             v        v       v        v
                         +-------+ +-------+ +-------+ +-------+
                         |Worker1| |Worker2| |Worker3| |Worker4|
                         | Node  | | Node  | | Node  | | Node  |
                         +-------+ +-------+ +-------+ +-------+
                             |         |         |         |
                             v         v         v         v
                         [Vol 1]   [Vol 2]   [Vol 3]   [Vol 4]
```

### Component Roles & Responsibilities

#### 1\. Controller Node (Coordinator & API Gateway)

- **Public API Exposure:** Implements all public `/api/v1/objects` endpoints.  
- **Payload Ingestion & Slicing:** Serves as the chunking engine. Incoming object streams are split into 1 MB chunks and hashed with SHA-256.  
- **Metadata Management:** Maintains an authoritative, transactional SQLite database containing file namespaces, chunk ordering, replica placements, and custom metadata.  
- **Replica Placement Engine:** Computes optimal worker placement targets using load-aware round-robin selection.  
- **Cluster Orchestration & Self-Healing:** Monitors worker heartbeats and triggers active re-replication when worker availability drops below $R=2$.

#### 2\. Worker Nodes (`worker-1` through `worker-4`)

- **Raw Chunk Storage:** Persists raw byte slices inside a container-mounted volume directory (`/data/chunks/{chunk_id}`).  
- **Data Integrity Safeguards:** Calculates SHA-256 digests on write and validates chunk digests prior to serving read requests.  
- **Liveness Reporting:** Dispatches recurring heartbeats to the controller every 3 seconds containing available capacity and active chunk counts.  
- **Internal API Service:** Exposes lightweight endpoints for chunk storage (`PUT`), retrieval (`GET`), and deletion (`DELETE`).

#### 3\. Client Layer

- Pure HTTP client (e.g., Python `requests`, `curl`, Postman) interacting strictly with the Controller via `/api/v1/objects` using standard `multipart/form-data` and `application/json`.  
- Has zero coupling to worker topologies, internal IP addresses, or chunk hashing mechanics.

---

## 3\. Authoritative Metadata Schema (SQLite)

The controller maintains metadata inside a persistent SQLite database enabled with Write-Ahead Logging (WAL) for concurrent read access:

```sql
-- Tracks uploaded binary objects and system-level attributes
CREATE TABLE IF NOT EXISTS objects (
    object_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    checksum TEXT NOT NULL,              -- Full-file SHA-256 digest
    chunk_count INTEGER NOT NULL,
    custom_metadata TEXT DEFAULT '{}',    -- Serialized JSON key-value store
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tracks chunk sequencing and per-chunk cryptographic hashes
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,         -- Zero-indexed sequential chunk order
    size INTEGER NOT NULL,                -- Size in bytes (<= 1,048,576)
    checksum TEXT NOT NULL,              -- SHA-256 digest of this individual chunk
    UNIQUE(object_id, chunk_index)
);

-- Tracks physical replica placement across registered workers
CREATE TABLE IF NOT EXISTS chunk_replicas (
    chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,             -- Unique worker container ID (e.g., "worker-1")
    PRIMARY KEY (chunk_id, worker_id)
);

-- Tracks worker health, registration state, and heartbeat recency
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    host_url TEXT NOT NULL,               -- e.g., "http://worker-1:8001"
    status TEXT NOT NULL DEFAULT 'ACTIVE',-- 'ACTIVE', 'DEAD'
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 4\. End-to-End Operational Workflows

### 4.1 Upload Flow (`POST /api/v1/objects`)

1. **Client Request:** Client issues `POST /api/v1/objects` with `multipart/form-data` containing the file payload and optional metadata JSON.  
2. **Identification & Buffer:** Controller assigns an RFC-4122 `object_id` (UUIDv4) and reads the input stream in fixed 1 MB ($1,048,576$ byte) increments.  
3. **Chunk Processing:** For each chunk $i\in [0,N-1]$:  
   - Compute the SHA-256 hash of the chunk data.  
   - Generate a deterministic or unique `chunk_id` (e.g., `{object_id}_chunk_{i}`).  
4. **Target Worker Selection:**  
   - Filter all workers with `status = 'ACTIVE'` and `last_heartbeat > NOW() - 10s`.  
   - Select 2 distinct workers (${W}_{A},{W}_{B}$) holding the lowest current number of chunks.  
5. **Parallel Write Dispatch:**  
   - Controller sends `PUT /internal/chunks/{chunk_id}` concurrently to ${W}_{A}$ and ${W}_{B}$.  
   - Payload includes raw binary bytes and expected checksum.  
   - Workers write bytes to disk and return `200 OK`.  
6. **Atomic Transaction Commit:**  
   - If both worker writes succeed, insert records into `objects`, `chunks`, and `chunk_replicas`.  
   - If any worker fails after retries, abort write pipeline, delete already sent chunks, and return `503 Service Unavailable`.  
7. **Response:** Return HTTP `201 Created` with full metadata JSON.

---

### 4.2 Download Flow (`GET /api/v1/objects/{object_id}`)

1. **Query Resolution:** Controller fetches the object record and ordered chunks from SQLite (`ORDER BY chunk_index ASC`). Return `404 Not Found` if missing.  
2. **Chunk Pipeline Streaming:**  
   - Initialize an HTTP streaming response to the client with the stored `Content-Type` and `Content-Length`.  
   - For each chunk in sequential order:  
     1. Retrieve candidate worker list from `chunk_replicas`.  
     2. Attempt `GET /internal/chunks/{chunk_id}` from primary replica worker ${W}_{1}$.  
     3. Compute SHA-256 of downloaded chunk bytes:  
        - **If checksum matches:** Stream bytes directly into the client output response buffer.  
        - **If worker fails, times out, or checksum mismatches:** Immediately fail over to secondary replica worker ${W}_{2}$.  
        - Mark the corrupted worker replica for background repair.  
3. **Completion:** Flush stream buffer upon sending all chunks.

---

### 4.3 Self-Healing & Repair Flow (Background Daemon)

A background loop runs on the Controller every 5 seconds:

1. **Heartbeat Sweep:** Mark any worker where `last_heartbeat < NOW() - 10s` as `DEAD`.  
2. **Under-Replicated Scan:**

```sql
SELECT c.chunk_id, COUNT(cr.worker_id) AS active_replicas
FROM chunks c
JOIN chunk_replicas cr ON c.chunk_id = cr.chunk_id
JOIN workers w ON cr.worker_id = w.worker_id
WHERE w.status = 'ACTIVE'
GROUP BY c.chunk_id
HAVING active_replicas < 2;
```

3. **Replica Restoration:**  
   - For each under-replicated chunk:  
     1. Locate the surviving healthy worker $W_{\text{src}}$.
     2. Download chunk payload from $W_{\text{src}}$.  
     3. Select an active worker $W_{\text{target}}$ that does **not** currently hold this chunk.  
     4. Dispatch `PUT /internal/chunks/{chunk_id}` to $W_{\text{target}}$.  
     5. Update `chunk_replicas` table with the new location.

---

## 5\. Failure Model & System Boundaries

### Handled Failures

- **Worker Crash / Network Disconnection:** Detected within 10 seconds via missed heartbeats. Re-replication automatically restores degraded chunks on remaining workers.  
- **Silent Disk Corruption (Bit Rot):** Caught during reads using SHA-256 checksum validation. Corrupted replicas are discarded, fetched from the healthy secondary worker, and rewritten.  
- **Worker Container Restarts:** When a worker container restarts (`docker restart worker-2`), it retains all chunks via persistent Docker volumes and re-registers itself automatically on its next heartbeat.

### Explicit Non-Goals / Handled Assumptions

- **Controller Crash:** The single controller is an intentional single point of failure (SPOF) for simplicity. It does not use Raft/Paxos consensus. If restarted, it recovers state completely from its persistent SQLite metadata database.  
- **Byzantine Failures:** Workers are assumed fail-stop or fail-corrupt with non-adversarial behavior (they do not maliciously sign false payloads).

---

## 6\. Formal System Invariants

1. **Replication Safety Invariant:** $$\forall c \in \text{Chunks}, \quad \text{Replicas}(c) = \min(2, |\text{Active Workers}|)$$ Every stored chunk maintains two valid copies across distinct workers during steady-state operation.
2. **Colocation Exclusion Invariant:** $$\forall c \in \text{Chunks}, \quad \text{Worker}(c_1) \neq \text{Worker}(c_2)$$ No two replicas of the same chunk ID can reside on the same worker node or share the same physical volume.
3. **Data Integrity Invariant:** $$\text{SHA256}(\text{Data}_{\text{Worker}}) = \text{Checksum}_{\text{Controller}}$$ A chunk is only accepted as valid if its disk-computed cryptographic digest strictly matches the value stored in the controller's metadata catalog.
4. **Contiguous Partition Invariant:** $$\sum_{i=0}^{N-1} \text{Size}(\text{Chunk}_i) = \text{Size}(\text{Object})$$ Chunk indices must form a contiguous sequence $\langle 0, 1, \dots, N-1 \rangle$ reconstructing the exact original byte stream without gaps or overlaps.

---

## 7\. Concrete API Specification

### Client REST API (Port 8000\)

| Method | Endpoint | Description | Success Code |
| :---- | :---- | :---- | :---- |
| `POST` | `/api/v1/objects` | Upload binary object (`multipart/form-data`) | `201 Created` |
| `GET` | `/api/v1/objects/{object_id}` | Download complete stitched binary object | `200 OK` |
| `GET` | `/api/v1/objects/{object_id}/metadata` | Fetch system and custom metadata | `200 OK` |
| `PATCH` | `/api/v1/objects/{object_id}/metadata` | Update custom key-value metadata | `200 OK` |
| `DELETE` | `/api/v1/objects/{object_id}` | Delete object and prune all chunk replicas | `204 No Content` |
| `GET` | `/api/v1/objects` | List all stored objects with pagination | `200 OK` |

### Internal Worker API (Ports 8001–8004)

| Method | Endpoint | Description |
| :---- | :---- | :---- |
| `POST` | `/internal/workers/register` | Initial worker registration on boot |
| `POST` | `/internal/workers/heartbeat` | Periodic worker liveness ping |
| `PUT` | `/internal/chunks/{chunk_id}` | Write binary chunk to worker disk |
| `GET` | `/internal/chunks/{chunk_id}` | Read binary chunk from worker disk |
| `DELETE` | `/internal/chunks/{chunk_id}` | Remove chunk file from disk |
| `GET` | `/internal/workers/{worker_id}/chunks` | Return inventory of all chunks on worker |

---

## 8\. Deployment Configuration (`docker-compose.yml`)

```yaml
version: '3.8'

services:
  controller:
    build:
      context: ./controller
    container_name: dfs-controller
    ports:
      - "8000:8000"
    volumes:
      - controller-metadata:/data/metadata
    environment:
      - PORT=8000
      - REPLICATION_FACTOR=2
      - HEARTBEAT_TIMEOUT_SEC=10
    networks:
      - dfs-network

  worker-1:
    build:
      context: ./worker
    container_name: worker-1
    ports:
      - "8001:8001"
    environment:
      - WORKER_ID=worker-1
      - PORT=8001
      - CONTROLLER_URL=http://controller:8000
    volumes:
      - worker-1-data:/data/chunks
    networks:
      - dfs-network

  worker-2:
    build:
      context: ./worker
    container_name: worker-2
    ports:
      - "8002:8001"
    environment:
      - WORKER_ID=worker-2
      - PORT=8001
      - CONTROLLER_URL=http://controller:8000
    volumes:
      - worker-2-data:/data/chunks
    networks:
      - dfs-network

  worker-3:
    build:
      context: ./worker
    container_name: worker-3
    ports:
      - "8003:8001"
    environment:
      - WORKER_ID=worker-3
      - PORT=8001
      - CONTROLLER_URL=http://controller:8000
    volumes:
      - worker-3-data:/data/chunks
    networks:
      - dfs-network

  worker-4:
    build:
      context: ./worker
    container_name: worker-4
    ports:
      - "8004:8001"
    environment:
      - WORKER_ID=worker-4
      - PORT=8001
      - CONTROLLER_URL=http://controller:8000
    volumes:
      - worker-4-data:/data/chunks
    networks:
      - dfs-network

volumes:
  controller-metadata:
  worker-1-data:
  worker-2-data:
  worker-3-data:
  worker-4-data:

networks:
  dfs-network:
    driver: bridge
```

&nbsp;

---

## 9\. Demonstration Verification Scripts

### Test 1: Worker Failure & Auto-Healing

```sh
# 1. Upload a 2.5 MB sample file (produces 3 chunks: 1MB, 1MB, 0.5MB)
OBJECT_ID=$(curl -s -X POST http://localhost:8000/api/v1/objects \
  -F "file=@sample.pdf" \
  -F 'metadata={"owner":"student","class":"cloud-computing"}' | jq -r '.object_id')

# 2. Inspect metadata to identify assigned workers per chunk
curl -s http://localhost:8000/api/v1/objects/${OBJECT_ID}/metadata | jq .

# 3. Simulate failure of worker-2
docker stop worker-2

# 4. Download file immediately; verify download works seamlessly via surviving replica
curl -s http://localhost:8000/api/v1/objects/${OBJECT_ID} -o downloaded.pdf
diff sample.pdf downloaded.pdf && echo "Download during worker failure: SUCCESS"

# 5. Wait 12 seconds for self-healing repair thread to trigger
sleep 12

# 6. Verify replication factor restored to 2 on surviving workers
curl -s http://localhost:8000/api/v1/objects/${OBJECT_ID}/metadata | jq '.chunks[].replicas'
```

### Test 2: Data Corruption Detection & Recovery

```sh
# 1. Identify a chunk stored on worker-1
CHUNK_ID=$(curl -s http://localhost:8000/api/v1/objects/${OBJECT_ID}/metadata | jq -r '.chunks[0].chunk_id')

# 2. Directly inject bit corruption into the chunk file on worker-1
docker exec -it worker-1 sh -c "echo 'CORRUPTED_BYTES' > /data/chunks/${CHUNK_ID}"

# 3. Download the object
curl -s http://localhost:8000/api/v1/objects/${OBJECT_ID} -o repaired.pdf

# 4. Verify that data integrity was maintained via secondary replica
diff sample.pdf repaired.pdf && echo "Integrity detection and repair: SUCCESS"
```

&nbsp;