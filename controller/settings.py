"""Controller configuration from environment variables."""
import os
from dataclasses import dataclass


class SettingsError(Exception):
    pass


CHUNK_SIZE = 1_048_576               # 1 MiB
REPLICATION_FACTOR = 2
UPLOAD_WORKER_TIMEOUT = 10.0         # seconds per worker HTTP request
MAX_RETRIES_PER_CHUNK = 3
PENDING_OBJECT_TTL_SECONDS = 600     # 10 minutes

MAX_METADATA_KEYS = 64
MAX_OBJECT_NAME_LENGTH = 255
WORKER_READ_TIMEOUT = 10.0           # controller -> worker GET /chunk
WORKER_DELETE_TIMEOUT = 5.0          # controller -> worker DELETE /chunk (no inline retry)
WORKER_SCRUB_TIMEOUT = 60.0          # controller -> worker POST /internal/scrub

# Day 6: Failure Handling & Re-Replication
REPAIR_GRACE_PERIOD = 30.0           # seconds before a dead-worker chunk is eligible for repair
REPAIR_INTERVAL = 10.0               # seconds between repair cycles
MAX_REPAIRS_PER_CYCLE = 20           # cap repairs per cycle to avoid starving client traffic
REPAIR_CONCURRENCY = 4               # max parallel copy operations
MAX_CHUNK_REPAIR_ATTEMPTS = 5        # give up on a chunk after this many failed repairs
ORPHAN_SAFETY_TTL_SECONDS = 600      # seconds before orphan chunks on a rejoining worker are deleted

# Day 7: Integrity & Scrubbing
SCRUB_INTERVAL = 300.0               # seconds between full-cluster scrub passes
WORKER_SCRUB_YIELD_DELAY = 0.005     # seconds to sleep between chunks during scrub


@dataclass(frozen=True)
class Settings:
    db_path: str = "/data/controller.db"
    heartbeat_timeout: float = 15.0          # seconds without a heartbeat => DEAD
    liveness_check_interval: float = 3.0     # how often the monitor sweeps
    replication_factor: int = REPLICATION_FACTOR
    chunk_size: int = CHUNK_SIZE
    heartbeat_interval: int = 5              # told to workers on registration
    upload_worker_timeout: float = UPLOAD_WORKER_TIMEOUT
    max_retries_per_chunk: int = MAX_RETRIES_PER_CHUNK
    pending_object_ttl_seconds: float = float(PENDING_OBJECT_TTL_SECONDS)
    max_metadata_bytes: int = 16 * 1024      # serialized custom metadata limit
    worker_read_timeout: float = WORKER_READ_TIMEOUT
    worker_delete_timeout: float = WORKER_DELETE_TIMEOUT
    worker_scrub_timeout: float = WORKER_SCRUB_TIMEOUT
    # Day 6
    repair_grace_period: float = REPAIR_GRACE_PERIOD
    repair_interval: float = REPAIR_INTERVAL
    max_repairs_per_cycle: int = MAX_REPAIRS_PER_CYCLE
    repair_concurrency: int = REPAIR_CONCURRENCY
    max_chunk_repair_attempts: int = MAX_CHUNK_REPAIR_ATTEMPTS
    orphan_safety_ttl_seconds: float = float(ORPHAN_SAFETY_TTL_SECONDS)
    # Day 7
    scrub_interval: float = SCRUB_INTERVAL
    worker_scrub_yield_delay: float = WORKER_SCRUB_YIELD_DELAY


def _num(env, key, default, cast, minimum):
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        value = cast(raw)
    except ValueError:
        raise SettingsError(f"{key} must be a number, got {raw!r}") from None
    if value < minimum:
        raise SettingsError(f"{key} must be >= {minimum}, got {value}")
    return value


def load(env=None) -> Settings:
    env = os.environ if env is None else env
    d = Settings()
    return Settings(
        db_path=env.get("DB_PATH") or d.db_path,
        heartbeat_timeout=_num(env, "HEARTBEAT_TIMEOUT", d.heartbeat_timeout, float, 0.1),
        liveness_check_interval=_num(env, "LIVENESS_CHECK_INTERVAL", d.liveness_check_interval, float, 0.05),
        replication_factor=_num(env, "REPLICATION_FACTOR", d.replication_factor, int, 1),
        chunk_size=_num(env, "CHUNK_SIZE", d.chunk_size, int, 1),
        heartbeat_interval=_num(env, "HEARTBEAT_INTERVAL", d.heartbeat_interval, int, 1),
        upload_worker_timeout=_num(env, "UPLOAD_WORKER_TIMEOUT", d.upload_worker_timeout, float, 0.1),
        max_retries_per_chunk=_num(env, "MAX_RETRIES_PER_CHUNK", d.max_retries_per_chunk, int, 0),
        pending_object_ttl_seconds=_num(env, "PENDING_OBJECT_TTL_SECONDS", d.pending_object_ttl_seconds, float, 0.0),
        max_metadata_bytes=_num(env, "MAX_METADATA_BYTES", d.max_metadata_bytes, int, 2),
        worker_read_timeout=_num(env, "WORKER_READ_TIMEOUT", d.worker_read_timeout, float, 0.1),
        worker_delete_timeout=_num(env, "WORKER_DELETE_TIMEOUT", d.worker_delete_timeout, float, 0.1),
        worker_scrub_timeout=_num(env, "WORKER_SCRUB_TIMEOUT", d.worker_scrub_timeout, float, 0.1),
        repair_grace_period=_num(env, "REPAIR_GRACE_PERIOD", d.repair_grace_period, float, 0.0),
        repair_interval=_num(env, "REPAIR_INTERVAL", d.repair_interval, float, 0.01),
        max_repairs_per_cycle=_num(env, "MAX_REPAIRS_PER_CYCLE", d.max_repairs_per_cycle, int, 1),
        repair_concurrency=_num(env, "REPAIR_CONCURRENCY", d.repair_concurrency, int, 1),
        max_chunk_repair_attempts=_num(env, "MAX_CHUNK_REPAIR_ATTEMPTS", d.max_chunk_repair_attempts, int, 1),
        orphan_safety_ttl_seconds=_num(env, "ORPHAN_SAFETY_TTL_SECONDS", d.orphan_safety_ttl_seconds, float, 0.0),
        scrub_interval=_num(env, "SCRUB_INTERVAL", d.scrub_interval, float, 0.01),
        worker_scrub_yield_delay=_num(env, "WORKER_SCRUB_YIELD_DELAY", d.worker_scrub_yield_delay, float, 0.0),
    )
