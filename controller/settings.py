"""Controller configuration from environment variables."""
import os
from dataclasses import dataclass


class SettingsError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    db_path: str = "/data/controller.db"
    heartbeat_timeout: float = 15.0          # seconds without a heartbeat => DEAD
    liveness_check_interval: float = 3.0     # how often the monitor sweeps
    replication_factor: int = 2
    chunk_size: int = 1_048_576              # 1 MiB
    heartbeat_interval: int = 5              # told to workers on registration


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
    )

