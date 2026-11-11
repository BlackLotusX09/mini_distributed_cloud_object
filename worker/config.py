"""Worker configuration, resolved strictly from environment variables."""
import os
import re
from dataclasses import dataclass


class ConfigError(Exception):
    pass


_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m)?")
_WORKER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]*")


def parse_duration(text: str) -> float:
    """'5s' -> 5.0, '500ms' -> 0.5, '2m' -> 120.0, '5' -> 5.0 (seconds)."""
    m = _DURATION_RE.fullmatch(text.strip())
    if not m:
        raise ConfigError(f"invalid duration: {text!r}")
    value, unit = float(m.group(1)), m.group(2) or "s"
    seconds = value * {"ms": 0.001, "s": 1, "m": 60}[unit]
    if seconds <= 0:
        raise ConfigError("duration must be > 0")
    return seconds


@dataclass(frozen=True)
class Config:
    worker_id: str
    data_dir: str
    controller_url: str
    port: int
    heartbeat_interval: float

    @property
    def address(self) -> str:
        """Advertised address: the container bridge name, never localhost."""
        return f"http://{self.worker_id}:{self.port}"


def load(env=None) -> Config:
    env = os.environ if env is None else env
    worker_id = env.get("WORKER_ID", "").strip()
    if not worker_id:
        raise ConfigError("WORKER_ID is required")
    if not _WORKER_ID_RE.fullmatch(worker_id):
        raise ConfigError(f"invalid WORKER_ID: {worker_id!r}")
    try:
        port = int(env.get("PORT", "8000"))
    except ValueError:
        raise ConfigError("PORT must be an integer") from None
    if not 0 < port < 65536:
        raise ConfigError("PORT out of range")
    return Config(
        worker_id=worker_id,
        data_dir=env.get("DATA_DIR", "/data"),
        controller_url=env.get("CONTROLLER_URL", "http://controller:8000").rstrip("/"),
        port=port,
        heartbeat_interval=parse_duration(env.get("HEARTBEAT_INTERVAL", "5s")),
    )

