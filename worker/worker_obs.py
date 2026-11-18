"""Worker structured logging: JSON lines with request-id correlation."""
import json
import os
import sys
import time

NODE_ID = os.environ.get("WORKER_ID", "worker")
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_MIN = _LEVELS.get(os.environ.get("LOG_LEVEL", "INFO").upper(), 20)


def set_node_id(node_id: str) -> None:
    global NODE_ID
    NODE_ID = node_id


def log(level: str, event: str, request_id: str = "background", **fields) -> None:
    if _LEVELS.get(level, 20) < _MIN:
        return
    now = time.time()
    rec = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + f".{int(now % 1 * 1000):03d}Z",
        "level": level,
        "component": "worker",
        "node_id": NODE_ID,
        "request_id": request_id,
        "event": event,
    }
    rec.update({k: v for k, v in fields.items() if v is not None})
    stream = sys.stderr if level in ("ERROR",) else sys.stdout
    print(json.dumps(rec, default=str), file=stream, flush=True)
