"""Replica placement. Pure function: no database, no I/O."""
import random
from collections.abc import Mapping
from typing import Iterable, List, Union


class InsufficientWorkersError(Exception):
    def __init__(self, needed: int, available: int):
        self.needed = needed
        self.available = available
        super().__init__(f"need {needed} healthy workers, have {available}")


def _candidates(alive_workers, exclude) -> dict:
    """Normalise input to {worker_id: chunk_count}, keeping only ALIVE, non-excluded workers.

    Accepts either a mapping {worker_id: chunk_count} or an iterable of dicts
    {"worker_id", "chunk_count" (default 0), "status" (default "ALIVE")}.
    """
    excluded = set(exclude)
    out = {}
    if isinstance(alive_workers, Mapping):
        items = ((wid, count, "ALIVE") for wid, count in alive_workers.items())
    else:
        items = (
            (w["worker_id"], w.get("chunk_count", 0), w.get("status", "ALIVE"))
            for w in alive_workers
        )
    for wid, count, status in items:
        if status == "ALIVE" and wid not in excluded:
            out[wid] = int(count)
    return out


def choose_workers(
    alive_workers: Union[Mapping, Iterable[dict]],
    n: int,
    exclude: Iterable[str] = (),
    rng=random,
) -> List[str]:
    """Pick `n` distinct workers: fewest chunks first, random tie-break.

    Raises InsufficientWorkersError(needed=n, available=<eligible count>) if
    fewer than `n` eligible workers exist.
    """
    if n < 0:
        raise ValueError("n must be >= 0")
    candidates = _candidates(alive_workers, exclude)
    if len(candidates) < n:
        raise InsufficientWorkersError(needed=n, available=len(candidates))
    ranked = sorted(candidates.items(), key=lambda kv: (kv[1], rng.random()))
    return [wid for wid, _ in ranked[:n]]

