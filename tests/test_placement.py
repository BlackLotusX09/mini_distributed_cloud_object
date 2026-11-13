import os
import random
import sys
from collections import Counter

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "controller"))

from placement import InsufficientWorkersError, choose_workers  # noqa: E402


def workers(spec):
    """spec: {id: (count, status)} -> list of dicts."""
    return [{"worker_id": w, "chunk_count": c, "status": s} for w, (c, s) in spec.items()]


ALL4 = {f"worker{i}": (0, "ALIVE") for i in range(1, 5)}


def test_returns_n_distinct_workers():
    for n in (1, 2, 3, 4):
        chosen = choose_workers(workers(ALL4), n)
        assert len(chosen) == n and len(set(chosen)) == n


def test_accepts_mapping_input():
    chosen = choose_workers({"a": 5, "b": 1, "c": 3}, 2)
    assert chosen == ["b", "c"]


def test_never_selects_dead_workers():
    spec = {**ALL4, "worker2": (0, "DEAD"), "worker3": (0, "DEAD")}
    for _ in range(100):
        assert set(choose_workers(workers(spec), 2)) == {"worker1", "worker4"}


def test_never_selects_excluded_workers():
    for _ in range(100):
        chosen = choose_workers(workers(ALL4), 2, exclude=("worker1", "worker2"))
        assert set(chosen) == {"worker3", "worker4"}


def test_exclude_accepts_any_iterable():
    chosen = choose_workers(workers(ALL4), 3, exclude={"worker1"})
    assert "worker1" not in chosen


@pytest.mark.parametrize("alive", [0, 1])
def test_insufficient_workers_when_too_few_alive(alive):
    spec = {f"worker{i}": (0, "ALIVE" if i <= alive else "DEAD") for i in range(1, 5)}
    with pytest.raises(InsufficientWorkersError) as e:
        choose_workers(workers(spec), 2)
    assert (e.value.needed, e.value.available) == (2, alive)
    assert str(e.value) == f"need 2 healthy workers, have {alive}"


def test_insufficient_when_exclusions_leave_too_few():
    with pytest.raises(InsufficientWorkersError) as e:
        choose_workers(workers(ALL4), 2, exclude=("worker1", "worker2", "worker3"))
    assert e.value.available == 1


def test_empty_input_raises():
    with pytest.raises(InsufficientWorkersError) as e:
        choose_workers([], 2)
    assert e.value.available == 0


def test_prefers_lowest_chunk_count():
    spec = {"w1": (10, "ALIVE"), "w2": (2, "ALIVE"), "w3": (7, "ALIVE"), "w4": (1, "ALIVE")}
    for _ in range(50):
        assert choose_workers(workers(spec), 1) == ["w4"]
        assert set(choose_workers(workers(spec), 2)) == {"w4", "w2"}


def test_ties_broken_among_equal_counts_only():
    spec = {"w1": (0, "ALIVE"), "w2": (0, "ALIVE"), "w3": (5, "ALIVE"), "w4": (5, "ALIVE")}
    seen = set()
    for _ in range(200):
        chosen = choose_workers(workers(spec), 1)
        assert chosen[0] in ("w1", "w2")
        seen.add(chosen[0])
    assert seen == {"w1", "w2"}  # tie-break is random, not fixed


def test_is_pure_does_not_mutate_input():
    data = workers(ALL4)
    snapshot = [dict(d) for d in data]
    choose_workers(data, 2)
    assert data == snapshot


def test_module_has_no_database_dependency():
    import placement

    assert "sqlite3" not in vars(placement) and "db" not in vars(placement)


def test_balance_with_random_tiebreak_static_counts():
    """All counts equal and never updated: pure tie-break must still be balanced."""
    rng = random.Random(1234)
    c = Counter()
    for _ in range(1000):
        c.update(choose_workers(workers(ALL4), 1, rng=rng))
    assert max(c.values()) / 1000 <= 0.35, c
    assert len(c) == 4


def test_balance_over_1000_placements_with_feedback():
    """Simulate real use: n=2, counts grow as chunks are placed."""
    rng = random.Random(42)
    counts = {w: 0 for w in ALL4}
    for _ in range(1000):
        for w in choose_workers(counts, 2, rng=rng):
            counts[w] += 1
    total = sum(counts.values())
    assert total == 2000
    assert max(counts.values()) / total <= 0.35, counts
    assert max(counts.values()) - min(counts.values()) <= 1  # least-chunks-first is near-perfect

