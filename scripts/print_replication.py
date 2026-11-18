#!/usr/bin/env python3
"""Query controller admin endpoints and render replication health and chunk placement tables."""
import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GET {url} -> HTTP {e.code}: {body}") from e
    except Exception as e:
        raise RuntimeError(f"GET {url} failed: {e}") from e


def _table(headers, rows):
    widths = [len(h) for h in headers]
    str_rows = [[str(cell) for cell in row] for row in rows]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    fmt = "| " + " | ".join(f"{{:<{w}}}" for w in widths) + " |"
    lines = [sep, fmt.format(*headers), sep]
    for row in str_rows:
        lines.append(fmt.format(*row))
    lines.append(sep)
    return "\n".join(lines)


def _fmt_ts(ts):
    if ts is None:
        return "never"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> int:
    parser = argparse.ArgumentParser(description="Display DFS replication health and chunk placement.")
    parser.add_argument("--url", default="http://localhost:8000", help="Controller base URL")
    parser.add_argument("object_ids", nargs="*", help="Optional object ID(s) to inspect")
    args = parser.parse_args()
    base = args.url.rstrip("/")

    rep = _get_json(f"{base}/api/v1/admin/replication")

    print("=== Cluster Replication Summary ===")
    summary_rows = [[
        rep.get("total_chunks", 0),
        rep.get("healthy", 0),
        rep.get("under_replicated", 0),
        rep.get("unavailable", 0),
        rep.get("unrecoverable", 0),
        rep.get("over_replicated", 0),
    ]]
    print(_table(
        ["Total Chunks", "Healthy (2)", "Under-Repl (1)", "Unavailable", "Unrecoverable", "Over-Repl (>2)"],
        summary_rows,
    ))

    scrub = rep.get("scrub") or {}
    print("\n=== Last Scrub Status ===")
    print(_table(
        ["Last Scrub At", "Chunks Checked", "Corrupt Found"],
        [[_fmt_ts(scrub.get("last_scrub_at")), scrub.get("chunks_checked", 0), scrub.get("corrupt_found", 0)]],
    ))

    workers = rep.get("workers") or []
    print("\n=== Worker Replica Health ===")
    w_rows = [
        [
            w["worker_id"],
            w["status"],
            w.get("valid_replicas", 0),
            w.get("corrupt_replicas", 0),
            w.get("pending_delete_replicas", 0),
        ]
        for w in workers
    ] or [["(none)", "-", 0, 0, 0]]
    print(_table(["Worker ID", "Status", "Valid (OK)", "Corrupt", "Pending Delete"], w_rows))

    object_ids = list(args.object_ids)
    if not object_ids:
        objs = _get_json(f"{base}/api/v1/objects?limit=200").get("objects") or []
        object_ids = [o["object_id"] for o in objs]

    for oid in object_ids:
        data = _get_json(f"{base}/api/v1/admin/objects/{oid}/chunks")
        print(f"\n=== Object {oid} ({data.get('name', '?')}, status={data.get('status', '?')}) ===")
        c_rows = []
        for ch in data.get("chunks") or []:
            rep_str = ", ".join(
                f"{r['worker_id']}:{r['state']}({r['worker_status']})"
                for r in ch.get("replicas") or []
            ) or "(none)"
            csum = (ch.get("checksum") or "")[:12]
            c_rows.append([ch["index"], ch["chunk_id"], ch["size"], csum, rep_str])
        if not c_rows:
            c_rows = [["-", "(no chunks / 0 bytes)", 0, "-", "-"]]
        print(_table(["Idx", "Chunk ID", "Size", "SHA256[:12]", "Replicas"], c_rows))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
