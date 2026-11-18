#!/usr/bin/env python3
"""Command-line interface for the Distributed Object Store.

Dependency-free (Python standard library only) with aligned tables by default
and raw JSON output via --json.
"""
import argparse
import hashlib
import http.client
import json
import mimetypes
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

CHUNK_STREAM_BYTES = 1_048_576  # 1 MiB


class CliError(Exception):
    def __init__(self, message: str, exit_code: int = 1):
        super().__init__(message)
        self.exit_code = exit_code


def _default_url() -> str:
    return (
        os.environ.get("OBJSTORE_URL")
        or os.environ.get("CONTROLLER_URL")
        or "http://localhost:8000"
    ).rstrip("/")


def _fmt_ts(ts) -> str:
    if ts is None:
        return "-"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError):
        return str(ts)


def _fmt_age(sec) -> str:
    if sec is None:
        return "-"
    try:
        return f"{float(sec):.1f}s ago"
    except (TypeError, ValueError):
        return str(sec)


def _fmt_bytes(n) -> str:
    if n is None:
        return "-"
    n = int(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n} B" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n} B"


def _table(headers, rows) -> str:
    widths = [len(str(h)) for h in headers]
    srows = [[str(cell) for cell in row] for row in rows]
    for row in srows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    fmt = "| " + " | ".join(f"{{:<{w}}}" for w in widths) + " |"
    out = [sep, fmt.format(*headers), sep]
    for row in srows:
        out.append(fmt.format(*row))
    out.append(sep)
    return "\n".join(out)


def _kv_table(pairs) -> str:
    return _table(["Field", "Value"], pairs)


def _format_api_error(status: int, raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "error" in obj:
            return f"HTTP {status} [{obj['error']}]: {obj.get('message', '')}"
    except ValueError:
        pass
    return f"HTTP {status}: {text or 'request failed'}"


def _request_json(base_url: str, method: str, path: str, body=None, headers=None, timeout: float = 60.0):
    url = base_url.rstrip("/") + path
    hdrs = {"Accept": "application/json", **(headers or {})}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise CliError(_format_api_error(e.code, e.read()), exit_code=1) from None
    except urllib.error.URLError as e:
        raise CliError(f"connection error ({url}): {e.reason}", exit_code=2) from None
    except Exception as e:
        raise CliError(f"request failed ({url}): {e}", exit_code=2) from None


def _stream_multipart_upload(base_url: str, file_path: str, custom_meta: dict, timeout: float = 300.0):
    if not os.path.isfile(file_path):
        raise CliError(f"file not found: {file_path}", exit_code=2)
    parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise CliError(f"invalid controller URL: {base_url}", exit_code=2)

    boundary = f"----objstore-{uuid.uuid4().hex}"
    filename = os.path.basename(file_path) or "upload.bin"
    safe_filename = filename.replace('"', "_").replace("\r", "_").replace("\n", "_")
    ctype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    file_size = os.path.getsize(file_path)

    prefix_parts = []
    if custom_meta:
        meta_json = json.dumps(custom_meta)
        prefix_parts.append(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="metadata"\r\n'
                f"Content-Type: application/json\r\n\r\n"
                f"{meta_json}\r\n"
            ).encode("utf-8")
        )
    prefix_parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{safe_filename}"\r\n'
            f"Content-Type: {ctype}\r\n\r\n"
        ).encode("utf-8")
    )
    head = b"".join(prefix_parts)
    tail = f"\r\n--{boundary}--\r\n".encode("utf-8")
    content_length = len(head) + file_size + len(tail)

    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    base_path = parsed.path.rstrip("/")
    conn = conn_cls(parsed.hostname, parsed.port, timeout=timeout)
    try:
        conn.putrequest("POST", f"{base_path}/api/v1/objects")
        conn.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
        conn.putheader("Content-Length", str(content_length))
        conn.putheader("Accept", "application/json")
        conn.endheaders()
        conn.send(head)
        with open(file_path, "rb") as f:
            while True:
                blk = f.read(CHUNK_STREAM_BYTES)
                if not blk:
                    break
                conn.send(blk)
        conn.send(tail)
        resp = conn.getresponse()
        raw = resp.read()
        if not (200 <= resp.status < 300):
            raise CliError(_format_api_error(resp.status, raw), exit_code=1)
        return json.loads(raw.decode("utf-8"))
    except CliError:
        raise
    except OSError as e:
        raise CliError(f"connection error ({base_url}): {e}", exit_code=2) from None
    except Exception as e:
        raise CliError(f"upload failed: {e}", exit_code=2) from None
    finally:
        conn.close()


def _parse_kv_pairs(items, allow_null: bool = False):
    out = {}
    for item in items or []:
        if "=" not in item:
            raise CliError(f"expected key=value pair, got {item!r}", exit_code=2)
        k, v = item.split("=", 1)
        if not k:
            raise CliError(f"empty key in {item!r}", exit_code=2)
        if allow_null and (v == "" or v == "null"):
            out[k] = None
        else:
            out[k] = v
    return out


# ------------------------------------------------------------ subcommands
def cmd_upload(args, base_url: str, as_json: bool) -> int:
    meta = _parse_kv_pairs(args.meta, allow_null=False)
    res = _stream_multipart_upload(base_url, args.file, meta)
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    rows = [
        ("object_id", res["object_id"]),
        ("name", res["name"]),
        ("size", f"{res['size']} ({_fmt_bytes(res['size'])})"),
        ("content_type", res.get("content_type") or "-"),
        ("chunk_count", res["chunk_count"]),
        ("checksum", res["checksum"]),
        ("created_at", _fmt_ts(res.get("created_at"))),
        ("metadata", json.dumps(res.get("metadata") or {})),
    ]
    print(_kv_table(rows))
    return 0


def cmd_download(args, base_url: str, as_json: bool) -> int:
    oid = args.object_id
    meta = _request_json(base_url, "GET", f"/api/v1/objects/{urllib.parse.quote(oid, safe='')}/metadata")
    expected_sha = meta.get("checksum")
    out_path = args.output
    if not out_path:
        default_name = os.path.basename((meta.get("name") or "").replace("\\", "/")) or oid
        out_path = default_name

    url = base_url.rstrip("/") + f"/api/v1/objects/{urllib.parse.quote(oid, safe='')}"
    req = urllib.request.Request(url, method="GET")
    hasher = hashlib.sha256()
    written = 0
    tmp_path = out_path + ".part"
    try:
        with urllib.request.urlopen(req, timeout=300.0) as resp:
            with open(tmp_path, "wb") as out_f:
                while True:
                    blk = resp.read(CHUNK_STREAM_BYTES)
                    if not blk:
                        break
                    out_f.write(blk)
                    hasher.update(blk)
                    written += len(blk)
    except urllib.error.HTTPError as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise CliError(_format_api_error(e.code, e.read()), exit_code=1) from None
    except urllib.error.URLError as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise CliError(f"connection error ({url}): {e.reason}", exit_code=2) from None
    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise CliError(f"download failed: {e}", exit_code=2) from None

    actual_sha = hasher.hexdigest()
    if expected_sha and actual_sha != expected_sha:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise CliError(
            f"Checksum: {actual_sha} != {expected_sha} (MISMATCH)",
            exit_code=1,
        )

    os.replace(tmp_path, out_path)
    if as_json:
        print(json.dumps({
            "object_id": oid,
            "output_path": out_path,
            "size": written,
            "checksum": actual_sha,
            "verified": True,
        }, indent=2))
        return 0

    print(f"Downloaded {oid} -> {out_path} ({_fmt_bytes(written)})")
    print(f"Checksum: {actual_sha} (VERIFIED)")
    return 0


def cmd_info(args, base_url: str, as_json: bool) -> int:
    oid = urllib.parse.quote(args.object_id, safe="")
    res = _request_json(base_url, "GET", f"/api/v1/objects/{oid}/metadata")
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    rows = [
        ("object_id", res["object_id"]),
        ("name", res["name"]),
        ("size", f"{res['size']} ({_fmt_bytes(res['size'])})"),
        ("content_type", res.get("content_type") or "-"),
        ("chunk_count", res["chunk_count"]),
        ("checksum", res.get("checksum") or "-"),
        ("created_at", _fmt_ts(res.get("created_at"))),
        ("modified_at", _fmt_ts(res.get("modified_at"))),
    ]
    print(_kv_table(rows))
    custom = res.get("custom") or {}
    if custom:
        print("\nCustom Metadata:")
        print(_table(["Key", "Value"], sorted(custom.items())))
    else:
        print("\nCustom Metadata: (none)")
    return 0


def cmd_patch(args, base_url: str, as_json: bool) -> int:
    patch = _parse_kv_pairs(args.pairs, allow_null=True)
    oid = urllib.parse.quote(args.object_id, safe="")
    res = _request_json(base_url, "PATCH", f"/api/v1/objects/{oid}/metadata", body=patch)
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    print(f"Updated object {res['object_id']} (modified_at={_fmt_ts(res.get('modified_at'))})")
    custom = res.get("custom") or {}
    if custom:
        print(_table(["Key", "Value"], sorted(custom.items())))
    else:
        print("Custom Metadata: (none)")
    return 0


def cmd_list(args, base_url: str, as_json: bool) -> int:
    qs = urllib.parse.urlencode({"limit": args.limit, "offset": args.offset})
    res = _request_json(base_url, "GET", f"/api/v1/objects?{qs}")
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    objs = res.get("objects") or []
    rows = [
        [
            o["object_id"],
            o["name"][:32],
            f"{o['size']} ({_fmt_bytes(o['size'])})",
            o["chunk_count"],
            (o.get("checksum") or "")[:12],
            _fmt_ts(o.get("created_at")),
        ]
        for o in objs
    ]
    if rows:
        print(_table(["Object ID", "Name", "Size", "Chunks", "SHA256[:12]", "Created At"], rows))
    else:
        print("(no objects found)")
    total = int(res.get("total", 0))
    offset = int(res.get("offset", 0))
    if total == 0 or not objs:
        print(f"Showing 0 of {total}")
    else:
        start = offset + 1
        end = offset + len(objs)
        print(f"Showing {start}–{end} of {total}")
    return 0


def cmd_delete(args, base_url: str, as_json: bool) -> int:
    oid = urllib.parse.quote(args.object_id, safe="")
    _request_json(base_url, "DELETE", f"/api/v1/objects/{oid}")
    if as_json:
        print(json.dumps({"object_id": args.object_id, "deleted": True}, indent=2))
    else:
        print(f"Deleted object {args.object_id}")
    return 0


def cmd_workers(args, base_url: str, as_json: bool) -> int:
    res = _request_json(base_url, "GET", "/api/v1/admin/workers")
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    rows = [
        [
            w["worker_id"],
            w["status"],
            w["address"],
            _fmt_age(w.get("last_heartbeat_age")),
            w.get("chunk_count", 0),
        ]
        for w in res
    ] or [["(none)", "-", "-", "-", 0]]
    print(_table(["worker_id", "status", "address", "last_heartbeat", "chunks_stored"], rows))
    return 0


def cmd_chunks(args, base_url: str, as_json: bool) -> int:
    oid = urllib.parse.quote(args.object_id, safe="")
    res = _request_json(base_url, "GET", f"/api/v1/admin/objects/{oid}/chunks")
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    print(f"Object: {res['object_id']} ({res.get('name', '-')}, status={res.get('status', '-')})")
    rows = []
    for ch in res.get("chunks") or []:
        reps = ", ".join(
            f"{r['worker_id']}:{r['state']}({r['worker_status']})"
            for r in ch.get("replicas") or []
        ) or "(none)"
        rows.append([
            ch["index"],
            ch["chunk_id"],
            ch["size"],
            (ch.get("checksum") or "")[:16],
            reps,
        ])
    if not rows:
        rows = [["-", "(no chunks / 0 bytes)", 0, "-", "-"]]
    print(_table(["Index", "Chunk ID", "Size (B)", "SHA256[:16]", "Replicas"], rows))
    return 0


def cmd_replication(args, base_url: str, as_json: bool) -> int:
    res = _request_json(base_url, "GET", "/api/v1/admin/replication")
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    print("=== Cluster Replication ===")
    print(_table(
        ["Total", "Healthy", "Under-Repl", "Unavailable", "Unrecoverable", "Over-Repl"],
        [[
            res["total_chunks"],
            res["healthy"],
            res["under_replicated"],
            res["unavailable"],
            res["unrecoverable"],
            res["over_replicated"],
        ]],
    ))
    scrub = res.get("scrub") or {}
    print("\n=== Scrub Status ===")
    print(_table(
        ["Last Scrub At", "Chunks Checked", "Corrupt Found"],
        [[_fmt_ts(scrub.get("last_scrub_at")), scrub.get("chunks_checked", 0), scrub.get("corrupt_found", 0)]],
    ))
    print("\n=== Worker Replicas ===")
    wrows = [
        [
            w["worker_id"],
            w["status"],
            w["valid_replicas"],
            w["corrupt_replicas"],
            w["pending_delete_replicas"],
        ]
        for w in res.get("workers") or []
    ] or [["(none)", "-", 0, 0, 0]]
    print(_table(["Worker ID", "Status", "Valid (OK)", "Corrupt", "Pending Delete"], wrows))
    return 0


def cmd_scrub(args, base_url: str, as_json: bool) -> int:
    res = _request_json(base_url, "POST", "/api/v1/admin/scrub", timeout=120.0)
    if as_json:
        print(json.dumps(res, indent=2))
        return 0
    print(_kv_table([
        ("workers_scrubbed", res.get("workers_scrubbed", 0)),
        ("chunks_checked", res.get("chunks_checked", 0)),
        ("corrupt_found", res.get("corrupt_found", 0)),
        ("repairs_triggered", res.get("repairs_triggered", False)),
    ]))
    return 0


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", default=argparse.SUPPRESS,
                        help="Controller base URL (default: $OBJSTORE_URL or http://localhost:8000)")
    common.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                        help="Output raw JSON instead of tables")

    parser = argparse.ArgumentParser(
        prog="objstore",
        description="Distributed Object Store CLI",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_up = sub.add_parser("upload", parents=[common], help="Upload a file")
    p_up.add_argument("file", help="Path to local file")
    p_up.add_argument("--meta", action="append", default=[], metavar="k=v",
                      help="Custom metadata key=value pair (repeatable)")
    p_up.set_defaults(func=cmd_upload)

    p_dl = sub.add_parser("download", parents=[common], help="Download an object and verify SHA-256")
    p_dl.add_argument("object_id", help="Object UUID4")
    p_dl.add_argument("-o", "--output", help="Output file path (default: stored filename)")
    p_dl.set_defaults(func=cmd_download)

    p_info = sub.add_parser("info", parents=[common], help="Show object metadata")
    p_info.add_argument("object_id", help="Object UUID4")
    p_info.set_defaults(func=cmd_info)

    p_patch = sub.add_parser("patch", parents=[common], help="Merge-patch custom metadata (k= or k=null deletes)")
    p_patch.add_argument("object_id", help="Object UUID4")
    p_patch.add_argument("pairs", nargs="+", metavar="k=v", help="Key-value pairs")
    p_patch.set_defaults(func=cmd_patch)

    p_list = sub.add_parser("list", parents=[common], help="List committed objects")
    p_list.add_argument("--limit", type=int, default=50, help="Page size (1..200)")
    p_list.add_argument("--offset", type=int, default=0, help="Pagination offset")
    p_list.set_defaults(func=cmd_list)

    p_del = sub.add_parser("delete", parents=[common], help="Delete an object")
    p_del.add_argument("object_id", help="Object UUID4")
    p_del.set_defaults(func=cmd_delete)

    p_workers = sub.add_parser("workers", parents=[common], help="List worker status")
    p_workers.set_defaults(func=cmd_workers)

    p_chunks = sub.add_parser("chunks", parents=[common], help="Inspect chunk/replica placement for an object")
    p_chunks.add_argument("object_id", help="Object UUID4")
    p_chunks.set_defaults(func=cmd_chunks)

    p_rep = sub.add_parser("replication", parents=[common], help="Show cluster replication health")
    p_rep.set_defaults(func=cmd_replication)

    p_scrub = sub.add_parser("scrub", parents=[common], help="Trigger immediate cluster scrub")
    p_scrub.set_defaults(func=cmd_scrub)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    base_url = getattr(args, "url", _default_url())
    as_json = bool(getattr(args, "json", False))
    try:
        return args.func(args, base_url, as_json)
    except CliError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return e.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
