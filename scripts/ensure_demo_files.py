#!/usr/bin/env python3
"""Create deterministic sample fixtures under demo/files/ for all demo scripts."""
import hashlib
import os
import struct
import sys
import zipfile
import zlib

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FILES_DIR = os.path.join(ROOT, "demo", "files")


def _png_bytes(width: int = 32, height: int = 32) -> bytes:
    """Build a valid deterministic RGBA PNG image using only stdlib zlib."""
    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    rows = []
    for y in range(height):
        row = bytearray([0])  # filter type 0
        for x in range(width):
            row.extend([(x * 8) & 0xFF, (y * 8) & 0xFF, ((x + y) * 4) & 0xFF, 255])
        rows.append(bytes(row))
    raw = b"".join(rows)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b"")


def _deterministic_bytes(size: int, seed: bytes) -> bytes:
    """Fast deterministic pseudo-random bytes using SHA-256 counter expansion."""
    out = bytearray()
    counter = 0
    block = hashlib.sha256(seed).digest()
    # Expand into a 64 KiB pseudo-random tile first, then XOR counter for variety across chunks
    tile = bytearray()
    while len(tile) < 65536:
        block = hashlib.sha256(block + struct.pack(">I", counter)).digest()
        tile.extend(block)
        counter += 1
    tile_bytes = bytes(tile)
    idx = 0
    while len(out) < size:
        need = min(size - len(out), len(tile_bytes))
        salt = hashlib.sha256(seed + struct.pack(">I", idx)).digest()
        # Slightly vary first 32 bytes per 64 KiB block so every 1 MiB chunk has unique SHA-256
        out.extend(salt[:min(32, need)])
        if need > 32:
            out.extend(tile_bytes[32:need])
        idx += 1
    return bytes(out[:size])


def _write_if_needed(path: str, data: bytes) -> None:
    if os.path.exists(path) and os.path.getsize(path) == len(data):
        return
    with open(path, "wb") as f:
        f.write(data)


def ensure_all() -> None:
    os.makedirs(FILES_DIR, exist_ok=True)

    txt = (
        "Distributed Object Store — Demo Sample Text\n"
        "UTF-8 check: αβγδ • café • naive • ✓\n"
        + ("line-payload-for-chunk-verification-0123456789\n" * 40)
    ).encode("utf-8")
    _write_if_needed(os.path.join(FILES_DIR, "sample.txt"), txt)

    _write_if_needed(os.path.join(FILES_DIR, "sample.png"), _png_bytes(48, 48))

    zip_path = os.path.join(FILES_DIR, "archive.zip")
    if not os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.txt", "Distributed Object Store sample zip archive.\n")
            zf.writestr("notes.json", '{"demo": true, "chunks": "1MiB"}\n')

    # 6,500,000 bytes -> 6 full 1,048,576-byte chunks + 1 tail chunk of 208,544 bytes (7 chunks total)
    medium_path = os.path.join(FILES_DIR, "medium_6mb.bin")
    if not (os.path.exists(medium_path) and os.path.getsize(medium_path) == 6_500_000):
        with open(medium_path, "wb") as f:
            f.write(_deterministic_bytes(6_500_000, b"dfs-demo-medium-6mb"))

    # 15,728,640 bytes -> 15 full 1 MiB chunks
    large_path = os.path.join(FILES_DIR, "large_15mb.bin")
    if not (os.path.exists(large_path) and os.path.getsize(large_path) == 15_728_640):
        with open(large_path, "wb") as f:
            f.write(_deterministic_bytes(15_728_640, b"dfs-demo-large-15mb"))


if __name__ == "__main__":
    ensure_all()
    if "--quiet" not in sys.argv:
        for fn in sorted(os.listdir(FILES_DIR)):
            p = os.path.join(FILES_DIR, fn)
            print(f"  {fn}: {os.path.getsize(p)} bytes")
