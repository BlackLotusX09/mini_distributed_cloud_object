import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest

WORKER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "worker")
sys.path.insert(0, WORKER_DIR)

from storage import (  # noqa: E402
    ChecksumMismatchError,
    ChunkConflictError,
    ChunkCorruptionError,
    ChunkTooLargeError,
    InvalidChunkIDException,
    NotFoundError,
    Storage,
    WriteStatus,
    sha256_hex,
)


class StorageTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name
        self.s = Storage(self.dir)

    def tearDown(self):
        self._tmp.cleanup()

    def chunk_file(self, cid):
        return os.path.join(self.dir, "chunks", cid[:2], cid + ".chunk")

    def all_files(self):
        return [os.path.join(d, f) for d, _, fs in os.walk(self.dir) for f in fs]


class IdValidation(StorageTestBase):
    def test_bad_ids(self):
        bad = ["", "../x", "a/b", "..", "a.b", "a b", "a\n", "x" * 129, "ü", "a\x00b", "%2e%2e"]
        for cid in bad:
            with self.subTest(cid=cid):
                for op in (
                    lambda c: self.s.write_chunk(c, b"x"),
                    self.s.read_chunk,
                    self.s.delete_chunk,
                ):
                    with self.assertRaises(InvalidChunkIDException):
                        op(cid)

    def test_good_ids(self):
        for cid in ["a", "abc_DEF-123", "x" * 128, "0" * 64]:
            self.assertEqual(self.s.write_chunk(cid, b"hi").status, WriteStatus.CREATED)

    def test_nothing_written_outside_root(self):
        with self.assertRaises(InvalidChunkIDException):
            self.s.write_chunk("../escape", b"x")
        self.assertFalse(os.path.exists(os.path.join(self.dir, "escape.chunk")))


class WriteReadDelete(StorageTestBase):
    def test_roundtrip_and_layout(self):
        data = os.urandom(5000)
        r = self.s.write_chunk("abc123", data)
        self.assertEqual((r.status, r.size, r.checksum), (WriteStatus.CREATED, 5000, sha256_hex(data)))
        self.assertEqual(self.s.read_chunk("abc123"), data)
        self.assertTrue(os.path.exists(self.chunk_file("abc123")))  # sharded by first 2 chars
        self.assertEqual([f for f in self.all_files() if f.endswith(".tmp")], [])

    def test_empty_chunk(self):
        self.s.write_chunk("empty", b"")
        self.assertEqual(self.s.read_chunk("empty"), b"")

    def test_idempotent_put(self):
        self.s.write_chunk("c1", b"same")
        r = self.s.write_chunk("c1", b"same")
        self.assertEqual(r.status, WriteStatus.ALREADY_EXISTS)

    def test_conflict(self):
        self.s.write_chunk("c1", b"one")
        with self.assertRaises(ChunkConflictError):
            self.s.write_chunk("c1", b"two")
        self.assertEqual(self.s.read_chunk("c1"), b"one")

    def test_expected_checksum_ok_and_mismatch_touches_no_disk(self):
        self.s.write_chunk("ok", b"data", expected_checksum=sha256_hex(b"data").upper())
        with self.assertRaises(ChecksumMismatchError):
            self.s.write_chunk("bad", b"data", expected_checksum=sha256_hex(b"other"))
        with self.assertRaises(ChecksumMismatchError):
            self.s.write_chunk("bad", b"data", expected_checksum="nothex")
        self.assertFalse(os.path.exists(os.path.dirname(self.chunk_file("bad"))))
        with self.assertRaises(NotFoundError):
            self.s.read_chunk("bad")

    def test_size_limit_boundary(self):
        self.s.write_chunk("max", b"\0" * 1_048_576)
        with self.assertRaises(ChunkTooLargeError):
            self.s.write_chunk("over", b"\0" * 1_048_577)
        with self.assertRaises(NotFoundError):
            self.s.read_chunk("over")

    def test_delete_idempotent(self):
        self.s.write_chunk("d1", b"x")
        self.s.delete_chunk("d1")
        self.s.delete_chunk("d1")
        self.s.delete_chunk("never-existed")
        with self.assertRaises(NotFoundError):
            self.s.read_chunk("d1")

    def test_read_missing(self):
        with self.assertRaises(NotFoundError):
            self.s.read_chunk("nope")

    def test_list(self):
        self.s.write_chunk("bb", b"22")
        self.s.write_chunk("aa", b"1")
        listing = self.s.list_chunks()
        self.assertEqual([(m.chunk_id, m.size, m.checksum) for m in listing],
                         [("aa", 1, sha256_hex(b"1")), ("bb", 2, sha256_hex(b"22"))])
        self.assertEqual(self.s.chunk_count(), 2)


class Corruption(StorageTestBase):
    def flip_bit(self, cid):
        p = self.chunk_file(cid)
        raw = bytearray(open(p, "rb").read())
        raw[len(raw) // 2] ^= 0x01
        open(p, "wb").write(bytes(raw))

    def test_single_bit_flip_refused(self):
        self.s.write_chunk("c1", os.urandom(4096))
        self.flip_bit("c1")
        with self.assertRaises(ChunkCorruptionError):
            self.s.read_chunk("c1")

    def test_truncated_refused(self):
        self.s.write_chunk("c1", b"hello world")
        open(self.chunk_file("c1"), "wb").write(b"hello")
        with self.assertRaises(ChunkCorruptionError):
            self.s.read_chunk("c1")

    def test_malformed_sidecar_is_corrupt(self):
        self.s.write_chunk("c1", b"x")
        open(self.chunk_file("c1")[: -len(".chunk")] + ".sha256", "w").write("garbage")
        with self.assertRaises(ChunkCorruptionError):
            self.s.read_chunk("c1")

    def test_payload_without_checksum_is_missing(self):
        self.s.write_chunk("c1", b"x")
        os.unlink(self.chunk_file("c1")[: -len(".chunk")] + ".sha256")
        with self.assertRaises(NotFoundError):
            self.s.read_chunk("c1")
        self.assertEqual(self.s.list_chunks(), [])

    def test_rewrite_heals_damaged_copy_of_same_content(self):
        data = b"A" * 100
        self.s.write_chunk("c1", data)
        self.flip_bit("c1")
        r = self.s.write_chunk("c1", data)
        self.assertEqual(r.status, WriteStatus.CREATED)
        self.assertEqual(self.s.read_chunk("c1"), data)

    def test_delete_then_put_replaces_corrupt_chunk(self):
        self.s.write_chunk("c1", b"good")
        self.flip_bit("c1")
        self.s.delete_chunk("c1")
        self.s.write_chunk("c1", b"good")
        self.assertEqual(self.s.read_chunk("c1"), b"good")


class CrashRecovery(StorageTestBase):
    CHILD = textwrap.dedent(
        """
        import os, sys, time
        sys.path.insert(0, sys.argv[1])
        from storage import Storage
        hang_on = int(sys.argv[3])
        calls = {"n": 0}
        real = os.replace
        def replace(src, dst):
            calls["n"] += 1
            if calls["n"] == hang_on:
                print("READY", flush=True)
                time.sleep(60)
            return real(src, dst)
        os.replace = replace
        Storage(sys.argv[2]).write_chunk("victim", b"x" * 100000)
        """
    )

    def killed_mid_write(self, hang_on):
        p = subprocess.Popen(
            [sys.executable, "-c", self.CHILD, WORKER_DIR, self.dir, str(hang_on)],
            stdout=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(p.stdout.readline().strip(), "READY")
        p.send_signal(signal.SIGKILL)
        p.wait()
        p.stdout.close()

    def test_sigkill_before_payload_rename_leaves_only_tmp(self):
        self.killed_mid_write(hang_on=1)
        files = [os.path.basename(f) for f in self.all_files()]
        self.assertEqual(files, [".victim.chunk.tmp"])
        s = Storage(self.dir)  # restart
        self.assertEqual(s.recovered_tmp_files, 1)
        self.assertEqual(self.all_files(), [])
        with self.assertRaises(NotFoundError):
            s.read_chunk("victim")
        self.assertEqual(s.list_chunks(), [])

    def test_sigkill_between_payload_and_checksum_is_not_a_chunk(self):
        self.killed_mid_write(hang_on=2)
        names = sorted(os.path.basename(f) for f in self.all_files())
        self.assertEqual(names, [".victim.sha256.tmp", "victim.chunk"])
        s = Storage(self.dir)
        with self.assertRaises(NotFoundError):
            s.read_chunk("victim")
        self.assertEqual(s.list_chunks(), [])
        # a retry completes the write
        s.write_chunk("victim", b"x" * 100000)
        self.assertEqual(s.read_chunk("victim"), b"x" * 100000)

    def test_recover_removes_stray_tmp_everywhere(self):
        os.makedirs(os.path.join(self.dir, "chunks", "ab"), exist_ok=True)
        for p in ("chunks/ab/.abc.chunk.tmp", "chunks/.x.tmp", "stray.tmp"):
            open(os.path.join(self.dir, p), "w").write("junk")
        self.assertEqual(Storage(self.dir).recovered_tmp_files, 3)

    def test_data_survives_restart(self):
        self.s.write_chunk("keep", b"persist me")
        self.assertEqual(Storage(self.dir).read_chunk("keep"), b"persist me")


class Concurrency(StorageTestBase):
    def test_concurrent_identical_puts(self):
        import threading

        results = []
        def go():
            results.append(self.s.write_chunk("same", b"payload").status)
        ts = [threading.Thread(target=go) for _ in range(16)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(results.count(WriteStatus.CREATED), 1)
        self.assertEqual(self.s.read_chunk("same"), b"payload")


if __name__ == "__main__":
    unittest.main()

