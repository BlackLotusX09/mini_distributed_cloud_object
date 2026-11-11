import http.client
import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "worker"))

from app import make_server  # noqa: E402
from storage import Storage, sha256_hex  # noqa: E402


class WorkerHTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.storage = Storage(cls._tmp.name)
        cls.server = make_server(cls.storage, "worker1", host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls._tmp.cleanup()

    def req(self, method, path, body=None, headers=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            c.request(method, path, body=body, headers=headers or {})
            r = c.getresponse()
            return r.status, dict(r.getheaders()), r.read()
        finally:
            c.close()

    def assert_error(self, resp, status, code):
        s, h, b = resp
        self.assertEqual(s, status, b)
        self.assertEqual(h["Content-Type"], "application/json")
        j = json.loads(b)
        self.assertEqual(j["error"], code)
        self.assertTrue(j["message"])

    # -- happy path
    def test_put_get_delete_cycle(self):
        data = os.urandom(3000)
        digest = sha256_hex(data)
        s, _, b = self.req("PUT", "/internal/chunks/cyc1", data, {"X-Chunk-Checksum": digest})
        self.assertEqual(s, 201)
        self.assertEqual(json.loads(b), {"chunk_id": "cyc1", "size": 3000, "checksum": digest})
        s, h, b = self.req("PUT", "/internal/chunks/cyc1", data)  # idempotent
        self.assertEqual(s, 200)
        s, h, b = self.req("GET", "/internal/chunks/cyc1")
        self.assertEqual((s, b), (200, data))
        self.assertEqual(h["Content-Type"], "application/octet-stream")
        self.assertEqual(h["X-Chunk-Checksum"], digest)
        self.assertEqual(self.req("DELETE", "/internal/chunks/cyc1")[0], 204)
        self.assertEqual(self.req("DELETE", "/internal/chunks/cyc1")[0], 204)
        self.assert_error(self.req("GET", "/internal/chunks/cyc1"), 404, "CHUNK_NOT_FOUND")

    def test_delete_never_existing_is_204(self):
        s, _, b = self.req("DELETE", "/internal/chunks/ghost")
        self.assertEqual((s, b), (204, b""))

    # -- errors
    def test_checksum_mismatch(self):
        self.assert_error(
            self.req("PUT", "/internal/chunks/cm1", b"abc", {"X-Chunk-Checksum": sha256_hex(b"zzz")}),
            400, "CHECKSUM_MISMATCH")
        self.assert_error(self.req("GET", "/internal/chunks/cm1"), 404, "CHUNK_NOT_FOUND")

    def test_conflict(self):
        self.req("PUT", "/internal/chunks/cf1", b"one")
        self.assert_error(self.req("PUT", "/internal/chunks/cf1", b"two"), 409, "CHUNK_CONFLICT")

    def test_size_boundary(self):
        s, _, _ = self.req("PUT", "/internal/chunks/big_ok", b"\0" * 1_048_576)
        self.assertEqual(s, 201)
        self.assert_error(self.req("PUT", "/internal/chunks/big_bad", b"\0" * 1_048_577), 400, "CHUNK_TOO_LARGE")
        self.assert_error(self.req("GET", "/internal/chunks/big_bad"), 404, "CHUNK_NOT_FOUND")
        self.assertEqual(len(self.req("GET", "/internal/chunks/big_ok")[2]), 1_048_576)

    def test_invalid_ids(self):
        paths = [
            "/internal/chunks/../etc/passwd",
            "/internal/chunks/..",
            "/internal/chunks/%2e%2e%2fsecret",
            "/internal/chunks/a%2Fb",
            "/internal/chunks/a/b",
            "/internal/chunks/a.b",
            "/internal/chunks/a%20b",
            "/internal/chunks/a%00b",
            "/internal/chunks/",
            "/internal/chunks/" + "x" * 129,
        ]
        for p in paths:
            for method in ("PUT", "GET", "DELETE"):
                with self.subTest(path=p, method=method):
                    body = b"data" if method == "PUT" else None
                    self.assert_error(self.req(method, p, body), 400, "INVALID_CHUNK_ID")

    def test_put_requires_content_length(self):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        c.putrequest("PUT", "/internal/chunks/nolen")
        c.putheader("Transfer-Encoding", "chunked")
        c.endheaders()
        c.send(b"4\r\ndata\r\n0\r\n\r\n")
        r = c.getresponse()
        self.assertEqual(r.status, 411)
        c.close()

    def test_unknown_route_and_method(self):
        self.assert_error(self.req("GET", "/nope"), 404, "NOT_FOUND")
        self.assert_error(self.req("POST", "/internal/chunks/x", b""), 405, "METHOD_NOT_ALLOWED")

    # -- corruption
    def test_bit_flip_returns_error_and_no_bytes(self):
        data = os.urandom(2048)
        self.req("PUT", "/internal/chunks/flip1", data)
        p = os.path.join(self.storage.root, "fl", "flip1.chunk")
        raw = bytearray(open(p, "rb").read())
        raw[100] ^= 0x01
        open(p, "wb").write(bytes(raw))
        s, h, b = self.req("GET", "/internal/chunks/flip1")
        self.assert_error((s, h, b), 409, "CHUNK_CORRUPT")
        self.assertNotIn("X-Chunk-Checksum", h)
        self.assertNotIn(bytes(raw)[:64], b)
        # repair path: delete then re-put good data
        self.req("DELETE", "/internal/chunks/flip1")
        self.assertEqual(self.req("PUT", "/internal/chunks/flip1", data)[0], 201)
        self.assertEqual(self.req("GET", "/internal/chunks/flip1")[2], data)

    # -- listing / health
    def test_list_chunks(self):
        self.req("PUT", "/internal/chunks/lst1", b"hello")
        s, _, b = self.req("GET", "/internal/workers/worker1/chunks")
        self.assertEqual(s, 200)
        j = json.loads(b)
        self.assertEqual(j["worker_id"], "worker1")
        self.assertIn({"chunk_id": "lst1", "size": 5, "checksum": sha256_hex(b"hello")}, j["chunks"])

    def test_list_foreign_worker(self):
        self.assert_error(self.req("GET", "/internal/workers/worker2/chunks"), 404, "WORKER_NOT_FOUND")

    def test_health(self):
        s, _, b = self.req("GET", "/health")
        j = json.loads(b)
        self.assertEqual(s, 200)
        self.assertEqual(j["worker_id"], "worker1")
        self.assertEqual(j["status"], "healthy")
        self.assertIsInstance(j["chunk_count"], int)
        self.assertGreater(j["free_bytes"], 0)


if __name__ == "__main__":
    unittest.main()

