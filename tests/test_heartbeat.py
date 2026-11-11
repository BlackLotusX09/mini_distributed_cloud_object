import json
import os
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "worker"))

import config as config_mod  # noqa: E402
from config import Config, ConfigError  # noqa: E402
from heartbeat import HeartbeatDaemon  # noqa: E402
from storage import Storage  # noqa: E402


class ConfigTest(unittest.TestCase):
    def test_defaults_and_address(self):
        c = config_mod.load({"WORKER_ID": "worker1"})
        self.assertEqual((c.data_dir, c.port, c.heartbeat_interval), ("/data", 8000, 5.0))
        self.assertEqual(c.address, "http://worker1:8000")
        self.assertNotIn("localhost", c.address)
        self.assertNotIn("127.0.0.1", c.address)

    def test_overrides(self):
        c = config_mod.load({"WORKER_ID": "w9", "PORT": "9000", "DATA_DIR": "/v",
                             "CONTROLLER_URL": "http://c:1/", "HEARTBEAT_INTERVAL": "500ms"})
        self.assertEqual((c.port, c.data_dir, c.controller_url, c.heartbeat_interval),
                         (9000, "/v", "http://c:1", 0.5))

    def test_durations(self):
        self.assertEqual(config_mod.parse_duration("5s"), 5)
        self.assertEqual(config_mod.parse_duration("2m"), 120)
        self.assertEqual(config_mod.parse_duration("3"), 3)

    def test_invalid(self):
        for env in [{}, {"WORKER_ID": "../x"}, {"WORKER_ID": "a", "PORT": "x"},
                    {"WORKER_ID": "a", "PORT": "0"}, {"WORKER_ID": "a", "HEARTBEAT_INTERVAL": "soon"}]:
            with self.subTest(env=env), self.assertRaises(ConfigError):
                config_mod.load(env)


class FakeController:
    """Scriptable controller: register_script / heartbeat_script are lists of status codes."""

    def __init__(self, register_script=(), heartbeat_script=()):
        self.register_script = list(register_script)
        self.heartbeat_script = list(heartbeat_script)
        self.registers, self.heartbeats = [], []
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path.endswith("/register"):
                    outer.registers.append((time.monotonic(), body))
                    script = outer.register_script
                else:
                    outer.heartbeats.append((time.monotonic(), body))
                    script = outer.heartbeat_script
                status = script.pop(0) if script else 200
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def wait_for(pred, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


class HeartbeatTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.storage = Storage(self._tmp.name)
        self.storage.write_chunk("c1", b"x")
        self.daemons, self.ctrls = [], []

    def tearDown(self):
        for d in self.daemons:
            d.stop()
            d.join(2)
        for c in self.ctrls:
            c.close()
        self._tmp.cleanup()

    def start(self, ctrl, interval=0.05, base=0.05, cap=0.2):
        self.ctrls.append(ctrl)
        cfg = Config("worker1", self._tmp.name, ctrl.url, 8000, interval)
        d = HeartbeatDaemon(cfg, self.storage, base_backoff=base, max_backoff=cap, timeout=1)
        self.daemons.append(d)
        d.start()
        return d

    def test_register_then_heartbeat_payloads(self):
        ctrl = FakeController()
        self.start(ctrl)
        self.assertTrue(wait_for(lambda: len(ctrl.heartbeats) >= 2))
        reg = ctrl.registers[0][1]
        self.assertEqual(reg["worker_id"], "worker1")
        self.assertEqual(reg["address"], "http://worker1:8000")
        self.assertEqual(reg["chunk_count"], 1)
        self.assertGreater(reg["free_bytes"], 0)
        hb = ctrl.heartbeats[0][1]
        self.assertEqual(set(hb), {"worker_id", "timestamp", "free_bytes", "chunk_count"})
        self.assertEqual(len(ctrl.registers), 1)

    def test_registration_retries_with_exponential_backoff(self):
        ctrl = FakeController(register_script=[503, 503, 503, 503])
        self.start(ctrl, base=0.05, cap=0.2)
        self.assertTrue(wait_for(lambda: len(ctrl.heartbeats) >= 1))
        times = [t for t, _ in ctrl.registers]
        self.assertEqual(len(times), 5)
        gaps = [b - a for a, b in zip(times, times[1:])]
        for gap, expected in zip(gaps, [0.05, 0.1, 0.2, 0.2]):  # doubles, capped
            self.assertGreaterEqual(gap, expected * 0.9)
        self.assertLess(gaps[3], 0.2 * 3)

    def test_survives_controller_down_then_registers(self):
        ctrl = FakeController()
        port = ctrl.server.server_address[1]
        ctrl.close()  # nothing listens: connection refused
        cfg = Config("worker1", self._tmp.name, f"http://127.0.0.1:{port}", 8000, 0.05)
        d = HeartbeatDaemon(cfg, self.storage, base_backoff=0.05, max_backoff=0.1, timeout=1)
        self.daemons.append(d)
        d.start()
        time.sleep(0.4)
        self.assertTrue(d.is_alive())  # never crashes
        # bring a controller up on the same port
        from http.server import ThreadingHTTPServer as S
        revived = FakeController.__new__(FakeController)
        revived.registers, revived.heartbeats = [], []
        revived.register_script = revived.heartbeat_script = []
        outer = revived

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                (outer.registers if self.path.endswith("/register") else outer.heartbeats).append(1)
                self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
            def log_message(self, *a): pass

        revived.server = S(("127.0.0.1", port), H)
        threading.Thread(target=revived.server.serve_forever, daemon=True).start()
        self.ctrls.append(revived)
        self.assertTrue(wait_for(lambda: len(revived.heartbeats) >= 1))
        self.assertEqual(len(revived.registers), 1)

    def test_reregisters_when_controller_says_unknown(self):
        ctrl = FakeController(heartbeat_script=[200, 404])
        self.start(ctrl)
        self.assertTrue(wait_for(lambda: len(ctrl.registers) >= 2))
        self.assertTrue(wait_for(lambda: len(ctrl.heartbeats) >= 3))

    def test_heartbeat_errors_keep_cadence_without_reregistering(self):
        ctrl = FakeController(heartbeat_script=[500, 500, 200])
        self.start(ctrl)
        self.assertTrue(wait_for(lambda: len(ctrl.heartbeats) >= 4))
        self.assertEqual(len(ctrl.registers), 1)


if __name__ == "__main__":
    unittest.main()

