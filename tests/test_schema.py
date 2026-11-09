import os
import sqlite3
import unittest

SCHEMA = os.path.join(os.path.dirname(__file__), "..", "controller", "schema.sql")


def fresh():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(open(SCHEMA).read())
    return conn


class SchemaTest(unittest.TestCase):
    def test_tables_exist(self):
        names = {r[0] for r in fresh().execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"objects", "chunks", "replicas", "workers", "custom_metadata"} <= names)

    def test_idempotent(self):
        conn = fresh()
        conn.executescript(open(SCHEMA).read())

    def test_cascade_delete(self):
        c = fresh()
        c.execute("INSERT INTO objects VALUES ('o','f',1,'x','h',1,NULL,NULL)")
        c.execute("INSERT INTO chunks VALUES ('o_0','o',0,1,'h')")
        c.execute("INSERT INTO workers (id,address) VALUES ('worker1','http://worker1:8001')")
        c.execute("INSERT INTO replicas (chunk_id,worker_id) VALUES ('o_0','worker1')")
        c.execute("INSERT INTO custom_metadata VALUES ('o','k','v')")
        c.execute("DELETE FROM objects WHERE object_id='o'")
        for t in ("chunks", "replicas", "custom_metadata"):
            self.assertEqual(c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0], 0)

    def test_replica_requires_worker(self):
        c = fresh()
        c.execute("INSERT INTO objects VALUES ('o','f',1,'x','h',1,NULL,NULL)")
        c.execute("INSERT INTO chunks VALUES ('o_0','o',0,1,'h')")
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO replicas (chunk_id,worker_id) VALUES ('o_0','nope')")

    def test_bad_state_rejected(self):
        c = fresh()
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO workers (id,address,status) VALUES ('w','a','WAT')")


if __name__ == "__main__":
    unittest.main()
