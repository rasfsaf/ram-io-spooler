import os
import shutil
import tempfile
import time
import unittest

from spooler.config import Config, SpoolerConfig
from spooler.read_cache import RAMReadCache
from spooler.server import SpoolerServer


class TestReadCache(unittest.TestCase):
    def setUp(self):
        self.cache = RAMReadCache(max_bytes=1024 * 1024, max_file_size=64 * 1024, ttl_seconds=2.0)

    def test_put_and_get(self):
        path = "/tmp/test.txt"
        data = b"hello cached world"
        self.cache.put(path, data)

        res = self.cache.get(path)
        self.assertEqual(res, data)
        self.assertEqual(self.cache.stats["hits"], 1)
        self.assertEqual(self.cache.stats["items_count"], 1)

    def test_invalidate(self):
        path = "/tmp/test.txt"
        self.cache.put(path, b"data")
        self.assertIsNotNone(self.cache.get(path))

        self.cache.invalidate(path)
        self.assertIsNone(self.cache.get(path))
        self.assertEqual(self.cache.stats["misses"], 1)

    def test_eviction_on_overflow(self):
        # Small cache of 100 bytes
        tiny_cache = RAMReadCache(max_bytes=100, max_file_size=100)
        tiny_cache.put("/f1", b"A" * 60)
        self.assertEqual(tiny_cache.stats["items_count"], 1)

        # Adding f2 causes f1 to be evicted
        tiny_cache.put("/f2", b"B" * 60)
        self.assertIsNone(tiny_cache.get("/f1"))
        self.assertEqual(tiny_cache.get("/f2"), b"B" * 60)


class TestReadYourOwnWrites(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.spool_dir = os.path.join(self.test_dir, "spool")
        os.makedirs(self.spool_dir, exist_ok=True)
        
        self.config = Config(
            spool_dir=self.spool_dir,
            flush_interval_sec=3600.0,  # Do not auto-flush in test
            read_cache_enabled=True,
            read_cache_max_bytes=10 * 1024 * 1024,
        )
        self.server = SpoolerServer(self.config)

    def tearDown(self):
        try:
            self.server.stop()
        except Exception:
            pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_read_your_own_writes_from_queue(self):
        target = os.path.join(self.test_dir, "unflushed_state.json")
        
        # 1. Dispatch write to queue (not flushed to disk yet)
        res_write = self.server._dispatch_command({
            "cmd": "write",
            "target_path": target,
            "text": "fresh data in ram",
        })
        self.assertIn(res_write.get("status"), ("ok", "queued"))
        self.assertFalse(os.path.exists(target))

        # 2. Read back: should come from queue
        res_read = self.server._dispatch_command({
            "cmd": "read",
            "target_path": target,
            "as_text": True,
        })
        self.assertEqual(res_read["status"], "ok")
        self.assertEqual(res_read["source"], "queue")
        self.assertEqual(res_read["text"], "fresh data in ram")

    def test_read_from_disk_and_caching(self):
        target = os.path.join(self.test_dir, "on_disk.txt")
        with open(target, "wb") as f:
            f.write(b"content already on disk")

        # 1. First read -> from disk
        res1 = self.server._dispatch_command({
            "cmd": "read",
            "target_path": target,
            "as_text": True,
        })
        self.assertEqual(res1["status"], "ok")
        self.assertEqual(res1["source"], "disk")
        self.assertEqual(res1["text"], "content already on disk")

        # 2. Second read -> from RAM cache
        res2 = self.server._dispatch_command({
            "cmd": "read",
            "target_path": target,
            "as_text": True,
        })
        self.assertEqual(res2["status"], "ok")
        self.assertEqual(res2["source"], "ram_cache")
        self.assertEqual(res2["text"], "content already on disk")

    def test_read_during_in_flight_flush_race_condition(self):
        """Verify that reading during drain_queue / in-flight flush never returns stale disk data."""
        target = os.path.join(self.test_dir, "race_condition_test.txt")
        # 1. Write v1 directly on disk
        with open(target, "wb") as f:
            f.write(b"v1_stale_content_on_disk")

        # 2. Queue v2 via spooler
        self.server._dispatch_command({
            "cmd": "spool",
            "target_path": target,
            "data": b"v2_fresh_in_flight_content",
            "op_type": "ATOMIC_WRITE"
        })

        # 3. Drain the queue (simulating PhysicalFlusher taking the batch)
        batch = self.server.task_manager.drain_queue()
        self.assertEqual(len(batch), 1)
        self.assertEqual(len(self.server.task_manager.queue), 0, "Queue must be empty")
        self.assertIn(batch[0].task_id, self.server.task_manager.in_flight_tasks, "Task must be tracked in in_flight_tasks")

        # 4. Critical check: read_file() during in-flight window
        # Disk still has v1_stale_content_on_disk, queue is empty!
        # The fix must read v2_fresh_in_flight_content from in_flight_tasks in RAM
        data, source = self.server.read_file(target)
        self.assertEqual(source, "in_flight")
        self.assertEqual(data, b"v2_fresh_in_flight_content")

        # 5. Complete flush (simulate os.replace and record_completed)
        with open(target, "wb") as f:
            f.write(b"v2_fresh_in_flight_content")
        self.server.task_manager.record_completed(batch[0])
        self.assertEqual(len(self.server.task_manager.in_flight_tasks), 0)

        # 6. Read again -> reads from disk (or cache) and has v2
        data2, source2 = self.server.read_file(target)
        self.assertEqual(data2, b"v2_fresh_in_flight_content")


if __name__ == "__main__":
    unittest.main()
