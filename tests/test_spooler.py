"""Unit tests for ram-io-spooler components."""

import os
import shutil
import tempfile
import unittest

from spooler.coalescer import coalesce_tasks
from spooler.config import SpoolerConfig
from spooler.elevator import elevator_sort
from spooler.flusher import PhysicalFlusher
from spooler.state import OpType, SpoolTask, TaskManager, TaskStatus


class TestRamIoSpooler(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="spooler_test_")
        self.staging_dir = os.path.join(self.test_dir, "staging")
        os.makedirs(self.staging_dir, exist_ok=True)
        self.config = SpoolerConfig(
            spool_dir=self.test_dir,
            staging_dir=self.staging_dir,
            flush_interval_sec=1.0,
        )
        self.task_manager = TaskManager(self.config)
        self.flusher = PhysicalFlusher(self.config, self.task_manager)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_write_coalescing_atomic(self) -> None:
        target_file = os.path.join(self.test_dir, "app_state.json")

        # Create 50 consecutive atomic updates to the same file
        for i in range(50):
            payload = f'{{"counter": {i}}}'.encode("utf-8")
            self.task_manager.enqueue(
                target_path=target_file,
                data=payload,
                op_type=OpType.ATOMIC_WRITE,
            )

        self.assertEqual(self.task_manager.pending_count, 50)

        # Drain and coalesce
        tasks = self.task_manager.drain_queue()
        coalesced = coalesce_tasks(tasks, self.task_manager)

        # 50 updates to same path must coalesce into exactly 1 physical write!
        self.assertEqual(len(coalesced), 1)
        self.assertEqual(self.task_manager.metrics.total_coalesced, 49)
        self.assertEqual(coalesced[0].target_path, target_file)

        # Flush the single coalesced task
        self.flusher.flush_task(coalesced[0])

        with open(target_file, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertEqual(content, '{"counter": 49}')

    def test_write_coalescing_append(self) -> None:
        log_file = os.path.join(self.test_dir, "access.log")

        # Create 10 log appends
        for i in range(10):
            line = f"log entry {i}\n".encode("utf-8")
            self.task_manager.enqueue(
                target_path=log_file,
                data=line,
                op_type=OpType.APPEND,
            )

        tasks = self.task_manager.drain_queue()
        coalesced = coalesce_tasks(tasks, self.task_manager)

        # 10 appends must coalesce into 1 consolidated append task
        self.assertEqual(len(coalesced), 1)
        self.assertEqual(self.task_manager.metrics.total_coalesced, 9)

        self.flusher.flush_task(coalesced[0])

        with open(log_file, "r", encoding="utf-8") as f:
            content = f.read()
        expected = "".join(f"log entry {i}\n" for i in range(10))
        self.assertEqual(content, expected)

    def test_elevator_sort(self) -> None:
        paths = [
            os.path.join(self.test_dir, "z_dir", "file2.txt"),
            os.path.join(self.test_dir, "a_dir", "file1.txt"),
            os.path.join(self.test_dir, "m_dir", "file3.txt"),
            os.path.join(self.test_dir, "a_dir", "file2.txt"),
        ]

        tasks = []
        for p in paths:
            t = self.task_manager.enqueue(target_path=p, data=b"data")
            tasks.append(t)

        sorted_tasks = elevator_sort(tasks)
        sorted_paths = [t.target_path for t in sorted_tasks]

        # Files in a_dir must be adjacent and first, followed by m_dir, then z_dir
        self.assertEqual(os.path.dirname(sorted_paths[0]), os.path.join(self.test_dir, "a_dir"))
        self.assertEqual(os.path.dirname(sorted_paths[1]), os.path.join(self.test_dir, "a_dir"))
        self.assertEqual(os.path.dirname(sorted_paths[2]), os.path.join(self.test_dir, "m_dir"))
        self.assertEqual(os.path.dirname(sorted_paths[3]), os.path.join(self.test_dir, "z_dir"))

    def test_zero_leak_eviction(self) -> None:
        target_file = os.path.join(self.test_dir, "data.bin")
        large_payload = b"X" * (128 * 1024)  # 128 KB

        task = self.task_manager.enqueue(
            target_path=target_file,
            data=large_payload,
            op_type=OpType.ATOMIC_WRITE,
        )

        staging_file = task.staging_file
        self.assertIsNotNone(staging_file)
        self.assertTrue(os.path.exists(staging_file))

        # Perform physical flush
        success = self.flusher.flush_task(task)
        self.assertTrue(success)
        self.assertEqual(task.status, TaskStatus.EVICTED)

        # Staging file in RAM/tmpfs MUST be deleted!
        self.assertFalse(os.path.exists(staging_file))
        # Payload in Python task object MUST be set to None!
        self.assertIsNone(task.in_memory_data)

        # Verify disk file is physically intact
        self.assertTrue(os.path.exists(target_file))
        self.assertEqual(os.path.getsize(target_file), len(large_payload))


if __name__ == "__main__":
    unittest.main()
