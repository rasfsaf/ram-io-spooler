"""Physical HDD Flusher with fsync guarantees and Zero-Leak page cache eviction."""

import gc
import logging
import os
import time
import uuid
from typing import Any, Optional

from .state import OpType, SpoolTask, TaskManager, TaskStatus

logger = logging.getLogger("ram_io_spooler.flusher")

# Check for posix_fadvise availability (Linux only)
_POSIX_FADVISE = getattr(os, "posix_fadvise", None)
_POSIX_FADV_DONTNEED = getattr(os, "POSIX_FADV_DONTNEED", 4)


class PhysicalFlusher:
    """Performs physical writes to the HDD, ensures fsync, and purges RAM & page cache."""

    def __init__(
        self,
        task_manager_or_config: Any = None,
        task_manager: Optional[TaskManager] = None,
        fsync_on_flush: bool = True,
        **kwargs: Any,
    ) -> None:
        if "fsync_enabled" in kwargs:
            fsync_on_flush = kwargs["fsync_enabled"]
        self.fsync_on_flush = fsync_on_flush
        self.fadvise_dontneed = kwargs.get("fadvise_dontneed", True)
        if isinstance(task_manager_or_config, TaskManager):
            self.task_manager = task_manager_or_config
        elif task_manager is not None:
            self.task_manager = task_manager
        else:
            mgr = getattr(task_manager_or_config, "task_manager", None)
            self.task_manager = mgr if mgr is not None else TaskManager()

    def flush_batch(self, tasks: list) -> int:
        """Flush a list of tasks and return count of successful flushes."""
        count = 0
        for task in tasks:
            if self.flush_task(task):
                count += 1
        return count

    def flush_task(self, task: SpoolTask) -> bool:
        """Execute physical write for a single task and perform zero-leak eviction."""
        task.status = TaskStatus.FLUSHING
        target_path = os.path.abspath(task.target_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        try:
            if task.op_type in (OpType.ATOMIC_WRITE, OpType.SQLITE_SYNC):
                self._flush_atomic(task, target_path)
            elif task.op_type == OpType.APPEND:
                self._flush_append(task, target_path)
            else:
                raise ValueError(f"Unsupported operation type: {task.op_type}")

            task.status = TaskStatus.SYNCED
            task.flushed_at = time.time()

            # Perform Zero-Leak Eviction
            self._evict_payload(task)
            task.status = TaskStatus.EVICTED

            self.task_manager.metrics.total_synced += 1
            self.task_manager.metrics.total_evicted += 1
            self.task_manager.metrics.total_bytes_written += task.byte_size
            self.task_manager.record_completed(task)
            return True

        except Exception as exc:
            logger.error("Failed to flush task %s to %s: %s", task.task_id, target_path, exc, exc_info=True)
            task.status = TaskStatus.FAILED
            task.error_message = str(exc)
            self.task_manager.metrics.total_failed += 1
            self.task_manager.record_completed(task)
            return False

    def _flush_atomic(self, task: SpoolTask, target_path: str) -> None:
        """Write to a temporary file, fsync, drop cache, and atomically replace target."""
        target_dir = os.path.dirname(target_path)
        temp_path = os.path.join(target_dir, f".spool_{uuid.uuid4().hex[:12]}.tmp")

        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY

        fd = os.open(temp_path, flags, 0o644)
        try:
            self._write_payload_to_fd(task, fd)
            # Physical sync to disk platter
            os.fsync(fd)

            # Evict from kernel page cache
            if _POSIX_FADVISE is not None:
                try:
                    _POSIX_FADVISE(fd, 0, 0, _POSIX_FADV_DONTNEED)
                except OSError:
                    pass
        finally:
            os.close(fd)

        # Atomic rename on the filesystem
        os.replace(temp_path, target_path)

        # Sync directory entry on POSIX systems to persist metadata
        self._sync_directory(target_dir)

    def _flush_append(self, task: SpoolTask, target_path: str) -> None:
        """Append data to the target file, fsync, and drop page cache."""
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY

        fd = os.open(target_path, flags, 0o644)
        try:
            self._write_payload_to_fd(task, fd)
            # Physical sync to disk platter
            os.fsync(fd)

            # Evict from kernel page cache
            if _POSIX_FADVISE is not None:
                try:
                    _POSIX_FADVISE(fd, 0, 0, _POSIX_FADV_DONTNEED)
                except OSError:
                    pass
        finally:
            os.close(fd)

    def _write_payload_to_fd(self, task: SpoolTask, fd: int) -> None:
        """Stream payload into file descriptor in 64KB blocks."""
        if task.in_memory_data is not None:
            data = task.in_memory_data
            total_written = 0
            while total_written < len(data):
                chunk = data[total_written : total_written + 65536]
                written = os.write(fd, chunk)
                total_written += written
            return

        if task.staging_file and os.path.exists(task.staging_file):
            with open(task.staging_file, "rb") as sf:
                while True:
                    chunk = sf.read(65536)
                    if not chunk:
                        break
                    os.write(fd, chunk)
            return

        raise RuntimeError(f"Task {task.task_id} has no available payload to flush")

    def _sync_directory(self, dir_path: str) -> None:
        """Fsync directory to ensure metadata entry is written on disk."""
        if hasattr(os, "O_DIRECTORY"):
            try:
                dfd = os.open(dir_path, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass

    def _evict_payload(self, task: SpoolTask) -> None:
        """Zero-Leak Eviction: remove staging blob, release python memory, trigger gc."""
        if task.staging_file and os.path.exists(task.staging_file):
            try:
                os.unlink(task.staging_file)
            except OSError:
                pass
            task.staging_file = None

        if task.in_memory_data is not None:
            task.in_memory_data = None

        self.task_manager.metrics.staging_bytes_current = max(
            0, self.task_manager.metrics.staging_bytes_current - task.byte_size
        )
        task.status = TaskStatus.EVICTED

        # Explicit garbage collection to free buffer objects in Python
        gc.collect()


HDDFlusher = PhysicalFlusher
Flusher = PhysicalFlusher
